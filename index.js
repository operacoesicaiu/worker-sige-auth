const axios = require('axios');
const { format, subMonths, parseISO, isBefore, isAfter } = require('date-fns');

function secureLog(message, isError = false) {
    const timestamp = new Date().toISOString();
    const logLevel = isError ? 'ERROR' : 'INFO';
    console.log(`[${timestamp}] [${logLevel}] ${message}`);
}

function sanitize(val) {
    if (typeof val !== 'string') return val;
    const formulaChars = ['=', '+', '-', '@'];
    if (formulaChars.some(char => val.startsWith(char))) return `'${val}`;
    return val;
}

function formatarDataBR(dataISO) {
    if (!dataISO) return '';
    // Handle already formatted dates (e.g., from SIGE API for DataFaturamento)
    if (dataISO.includes('/')) {
        return dataISO; // Assume it's already dd/MM/yyyy
    }
    try {
        const data = parseISO(dataISO.replace('Z', '')); // Remove 'Z' for consistent parsing
        return format(data, 'dd/MM/yyyy');
    } catch (error) {
        // Fallback for dates that might not be ISO or have other formats
        const dateParts = dataISO.split('-');
        if (dateParts.length === 3) {
            return `${dateParts[2]}/${dateParts[1]}/${dateParts[0]}`;
        }
        return dataISO; // Return original if cannot parse
    }
}

// Helper to parse dd/MM/yyyy to Date object
function parseDateBR(dateStr) {
    if (!dateStr) return null;
    const [day, month, year] = dateStr.split('/');
    return new Date(`${year}-${month}-${day}T00:00:00.000Z`);
}

async function getERPData(spreadsheetId, googleToken, uniqueCpfs, minDateOverall) {
    secureLog(`Buscando dados do ERP para ${uniqueCpfs.length} CPFs/CNPJs a partir de ${format(minDateOverall, 'dd/MM/yyyy')}`);
    const erpDataMap = {};

    // To optimize, we will try to fetch a broad range that covers all necessary columns.
    // We assume the data is generally sorted by date, or at least we can filter efficiently after fetching.
    // Let's fetch columns D, G, N, Q, T, AH. This corresponds to indices 3, 6, 13, 16, 19, 33.
    // The range will be 'ERP!D:AH'. This assumes the sheet is named 'ERP'.

    const range = 'ERP!D:AH'; // Covers CPF/CNPJ, Tipo Serviço, Responsável, Data ERP, Mês/Ano ERP

    try {
        const response = await axios.get(
            `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${range}?key=${googleToken}`,
            { headers: { 'Authorization': `Bearer ${googleToken}` } }
        );

        const values = response.data.values || [];
        if (values.length === 0) {
            secureLog("Nenhum dado encontrado na planilha ERP.", false);
            return {};
        }

        // Skip header row if present. Assuming first row is header.
        const dataRows = values.slice(1); 

        // Filter and organize ERP data by CPF/CNPJ
        for (const row of dataRows) {
            const erpCpf = row[0]; // Column D (index 0 in fetched range D:AH)
            const erpDateStr = row[16]; // Column T (index 16 in fetched range D:AH)
            const erpServiceType = row[3]; // Column G (index 3 in fetched range D:AH)
            const erpMonthYear = row[30]; // Column AH (index 30 in fetched range D:AH)

            const erpDate = parseDateBR(erpDateStr);

            if (erpCpf && erpDate && isAfter(erpDate, minDateOverall) && uniqueCpfs.includes(erpCpf)) {
                if (!erpDataMap[erpCpf]) {
                    erpDataMap[erpCpf] = [];
                }
                // Store the raw row and relevant parsed data for later calculations
                erpDataMap[erpCpf].push({
                    rawRow: row,
                    date: erpDate,
                    serviceType: erpServiceType,
                    monthYear: erpMonthYear
                });
            }
        }

        secureLog(`Dados do ERP filtrados para ${Object.keys(erpDataMap).length} CPFs/CNPJs.`);
        return erpDataMap;

    } catch (error) {
        secureLog(`Erro ao buscar dados do ERP: ${error.message}`, true);
        return {};
    }
}

async function run() {
    try {
        const { SIGE_TOKEN, SIGE_USER, SIGE_APP, GOOGLE_TOKEN, SPREADSHEET_ID, ERP_SPREADSHEET_ID } = process.env;

        const sigeHeaders = {
            "Authorization-Token": SIGE_TOKEN,
            "User": SIGE_USER,
            "App": SIGE_APP,
            "Content-Type": "application/json",
        };

        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dataBusca = ontem.toISOString().split("T")[0];

        secureLog(`Iniciando extração SIGE para o dia: ${dataBusca}`);

        const resSige = await axios.get("https://api.sigecloud.com.br/request/Pedidos/Pesquisar", {
            headers: sigeHeaders,
            params: {
                status: "Pedido Faturado",
                dataInicial: dataBusca,
                dataFinal: dataBusca,
                filtrarPor: 3,
                pageSize: 100
            }
        });

        const pedidos = resSige.data || [];
        secureLog(`Pedidos encontrados: ${pedidos.length}`);

        if (pedidos.length === 0) return;

        const uniqueCpfs = [...new Set(pedidos.map(p => p.ClienteCNPJ).filter(Boolean))];
        let minSigeDate = new Date(); // Initialize with a future date
        if (pedidos.length > 0) {
            minSigeDate = pedidos.reduce((min, p) => {
                const pDate = parseDateBR(formatarDataBR(p.DataFaturamento || p.Data));
                return pDate && isBefore(pDate, min) ? pDate : min;
            }, minSigeDate);
        }

        const minDateOverall = subMonths(minSigeDate, 6); // 6 months before the earliest SIGE order

        // Pre-fetch relevant ERP data once
        let erpDataMap = {};
        if (uniqueCpfs.length > 0) {
             erpDataMap = await getERPData(ERP_SPREADSHEET_ID, GOOGLE_TOKEN, uniqueCpfs, minDateOverall);
        }

        const rows = [];

        for (const p of pedidos) {
            try {
                // Busca detalhada para pegar Celular, Telefone e Nome Fantasia
                let c = {};
                const clienteCpf = p.ClienteCNPJ || "";
                if (clienteCpf) {
                    try {
                        const resPessoa = await axios.get("https://api.sigecloud.com.br/request/Pessoas/Pesquisar", {
                            headers: sigeHeaders,
                            params: { cpfcnpj: clienteCpf }
                        });
                        if (resPessoa.data && resPessoa.data.length > 0) {
                            c = resPessoa.data[0]; // Take the first person found
                        } else {
                            secureLog(`Cliente com CPF/CNPJ não encontrado na API ou dados vazios para Pedido ${p.Codigo}`, false);
                        }
                    } catch (errApi) {
                        secureLog(`Erro na API ao buscar detalhes do cliente para Pedido ${p.Codigo}. Status: ${errApi.response ? errApi.response.status : "N/A"}`, true);
                    }
                }

                const dataVendaSige = parseDateBR(formatarDataBR(p.DataFaturamento || p.Data));
                const clienteErpEntries = erpDataMap[clienteCpf] || [];

                // Coluna R: Mês/Ano da Venda
                const mesAnoVenda = dataVendaSige ? format(dataVendaSige, 'MM/yyyy') : '';

                // Funções para MAXIFS e INDEX/MATCH
                const getMaxifsDate = (serviceType) => {
                    let maxDate = null;
                    for (const erpEntry of clienteErpEntries) {
                        const erpDate = erpEntry.date;
                        const erpServiceType = erpEntry.serviceType;
                        
                        // Check if within 6 months *before* dataVendaSige
                        const sixMonthsBeforeVenda = subMonths(dataVendaSige, 6);
                        if (erpDate && isBefore(erpDate, dataVendaSige) && isAfter(erpDate, sixMonthsBeforeVenda) && erpServiceType === serviceType) {
                            if (!maxDate || isAfter(erpDate, maxDate)) {
                                maxDate = erpDate;
                            }
                        }
                    }
                    return maxDate ? format(maxDate, 'dd/MM/yyyy') : 0;
                };

                const getResponsavelAgendamento = (matchDateStr) => {
                    if (matchDateStr === 0 || !matchDateStr) return "Sem vendedor";
                    const matchDate = parseDateBR(matchDateStr);

                    for (const erpEntry of clienteErpEntries) {
                        const erpDate = erpEntry.date;
                        // Column N is index 13 from the original sheet, which is index 10 in our fetched D:AH range (D=0, ..., N=10)
                        const responsavel = erpEntry.rawRow[10]; 

                        if (erpDate && matchDate && erpDate.getTime() === matchDate.getTime()) {
                            return sanitize(responsavel || "Sem vendedor");
                        }
                    }
                    return "Sem vendedor";
                };

                const novoServicoDate = getMaxifsDate("Novo Serviço"); // Coluna P (no excel)
                const retiradaDate = getMaxifsDate("Retirada");     // Coluna O (no excel)

                const responsavelAgendamentoNovoServico = getResponsavelAgendamento(novoServicoDate); // Coluna M
                const responsavelAgendamentoRetirada = getResponsavelAgendamento(retiradaDate); // Coluna P

                const comissaoNovoServico = novoServicoDate !== 0 ? (p.ValorFinal || 0) * 0.5 : (p.ValorFinal || 0);
                const comissaoRetirada = retiradaDate !== 0 ? (p.ValorFinal || 0) * 0.5 : 0;

                // Lógica da Coluna J (Documento) exatamente como no seu sige_api.js
                const numNF = p.NumeroNFe || "";
                const documentoFormatado = `Pedido ${p.Codigo}${numNF ? ' / NF Nº ' + numNF : ''}`;

                // Montagem seguindo a ordem exata que você passou
                rows.push([
                    sanitize((c.Celular || "").replace("+", "")),                    // A - Cliente Celular
                    p.Codigo,                                     // B - Código
                    sanitize(p.StatusSistema || ""),              // C - Venda Status do Sistema
                    formatarDataBR(p.DataFaturamento || p.Data),  // D - Venda Data
                    sanitize(c.NomeFantasia || p.Cliente || ""),  // E - Cliente Nome Fantasia
                    sanitize(c.Telefone || ""),                   // F - Cliente Telefone
                    sanitize(c.Email || p.ClienteEmail || ""),    // G - Cliente E-mail
                    p.ValorFinal || 0,                            // H - Venda Valor Total
                    sanitize(p.Vendedor || ""),                   // I - Venda Vendedor
                    sanitize(documentoFormatado),                 // J - Nº Documento (Pedido + NF)
                    sanitize(p.ClienteCNPJ || ""),                 // K - Cliente CPF/CNPJ
                    novoServicoDate,                              // L - Novo Serviço (MAXIFS Date)
                    responsavelAgendamentoNovoServico,            // M - Responsável Agendamento Novo Serviço
                    comissaoNovoServico,                          // N - Comissão Novo Serviço
                    retiradaDate,                                 // O - Retirada (MAXIFS Date)
                    responsavelAgendamentoRetirada,               // P - Responsável Agendamento Retirada
                    comissaoRetirada,                             // Q - Comissão Retirada
                    mesAnoVenda                                   // R - Mês/Ano da Venda
                ]);

            } catch (errCliente) {
                secureLog(`Erro ao detalhar dados adicionais para o Pedido ${p.Codigo}. Verifique a conectividade com a API de Pessoas ou a lógica de ERP.`, true);
                // Fallback row with original data plus empty new columns
                rows.push([
                    "", p.Codigo, p.StatusSistema, formatarDataBR(p.DataFaturamento), p.Cliente, "", p.ClienteEmail, p.ValorFinal, p.Vendedor, `Pedido ${p.Codigo}`, p.ClienteCNPJ,
                    0, "Sem vendedor", (p.ValorFinal || 0), 0, "Sem vendedor", 0, ""
                ]);
            }
        }

        secureLog("Enviando dados para o Google Sheets (Aba Faturamento)...");
        
        await axios.post(
            `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Faturamento:append?valueInputOption=USER_ENTERED`,
            { values: rows },
            { headers: { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' } }
        );

        secureLog("Processo finalizado com sucesso.");

    } catch (err) {
        secureLog(`Erro Crítico: ${err.message}`, true);
        process.exit(1);
    }
}

run();
