const axios = require('axios');

// --- UTILITÁRIOS ---

function secureLog(message, isError = false) {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] [${isError ? 'ERROR' : 'INFO'}] ${message}`);
}

function sanitize(val) {
    if (typeof val !== 'string') return val;
    const formulaChars = ['=', '+', '-', '@'];
    if (formulaChars.some(char => val.startsWith(char))) return `'${val}`;
    return val;
}

function formatarDataBR(dataInput) {
    if (!dataInput) return "";
    const data = new Date(dataInput);
    return data.toLocaleDateString('pt-BR');
}

// Converte string DD/MM/YYYY para Número Serial do Excel (essencial para suas fórmulas)
function dateToExcelSerial(dateStr) {
    if (!dateStr || typeof dateStr !== 'string' || !dateStr.includes('/')) return "";
    const partes = dateStr.split('/');
    const date = new Date(partes[2], partes[1] - 1, partes[0]);
    if (isNaN(date)) return "";
    const returnDateTime = 25569.0 + (date.getTime() - (date.getTimezoneOffset() * 60 * 1000)) / (1000 * 60 * 60 * 24);
    return Math.floor(returnDateTime);
}

// --- EXECUÇÃO PRINCIPAL ---

async function run() {
    try {
        const { SIGE_TOKEN, SIGE_USER, SIGE_APP, GOOGLE_TOKEN, SPREADSHEET_ID, ERP_SPREADSHEET_ID } = process.env;
        const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };
        const sigeHeaders = { "Authorization-Token": SIGE_TOKEN, "User": SIGE_USER, "App": SIGE_APP, "Content-Type": "application/json" };

        // 1. Coleta bloco recente do ERP (25.000 linhas)
        secureLog("Lendo bloco recente do ERP (25k linhas)...");
        const meta = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${ERP_SPREADSHEET_ID}?fields=sheets(properties(title,gridProperties/rowCount))`, { headers: gHeaders });
        const erpSheet = meta.data.sheets.find(s => s.properties.title === 'ERP');
        const totalRows = erpSheet.properties.gridProperties.rowCount;
        const startRow = Math.max(1, totalRows - 25000);
        
        const resErp = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${ERP_SPREADSHEET_ID}/values/ERP!A${startRow}:AH${totalRows}`, { headers: gHeaders });
        const erpRows = resErp.data.values || [];
        
        // Mapeamento baseado no seu CSV: D=3(CPF), G=6(Tipo), P=15(Responsável), Q=16(Chave), T=19(Data)
        const COL = { CPF: 3, TIPO: 6, RESP: 15, CHAVE: 16, DATA: 19 };

        // 2. Busca Pedidos faturados ontem no SIGE
        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dataBusca = ontem.toISOString().split('T')[0];
        secureLog(`Buscando pedidos SIGE: ${dataBusca}`);

        const resSige = await axios.get("https://api.sigecloud.com.br/request/Pedidos/Pesquisar", {
            headers: sigeHeaders,
            params: { status: "Pedido Faturado", dataInicial: dataBusca, dataFinal: dataBusca, filtrarPor: 3, pageSize: 100 }
        });

        const pedidos = resSige.data || [];
        if (pedidos.length === 0) return secureLog("Nenhum pedido para processar.");

        const rowsFinal = [];

        for (const p of pedidos) {
            const clienteCpf = p.ClienteCNPJ || "";
            const clienteCpfLimpo = clienteCpf.replace(/\D/g, "");
            
            // --- BUSCA DETALHADA DO CLIENTE (PARA COLUNAS A e F) ---
            let c = {};
            if (clienteCpf) {
                try {
                    const resP = await axios.get("https://api.sigecloud.com.br/request/Pessoas/Pesquisar", {
                        headers: sigeHeaders, params: { cpfcnpj: clienteCpf }
                    });
                    if (resP.data && resP.data.length > 0) c = resP.data[0];
                } catch (e) { secureLog(`Erro API Pessoas para CPF ${clienteCpf}`); }
            }

            // --- BUSCA DE AGENDAMENTOS NO ERP ---
            let rawDataNovoServico = "";
            let rawDataRetirada = "";

            for (let i = erpRows.length - 1; i >= 0; i--) {
                const r = erpRows[i];
                const erpCpfLimpo = (r[COL.CPF] || "").replace(/\D/g, "");

                if (erpCpfLimpo === clienteCpfLimpo && clienteCpfLimpo !== "") {
                    const tipo = (r[COL.TIPO] || "").toLowerCase();
                    if (!rawDataNovoServico && tipo.includes("novo")) rawDataNovoServico = r[COL.DATA];
                    if (!rawDataRetirada && tipo.includes("retirada")) rawDataRetirada = r[COL.DATA];
                }
                if (rawDataNovoServico && rawDataRetirada) break;
            }

            const buscarResp = (dataAchada) => {
                if (!dataAchada) return "Sem vendedor";
                const chaveBuscadaLimpa = (dataAchada + clienteCpfLimpo).replace(/\D/g, "");
                const match = erpRows.find(r => (r[COL.CHAVE] || "").replace(/\D/g, "").includes(chaveBuscadaLimpa));
                return match ? match[COL.RESP] : "Sem vendedor";
            };

            const dataVenda = new Date(p.DataFaturamento || p.Data);
            const valorTotal = p.ValorFinal || 0;

            // Montagem da linha final para o Google Sheets
            rowsFinal.push([
                sanitize((c.Celular || "").replace("+", "")), // A - Celular (API Pessoas)
                p.Codigo, // B - Código
                sanitize(p.StatusSistema || ""), // C - Status
                formatarDataBR(dataVenda), // D - Data Venda
                sanitize(c.NomeFantasia || p.Cliente || ""), // E - Nome (API Pessoas)
                sanitize(c.Telefone || ""), // F - Telefone (API Pessoas)
                sanitize(c.Email || p.ClienteEmail || ""), // G - Email
                valorTotal, // H - Valor
                sanitize(p.Vendedor || ""), // I - Vendedor SIGE
                sanitize(`Pedido ${p.Codigo}${p.NumeroNFe ? ' / NF Nº ' + p.NumeroNFe : ''}`), // J - Documento
                sanitize(clienteCpf), // K - CPF
                rawDataNovoServico ? dateToExcelSerial(rawDataNovoServico) : "", // L - Data Serial Novo
                sanitize(buscarResp(rawDataNovoServico)), // M - Responsável Novo
                rawDataRetirada !== "" ? (valorTotal * 0.5) : valorTotal, // N - Valor Calculado
                rawDataRetirada ? dateToExcelSerial(rawDataRetirada) : "", // O - Data Serial Retirada (ex: 46106)
                sanitize(buscarResp(rawDataRetirada)), // P - Responsável Retirada
                rawDataRetirada !== "" ? (valorTotal * 0.5) : 0, // Q - Valor Retirada
                `${dataVenda.getMonth() + 1}/${dataVenda.getFullYear()}` // R - Mês/Ano
            ]);
        }

        // 3. Envio para a aba Faturamento
        if (rowsFinal.length > 0) {
            await axios.post(
                `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Faturamento:append?valueInputOption=USER_ENTERED`,
                { values: rowsFinal }, { headers: gHeaders }
            );
            secureLog(`Processo finalizado: ${rowsFinal.length} registros inseridos.`);
        }

    } catch (err) {
        secureLog(`Erro Crítico: ${err.message}`, true);
        process.exit(1);
    }
}

run();
