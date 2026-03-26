const axios = require('axios');

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

// Converte Data do Sheets (DD/MM/YYYY) ou ISO para Número Serial do Excel
function dateToExcelSerial(dateInput) {
    let date;
    if (typeof dateInput === 'string' && dateInput.includes('/')) {
        const [d, m, y] = dateInput.split('/');
        date = new Date(y, m - 1, d);
    } else {
        date = new Date(dateInput);
    }
    const returnDateTime = 25569.0 + (date.getTime() - (date.getTimezoneOffset() * 60 * 1000)) / (1000 * 60 * 60 * 24);
    return Math.floor(returnDateTime);
}

async function run() {
    try {
        const { SIGE_TOKEN, SIGE_USER, SIGE_APP, GOOGLE_TOKEN, SPREADSHEET_ID, ERP_SPREADSHEET_ID } = process.env;
        const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };
        const sigeHeaders = { "Authorization-Token": SIGE_TOKEN, "User": SIGE_USER, "App": SIGE_APP, "Content-Type": "application/json" };

        secureLog("Lendo base ERP...");
        const resErp = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${ERP_SPREADSHEET_ID}/values/ERP!A:AH`, { headers: gHeaders });
        const erpRows = resErp.data.values || [];
        
        // Mapeamento baseado no seu CSV: 
        // D=3 (CPF), G=6 (Tipo), P=15 (Responsável), Q=16 (Chave), T=19 (Data), AH=33 (Mês)
        const COL = { CPF: 3, TIPO: 6, RESP: 15, CHAVE: 16, DATA: 19, MES: 33 };

        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dataBusca = ontem.toISOString().split('T')[0];

        const resSige = await axios.get("https://api.sigecloud.com.br/request/Pedidos/Pesquisar", {
            headers: sigeHeaders,
            params: { status: "Pedido Faturado", dataInicial: dataBusca, dataFinal: dataBusca, filtrarPor: 3, pageSize: 100 }
        });

        const pedidos = resSige.data || [];
        if (pedidos.length === 0) return secureLog("Sem novos pedidos.");

        const rowsFinal = [];

        for (const p of pedidos) {
            const clienteCpfOriginal = p.ClienteCNPJ || "";
            const clienteCpfLimpo = clienteCpfOriginal.replace(/\D/g, "");
            const dataVenda = new Date(p.DataFaturamento || p.Data);
            
            let rawDataNovoServico = "";
            let rawDataRetirada = "";

            // Busca Reversa no ERP (Limitada às últimas 10k linhas para performance)
            const startSearch = Math.max(1, erpRows.length - 10000);
            for (let i = erpRows.length - 1; i >= startSearch; i--) {
                const r = erpRows[i];
                const erpCpfLimpo = (r[COL.CPF] || "").replace(/\D/g, "");

                if (erpCpfLimpo === clienteCpfLimpo) {
                    const tipo = (r[COL.TIPO] || "").toLowerCase();
                    if (!rawDataNovoServico && tipo.includes("novo")) rawDataNovoServico = r[COL.DATA];
                    if (!rawDataRetirada && tipo.includes("retirada")) rawDataRetirada = r[COL.DATA];
                }
                if (rawDataNovoServico && rawDataRetirada) break;
            }

            // FUNÇÃO DE BUSCA DO VENDEDOR (Normaliza Chave da Coluna Q)
            const buscarResp = (dataAchada) => {
                if (!dataAchada) return "Sem vendedor";
                // Limpa tudo o que não é número da chave para comparar (Data + CPF)
                const chaveAlvo = (dataAchada + clienteCpfLimpo).replace(/\D/g, "");
                
                const match = erpRows.find(r => {
                    const chavePlanilha = (r[COL.CHAVE] || "").replace(/\D/g, "");
                    return chavePlanilha.includes(chaveAlvo) || chaveAlvo.includes(chavePlanilha);
                });
                return match ? match[COL.RESP] : "Sem vendedor";
            };

            const respM = buscarResp(rawDataNovoServico);
            const respP = buscarResp(rawDataRetirada);

            const valorTotal = p.ValorFinal || 0;
            const valN = rawDataRetirada !== "" ? valorTotal * 0.5 : valorTotal;
            const valQ = rawDataRetirada !== "" ? valorTotal * 0.5 : 0;

            rowsFinal.push([
                "", // A
                p.Codigo, // B
                sanitize(p.StatusSistema || ""), // C
                formatarDataBR(dataVenda), // D
                sanitize(p.Cliente || ""), // E
                "", // F
                sanitize(p.ClienteEmail || ""), // G
                valorTotal, // H
                sanitize(p.Vendedor || ""), // I
                sanitize(`Pedido ${p.Codigo}`), // J
                sanitize(clienteCpfOriginal), // K
                rawDataNovoServico ? dateToExcelSerial(rawDataNovoServico) : "", // L
                sanitize(respM), // M
                valN, // N
                rawDataRetirada ? dateToExcelSerial(rawDataRetirada) : "", // O
                sanitize(respP), // P
                valQ, // Q
                `${dataVenda.getMonth() + 1}/${dataVenda.getFullYear()}` // R
            ]);
        }

        if (rowsFinal.length > 0) {
            await axios.post(
                `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Faturamento:append?valueInputOption=USER_ENTERED`,
                { values: rowsFinal }, { headers: gHeaders }
            );
            secureLog(`Processado: ${rowsFinal.length} pedidos.`);
        }
    } catch (err) {
        secureLog(err.message, true);
    }
}
run();
