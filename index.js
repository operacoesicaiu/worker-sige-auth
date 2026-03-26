const axios = require('axios');

// --- CONFIGURAÇÕES E UTILITÁRIOS ---

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

function dateToExcelSerial(dateInput) {
    let date;
    if (typeof dateInput === 'string' && dateInput.includes('/')) {
        const [d, m, y] = dateInput.split('/');
        date = new Date(y, m - 1, d);
    } else {
        date = new Date(dateInput);
    }
    if (isNaN(date)) return "";
    const returnDateTime = 25569.0 + (date.getTime() - (date.getTimezoneOffset() * 60 * 1000)) / (1000 * 60 * 60 * 24);
    return Math.floor(returnDateTime);
}

// --- EXECUÇÃO ---

async function run() {
    try {
        const { SIGE_TOKEN, SIGE_USER, SIGE_APP, GOOGLE_TOKEN, SPREADSHEET_ID, ERP_SPREADSHEET_ID } = process.env;
        const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };
        const sigeHeaders = { "Authorization-Token": SIGE_TOKEN, "User": SIGE_USER, "App": SIGE_APP, "Content-Type": "application/json" };

        // 1. DESCOBRIR O TAMANHO DA ABA ERP E DEFINIR O RANGE
        secureLog("Calculando intervalo de busca no ERP...");
        const meta = await axios.get(
            `https://sheets.googleapis.com/v4/spreadsheets/${ERP_SPREADSHEET_ID}?fields=sheets(properties(title,gridProperties/rowCount))`,
            { headers: gHeaders }
        );
        const erpSheet = meta.data.sheets.find(s => s.properties.title === 'ERP');
        const totalRows = erpSheet.properties.gridProperties.rowCount;
        
        // Coletamos as últimas 25.000 linhas
        const numLinhas = 25000;
        const startRow = Math.max(1, totalRows - numLinhas);
        const range = `ERP!A${startRow}:AH${totalRows}`;

        secureLog(`Coletando bloco de ${numLinhas} linhas (${range})...`);
        const resErp = await axios.get(
            `https://sheets.googleapis.com/v4/spreadsheets/${ERP_SPREADSHEET_ID}/values/${range}`,
            { headers: gHeaders }
        );
        const erpRows = resErp.data.values || [];

        // Mapeamento (Ajuste os índices se o bloco começar em startRow > 1, mas o .find lida com o array relativo)
        const COL = { CPF: 3, TIPO: 6, RESP: 15, CHAVE: 16, DATA: 19 };

        // 2. BUSCAR PEDIDOS NO SIGE
        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dataBusca = ontem.toISOString().split('T')[0];

        const resSige = await axios.get("https://api.sigecloud.com.br/request/Pedidos/Pesquisar", {
            headers: sigeHeaders,
            params: { status: "Pedido Faturado", dataInicial: dataBusca, dataFinal: dataBusca, filtrarPor: 3, pageSize: 100 }
        });

        const pedidos = resSige.data || [];
        if (pedidos.length === 0) return secureLog("Nenhum pedido faturado para processar.");

        const rowsFinal = [];

        for (const p of pedidos) {
            const clienteCpfLimpo = (p.ClienteCNPJ || "").replace(/\D/g, "");
            const dataVenda = new Date(p.DataFaturamento || p.Data);
            
            let rawDataNovoServico = "";
            let rawDataRetirada = "";

            // Busca no bloco de 25k (de baixo para cima)
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

            // Busca do Responsável baseada na Chave Normalizada (Data + CPF)
            const buscarResp = (dataAchada) => {
                if (!dataAchada) return "Sem vendedor";
                const chaveBuscadaLimpa = (dataAchada + clienteCpfLimpo).replace(/\D/g, "");
                
                const match = erpRows.find(r => {
                    const chavePlanilha = (r[COL.CHAVE] || "").replace(/\D/g, "");
                    return chavePlanilha !== "" && (chavePlanilha.includes(chaveBuscadaLimpa) || chaveBuscadaLimpa.includes(chavePlanilha));
                });
                return match ? match[COL.RESP] : "Sem vendedor";
            };

            const respM = buscarResp(rawDataNovoServico);
            const respP = buscarResp(rawDataRetirada);

            rowsFinal.push([
                "", // A
                p.Codigo, // B
                sanitize(p.StatusSistema || ""), // C
                formatarDataBR(dataVenda), // D
                sanitize(p.Cliente || ""), // E
                "", // F
                sanitize(p.ClienteEmail || ""), // G
                p.ValorFinal || 0, // H
                sanitize(p.Vendedor || ""), // I
                sanitize(`Pedido ${p.Codigo}`), // J
                sanitize(p.ClienteCNPJ || ""), // K
                rawDataNovoServico ? dateToExcelSerial(rawDataNovoServico) : "", // L
                sanitize(respM), // M
                rawDataRetirada !== "" ? (p.ValorFinal * 0.5) : (p.ValorFinal || 0), // N
                rawDataRetirada ? dateToExcelSerial(rawDataRetirada) : "", // O
                sanitize(respP), // P
                rawDataRetirada !== "" ? (p.ValorFinal * 0.5) : 0, // Q
                `${dataVenda.getMonth() + 1}/${dataVenda.getFullYear()}` // R
            ]);
        }

        if (rowsFinal.length > 0) {
            await axios.post(
                `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Faturamento:append?valueInputOption=USER_ENTERED`,
                { values: rowsFinal }, { headers: gHeaders }
            );
            secureLog(`Concluído: ${rowsFinal.length} registros enviados.`);
        }
    } catch (err) {
        secureLog(`Erro: ${err.message}`, true);
        process.exit(1);
    }
}

run();
