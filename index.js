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

// Converte string DD/MM/YYYY para Número Serial do Excel
function dateToExcelSerial(dateStr) {
    if (!dateStr || typeof dateStr !== 'string') return "";
    const partes = dateStr.split('/');
    if (partes.length !== 3) return "";
    const date = new Date(partes[2], partes[1] - 1, partes[0]);
    const returnDateTime = 25569.0 + (date.getTime() - (date.getTimezoneOffset() * 60 * 1000)) / (1000 * 60 * 60 * 24);
    return Math.floor(returnDateTime);
}

// --- EXECUÇÃO ---

async function run() {
    try {
        const { SIGE_TOKEN, SIGE_USER, SIGE_APP, GOOGLE_TOKEN, SPREADSHEET_ID, ERP_SPREADSHEET_ID } = process.env;
        const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };
        const sigeHeaders = { "Authorization-Token": SIGE_TOKEN, "User": SIGE_USER, "App": SIGE_APP, "Content-Type": "application/json" };

        // 1. Coleta bloco recente do ERP (25k linhas)
        secureLog("Calculando intervalo ERP...");
        const meta = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${ERP_SPREADSHEET_ID}?fields=sheets(properties(title,gridProperties/rowCount))`, { headers: gHeaders });
        const erpSheet = meta.data.sheets.find(s => s.properties.title === 'ERP');
        const totalRows = erpSheet.properties.gridProperties.rowCount;
        const startRow = Math.max(1, totalRows - 25000);
        
        const resErp = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${ERP_SPREADSHEET_ID}/values/ERP!A${startRow}:AH${totalRows}`, { headers: gHeaders });
        const erpRows = resErp.data.values || [];
        const COL = { CPF: 3, TIPO: 6, RESP: 15, CHAVE: 16, DATA: 19 };

        // 2. Busca Pedidos no SIGE
        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dataBusca = ontem.toISOString().split('T')[0];

        const resSige = await axios.get("https://api.sigecloud.com.br/request/Pedidos/Pesquisar", {
            headers: sigeHeaders, params: { status: "Pedido Faturado", dataInicial: dataBusca, dataFinal: dataBusca, filtrarPor: 3, pageSize: 100 }
        });

        const pedidos = resSige.data || [];
        if (pedidos.length === 0) return secureLog("Sem pedidos faturados.");

        const rowsFinal = [];

        for (const p of pedidos) {
            const clienteCpf = p.ClienteCNPJ || "";
            const clienteCpfLimpo = clienteCpf.replace(/\D/g, "");
            
            // --- RESTAURADO: Busca de Celular e Telefone (Colunas A e F) ---
            let detalhesCliente = {};
            if (clienteCpf) {
                try {
                    const resP = await axios.get("https://api.sigecloud.com.br/request/Pessoas/Pesquisar", {
                        headers: sigeHeaders, params: { cpfcnpj: clienteCpf }
                    });
                    if (resP.data?.length > 0) detalhesCliente = resP.data[0];
                } catch (e) { secureLog(`Erro ao buscar detalhes do cliente ${clienteCpf}`); }
            }

            let rawDataNovoServico = "";
            let rawDataRetirada = "";

            // Busca no ERP
            for (let i = erpRows.length - 1; i >= 0; i--) {
                const r = erpRows[i];
                if ((r[COL.CPF] || "").replace(/\D/g, "") === clienteCpfLimpo && clienteCpfLimpo !== "") {
                    const tipo = (r[COL.TIPO] || "").toLowerCase();
                    if (!rawDataNovoServico && tipo.includes("novo")) rawDataNovoServico = r[COL.DATA];
                    if (!rawDataRetirada && tipo.includes("retirada")) rawDataRetirada = r[COL.DATA];
                }
                if (rawDataNovoServico && rawDataRetirada) break;
            }

            const buscarResp = (dataAchada) => {
                if (!dataAchada) return "Sem vendedor";
                const chaveAlvo = (dataAchada + clienteCpfLimpo).replace(/\D/g, "");
                const match = erpRows.find(r => (r[COL.CHAVE] || "").replace(/\D/g, "").includes(chaveAlvo));
                return match ? match[COL.RESP] : "Sem vendedor";
            };

            const dataVenda = new Date(p.DataFaturamento || p.Data);

            rowsFinal.push([
                sanitize((detalhesCliente.Celular || "").replace("+", "")), // A - Celular
                p.Codigo, // B
                sanitize(p.StatusSistema || ""), // C
                formatarDataBR(dataVenda), // D
                sanitize(detalhesCliente.NomeFantasia || p.Cliente || ""), // E
                sanitize(detalhesCliente.Telefone || ""), // F - Telefone
                sanitize(detalhesCliente.Email || p.ClienteEmail || ""), // G
                p.ValorFinal || 0, // H
                sanitize(p.Vendedor || ""), // I
                sanitize(`Pedido ${p.Codigo}${p.NumeroNFe ? ' / NF Nº ' + p.NumeroNFe : ''}`), // J
                sanitize(clienteCpf), // K
                rawDataNovoServico ? dateToExcelSerial(rawDataNovoServico) : "", // L - Número Serial
                sanitize(buscarResp(rawDataNovoServico)), // M
                rawDataRetirada !== "" ? (p.ValorFinal * 0.5) : (p.ValorFinal || 0), // N
                rawDataRetirada ? dateToExcelSerial(rawDataRetirada) : "", // O - Número Serial (ex: 46106)
                sanitize(buscarResp(rawDataRetirada)), // P
                rawDataRetirada !== "" ? (p.ValorFinal * 0.5) : 0, // Q
                `${dataVenda.getMonth() + 1}/${dataVenda.getFullYear()}` // R
            ]);
        }

        if (rowsFinal.length > 0) {
            await axios.post(
                `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Faturamento:append?valueInputOption=USER_ENTERED`,
                { values: rowsFinal }, { headers: gHeaders }
            );
            secureLog(`Sucesso: ${rowsFinal.length} pedidos enviados.`);
        }
    } catch (err) {
        secureLog(`Erro: ${err.message}`, true);
        process.exit(1);
    }
}
run();
