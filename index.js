const axios = require('axios');

function secureLog(message, isError = false) {
    const timestamp = new Date().toISOString();
    const logLevel = isError ? 'ERROR' : 'INFO';
    console.log(`[${timestamp}] [${logLevel}] ${message}`);
}

function sanitize(val) {
    if (typeof val !== 'string') return val;
    const formulaChars = ['=', '+', '-', '@'];
    if (formulaChars.some(char => val.startsWith(char))) {
        return `'${val}`;
    }
    return val;
}

async function run() {
    try {
        const { SIGE_TOKEN, SIGE_USER, SIGE_APP, GOOGLE_TOKEN, SPREADSHEET_ID } = process.env;
        const sId = SPREADSHEET_ID.trim();
        const gToken = GOOGLE_TOKEN.trim();

        const sigeHeaders = {
            "Authorization-Token": SIGE_TOKEN,
            "User": SIGE_USER,
            "App": SIGE_APP,
            "Content-Type": "application/json",
        };

        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dataFormatada = ontem.toISOString().split('T')[0];

        secureLog(`Buscando pedidos SIGE: ${dataFormatada}`);

        const resSige = await axios.get("https://api.sigecloud.com.br/request/Pedidos/Pesquisar", {
            headers: sigeHeaders,
            params: { status: "Pedido Faturado", dataInicial: dataFormatada, dataFinal: dataFormatada, filtrarPor: 3, pageSize: 100 }
        });

        const pedidos = resSige.data || [];
        if (pedidos.length === 0) {
            secureLog("Nenhum dado para processar.");
            return;
        }

        const rows = pedidos.map(p => [
            sanitize(p.ClienteCNPJ || ""),
            sanitize(p.Cliente || ""),
            sanitize(p.ClienteEmail || ""),
            p.DataFaturamento || p.Data || "",
            `Pedido ${p.Codigo}${p.NumeroNFe ? ' / NF ' + p.NumeroNFe : ''}`,
            p.ValorFinal || 0,
            sanitize(p.Vendedor || ""),
            sanitize(p.StatusSistema || ""),
            p.Codigo
        ]);

        const aba = "Faturamento";
        secureLog(`Tentando enviar ${rows.length} linhas para a aba [${aba}] na planilha [${sId}]`);
        
        const url = `https://sheets.googleapis.com/v4/spreadsheets/${sId}/values/${encodeURIComponent(aba)}:append?valueInputOption=USER_ENTERED`;

        const response = await axios.post(url, { values: rows }, { 
            headers: { 'Authorization': `Bearer ${gToken}`, 'Content-Type': 'application/json' } 
        });

        secureLog(`Sucesso! Planilha atualizada. Range: ${response.data.updates.updatedRange}`);

    } catch (err) {
        if (err.response) {
            secureLog(`ERRO GOOGLE DETALHADO: ${JSON.stringify(err.response.data.error)}`, true);
        } else {
            secureLog(`ERRO: ${err.message}`, true);
        }
        process.exit(1);
    }
}

run();
