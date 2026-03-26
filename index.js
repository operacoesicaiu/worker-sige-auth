const axios = require('axios');

// Função de log seguro (Padrão iCaiu)
function secureLog(message, isError = false) {
    const timestamp = new Date().toISOString();
    const logLevel = isError ? 'ERROR' : 'INFO';
    console.log(`[${timestamp}] [${logLevel}] ${message}`);
}

/**
 * Segurança: Sanitiza strings para evitar "Spreadsheet Formula Injection".
 */
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
        const { 
            SIGE_TOKEN, SIGE_USER, SIGE_APP, 
            GOOGLE_TOKEN, SPREADSHEET_ID 
        } = process.env;

        if (!SIGE_TOKEN || !GOOGLE_TOKEN) {
            throw new Error("Variáveis de ambiente essenciais ausentes.");
        }

        const sigeHeaders = {
            "Authorization-Token": SIGE_TOKEN,
            "User": SIGE_USER,
            "App": SIGE_APP,
            "Content-Type": "application/json",
        };

        // 1. Definir período (Ontem) para processamento diário
        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dataFormatada = ontem.toISOString().split('T')[0];

        secureLog(`Iniciando busca de pedidos SIGE para o dia: ${dataFormatada}`);

        // 2. Buscar Pedidos no SIGE
        const resSige = await axios.get("https://api.sigecloud.com.br/request/Pedidos/Pesquisar", {
            headers: sigeHeaders,
            params: {
                status: "Pedido Faturado",
                dataInicial: dataFormatada,
                dataFinal: dataFormatada,
                filtrarPor: 3, // Data de Faturamento
                pageSize: 100
            }
        });

        const pedidos = resSige.data || [];
        secureLog(`Pedidos encontrados: ${pedidos.length}`);

        if (pedidos.length === 0) {
            secureLog("Nenhum dado novo para processar.");
            return;
        }

        // 3. Tratar dados
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

        // 4. Mandar para o Google Sheets (Aba: Faturamento)
        secureLog("Enviando dados para a aba Faturamento (Modo Append)...");
        
        const range = "Faturamento!A:A:append";
        const url = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(range)}?valueInputOption=USER_ENTERED`;

        await axios.post(url, { values: rows }, { 
            headers: { 
                'Authorization': `Bearer ${GOOGLE_TOKEN}`,
                'Content-Type': 'application/json' 
            } 
        });

        secureLog("Processo concluído com sucesso. Dados inseridos após o cabeçalho.");

    } catch (err) {
        secureLog(`Erro na execução: ${err.message}`, true);
        process.exit(1);
    }
}

run();
