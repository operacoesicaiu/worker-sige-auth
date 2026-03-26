const axios = require('axios');

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
    if (!dataISO) return "";
    const data = new Date(dataISO);
    return data.toLocaleDateString('pt-BR');
}

async function run() {
    try {
        const { SIGE_TOKEN, SIGE_USER, SIGE_APP, GOOGLE_TOKEN, SPREADSHEET_ID } = process.env;

        const sigeHeaders = {
            "Authorization-Token": SIGE_TOKEN,
            "User": SIGE_USER,
            "App": SIGE_APP,
            "Content-Type": "application/json",
        };

        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dataBusca = ontem.toISOString().split('T')[0];

        secureLog(`Iniciando extração SIGE para o dia: ${dataBusca}`);

        // 1. Busca de Pedidos
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

        const rows = [];

        // 2. Chamada EXTRA OBRIGATÓRIA para cada cliente
        for (const p of pedidos) {
            try {
                // Importante: Usamos p.ClienteID para a chamada extra
                const resCliente = await axios.get(`https://api.sigecloud.com.br/request/Clientes/Obter/${p.ClienteID}`, {
                    headers: sigeHeaders
                });
                
                const c = resCliente.data || {};

                // Lógica de Documento (Coluna J) conforme sige_api.js
                const numNF = p.NumeroNFe || "";
                const documentoFormatado = `Pedido ${p.Codigo}${numNF ? ' / NF Nº ' + numNF : ''}.`;

                // Montagem na ordem exata solicitada
                rows.push([
                    sanitize(c.Celular || ""),                    // A - Cliente Celular
                    p.Codigo,                                     // B - Código
                    sanitize(p.StatusSistema || ""),              // C - Venda Status do Sistema
                    formatarDataBR(p.DataFaturamento || p.Data),  // D - Venda Data
                    sanitize(c.NomeFantasia || p.Cliente || ""),  // E - Cliente Nome Fantasia
                    sanitize(c.Telefone || ""),                   // F - Cliente Telefone
                    sanitize(c.Email || p.ClienteEmail || ""),    // G - Cliente E-mail
                    p.ValorFinal || 0,                            // H - Venda Valor Total
                    sanitize(p.Vendedor || ""),                   // I - Venda Vendedor
                    sanitize(documentoFormatado),                 // J - Nº Documento
                    sanitize(p.ClienteCNPJ || "")                 // K - Cliente CPF/CNPJ
                ]);

            } catch (errCliente) {
                // Se o 404 persistir em algum ID, este log ajudará a identificar qual ID está falhando
                secureLog(`Falha na chamada extra do ClienteID ${p.ClienteID} (Pedido ${p.Codigo}): ${errCliente.message}`, true);
                
                // Fallback para não perder a linha na planilha caso um cliente específico dê erro
                const docErr = `Pedido ${p.Codigo}${p.NumeroNFe ? ' / NF Nº ' + p.NumeroNFe : ''}.`;
                rows.push(["", p.Codigo, p.StatusSistema, formatarDataBR(p.DataFaturamento), p.Cliente, "", p.ClienteEmail, p.ValorFinal, p.Vendedor, docErr, p.ClienteCNPJ]);
            }
        }

        // 3. Envio para Google Sheets
        secureLog("Enviando dados consolidados para o Google Sheets...");
        
        await axios.post(
            `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Faturamento:append?valueInputOption=USER_ENTERED`,
            { values: rows },
            { headers: { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' } }
        );

        secureLog("Processo concluído com sucesso.");

    } catch (err) {
        secureLog(`Erro Crítico no Fluxo: ${err.message}`, true);
        process.exit(1);
    }
}

run();
