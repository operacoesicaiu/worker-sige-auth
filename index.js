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

        secureLog(`Buscando pedidos faturados em: ${dataBusca}`);

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

        for (const p of pedidos) {
            try {
                secureLog(`Buscando detalhes do cliente: ${p.Cliente}`);
                
                const resCliente = await axios.get(`https://api.sigecloud.com.br/request/Clientes/Obter/${p.ClienteID}`, {
                    headers: sigeHeaders
                });
                const c = resCliente.data || {};

                // ORDEM SOLICITADA:
                // Celular, Código, Status, Data, Nome Fantasia, Telefone, Email, Valor, Vendedor, Doc, CNPJ
                rows.push([
                    sanitize(c.Celular || ""),                    // Cliente Celular
                    p.Codigo,                                     // Código
                    sanitize(p.StatusSistema || ""),              // Venda Status do Sistema
                    formatarDataBR(p.DataFaturamento || p.Data),  // Venda Data
                    sanitize(c.NomeFantasia || p.Cliente || ""),  // Cliente Nome Fantasia
                    sanitize(c.Telefone || ""),                   // Cliente Telefone
                    sanitize(c.Email || p.ClienteEmail || ""),    // Cliente E-mail
                    p.ValorFinal || 0,                            // Venda Valor Total
                    sanitize(p.Vendedor || ""),                   // Venda Vendedor
                    sanitize(p.NumeroNFe || ""),                  // Nº Documento
                    sanitize(p.ClienteCNPJ || "")                 // Cliente CPF/CNPJ
                ]);

            } catch (errCliente) {
                secureLog(`Erro nos detalhes do cliente (Pedido ${p.Codigo}): ${errCliente.message}`, true);
                // Fallback para não travar o processo
                rows.push(["", p.Codigo, p.StatusSistema, formatarDataBR(p.DataFaturamento), p.Cliente, "", p.ClienteEmail, p.ValorFinal, p.Vendedor, p.NumeroNFe, p.ClienteCNPJ]);
            }
        }

        secureLog("Enviando dados formatados para o Google Sheets...");
        
        await axios.post(
            `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Faturamento:append?valueInputOption=USER_ENTERED`,
            { values: rows },
            { 
                headers: { 
                    'Authorization': `Bearer ${GOOGLE_TOKEN}`,
                    'Content-Type': 'application/json' 
                } 
            }
        );

        secureLog("Finalizado com sucesso.");

    } catch (err) {
        secureLog(`Erro crítico: ${err.message}`, true);
        process.exit(1);
    }
}

run();
