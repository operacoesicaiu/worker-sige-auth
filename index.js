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

// Helper para formatar data ISO para DD/MM/YYYY
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

        // 1. Definir período (Ontem)
        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dataBusca = ontem.toISOString().split('T')[0];

        secureLog(`Iniciando busca de pedidos para o dia: ${dataBusca}`);

        // 2. Buscar Pedidos Faturados
        const resSige = await axios.get("https://api.sigecloud.com.br/request/Pedidos/Pesquisar", {
            headers: sigeHeaders,
            params: {
                status: "Pedido Faturado",
                dataInicial: dataBusca,
                dataFinal: dataBusca,
                filtrarPor: 3, // Data de Faturamento
                pageSize: 100
            }
        });

        const pedidos = resSige.data || [];
        secureLog(`Pedidos encontrados: ${pedidos.length}`);

        if (pedidos.length === 0) return;

        const rows = [];

        // 3. Para cada pedido, buscar detalhes do cliente (Conforme sige_api.js)
        for (const p of pedidos) {
            try {
                secureLog(`Processando Pedido ${p.Codigo} - Cliente: ${p.Cliente}`);
                
                // Busca detalhada do cliente para pegar Telefone e outros campos
                const resCliente = await axios.get(`https://api.sigecloud.com.br/request/Clientes/Obter/${p.ClienteID}`, {
                    headers: sigeHeaders
                });
                const c = resCliente.data || {};

                // Montagem das colunas seguindo a ordem do seu sige_api.js
                rows.push([
                    p.Codigo,                                      // CódigoVenda
                    sanitize(p.StatusSistema || ""),              // Status do Sistema
                    formatarDataBR(p.DataFaturamento || p.Data),  // Venda.Data (DD/MM/YYYY)
                    sanitize(c.NomeFantasia || p.Cliente || ""),  // Cliente.Nome Fantasia
                    sanitize(c.Telefone || ""),                   // Cliente.Telefone
                    sanitize(c.Email || p.ClienteEmail || ""),    // Cliente.E-mail
                    p.ValorFinal || 0,                            // Venda.Valor Total
                    sanitize(p.Vendedor || ""),                   // Venda.Vendedor
                    sanitize(p.NumeroNFe || ""),                  // Nº Documento
                    sanitize(p.ClienteCNPJ || "")                 // Cliente.CPF/CNPJ
                ]);

            } catch (errCliente) {
                secureLog(`Erro ao buscar cliente do pedido ${p.Codigo}: ${errCliente.message}`, true);
                // Caso falhe o cliente, insere com dados básicos do pedido para não perder a linha
                rows.push([p.Codigo, p.StatusSistema, formatarDataBR(p.DataFaturamento), p.Cliente, "", p.ClienteEmail, p.ValorFinal, p.Vendedor, p.NumeroNFe, p.ClienteCNPJ]);
            }
        }

        // 4. Enviar para o Google Sheets na aba Faturamento
        secureLog("Enviando dados para a aba Faturamento...");
        
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

        secureLog("Processo concluído com sucesso.");

    } catch (err) {
        secureLog(`Erro na execução: ${err.message}`, true);
        process.exit(1);
    }
}

run();
