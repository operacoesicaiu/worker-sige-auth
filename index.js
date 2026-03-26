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
    if (!dateStr || typeof dateStr !== 'string' || !dateStr.includes('/')) return "";
    const partes = dateStr.split('/');
    const date = new Date(partes[2], partes[1] - 1, partes[0]);
    if (isNaN(date)) return "";
    const returnDateTime = 25569.0 + (date.getTime() - (date.getTimezoneOffset() * 60 * 1000)) / (1000 * 60 * 60 * 24);
    return Math.floor(returnDateTime);
}

// --- NOVOS HELPERS ---

function buscarDataRetirada(erpRows, cpfLimpo, dataVenda, COL) {
    let maxDataSerial = 0;
    const dataLimite = new Date(dataVenda);
    
    erpRows.forEach(r => {
        const erpCpfLimpo = (r[COL.CPF] || "").replace(/\D/g, "");
        const tipo = (r[COL.TIPO] || "").toLowerCase();
        const dataERPStr = r[COL.DATA]; 
        
        if (erpCpfLimpo === cpfLimpo && tipo.includes("retirada") && dataERPStr) {
            const partes = dataERPStr.split('/');
            if (partes.length === 3) {
                const dataERP = new Date(partes[2], partes[1] - 1, partes[0]);
                if (dataERP <= dataLimite) {
                    const serial = dateToExcelSerial(dataERPStr);
                    if (serial > maxDataSerial) maxDataSerial = serial;
                }
            }
        }
    });
    return maxDataSerial > 0 ? maxDataSerial : "";
}

const buscarResp = (dataSerialBuscada, cpfLimpo, erpRows, COL) => {
    if (!dataSerialBuscada) return "Sem vendedor";

    // Remove o apóstrofo caso ele exista para fazer a comparação numérica
    const serialLimpo = Number(dataSerialBuscada.toString().replace("'", ""));

    const match = erpRows.find(r => {
        const erpCpfLimpo = (r[COL.CPF] || "").replace(/\D/g, "");
        const erpDataSerial = dateToExcelSerial(r[COL.DATA]);
        
        return (erpDataSerial === serialLimpo && erpCpfLimpo === cpfLimpo);
    });

    return match ? match[COL.RESP] : "Sem vendedor";
};

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
        
        // Mapeamento baseado no CSV: D=3(CPF), G=6(Tipo), P=15(Responsável), Q=16(Chave), T=19(Data)
        const COL = { CPF: 3, TIPO: 6, RESP: 15, CHAVE: 16, DATA: 19 };

        // 2. Busca Pedidos faturados de 01/03/2026 a 26/03/2026
        const startDate = new Date('2026-03-01');
        const endDate = new Date('2026-03-26');
        const pedidos = [];
        
        secureLog(`Iniciando busca de pedidos SIGE de ${startDate.toISOString().split('T')[0]} a ${endDate.toISOString().split('T')[0]}`);
        
        // Loop através de cada dia no intervalo
        for (let currentDate = new Date(startDate); currentDate <= endDate; currentDate.setDate(currentDate.getDate() + 1)) {
            const dataBusca = currentDate.toISOString().split('T')[0];
            secureLog(`Buscando pedidos SIGE para: ${dataBusca}`);
            
            try {
                const resSige = await axios.get("https://api.sigecloud.com.br/request/Pedidos/Pesquisar", {
                    headers: sigeHeaders,
                    params: { status: "Pedido Faturado", dataInicial: dataBusca, dataFinal: dataBusca, filtrarPor: 3, pageSize: 100 }
                });
                
                const diaPedidos = resSige.data || [];
                if (diaPedidos.length > 0) {
                    pedidos.push(...diaPedidos);
                    secureLog(`Dia ${dataBusca}: ${diaPedidos.length} pedidos encontrados`);
                } else {
                    secureLog(`Dia ${dataBusca}: Nenhum pedido encontrado`);
                }
            } catch (error) {
                secureLog(`Erro ao buscar pedidos para ${dataBusca}: ${error.message}`, true);
                // Continua com os próximos dias mesmo se houver erro em um dia específico
            }
        }
        
        if (pedidos.length === 0) {
            secureLog("Nenhum pedido encontrado no período especificado.");
            return;
        }
        
        secureLog(`Total de pedidos coletados: ${pedidos.length}`);

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
                } catch (e) { secureLog("Erro API Pessoas para CPF"); }
            }

            // --- LÓGICA COLUNA L (Novo Serviço) ---
            // Busca a data do tipo "Novo" mais próxima/recente
           let serialNovo = "";
            let respNovo = "Sem vendedor";
            let serialRetirada = "";
            let respRetirada = "Sem vendedor";

            const dataVenda = new Date(p.DataFaturamento || p.Data);
            const valorTotal = p.ValorFinal || 0;

            // Busca os dados de "Novo" e "Retirada" em uma única passagem
            erpRows.slice().reverse().forEach(r => {
                const erpCpfLimpo = (r[COL.CPF] || "").replace(/\D/g, "");
                if (erpCpfLimpo !== clienteCpfLimpo) return;

                const tipo = (r[COL.TIPO] || "").toLowerCase();
                const dataERPStr = r[COL.DATA];
                
                // Se for NOVO e ainda não achamos o mais recente
                if (tipo.includes("novo") && serialNovo === "") {
                    serialNovo = dateToExcelSerial(dataERPStr);
                    respNovo = r[COL.RESP] || "Sem vendedor";
                }

                // Se for RETIRADA e for antes/no dia da venda
                if (tipo.includes("retirada") && serialRetirada === "") {
                    const partes = dataERPStr.split('/');
                    const dataERP = new Date(partes[2], partes[1] - 1, partes[0]);
                    if (dataERP <= dataVenda) {
                        serialRetirada = dateToExcelSerial(dataERPStr);
                        respRetirada = r[COL.RESP] || "Sem vendedor";
                    }
                }
            });

            // Ajuste para não virar data e inserir 0 se estiver vazio
            const displayNovo = serialNovo !== "" ? `'${serialNovo}` : 0;
            const displayRetirada = serialRetirada !== "" ? `'${serialRetirada}` : 0;

            // --- MONTAGEM DA LINHA ---
            rowsFinal.push([
                sanitize((c.Celular || "").replace("+", "")), // A
                p.Codigo, // B
                sanitize(p.StatusSistema || ""), // C
                formatarDataBR(dataVenda), // D
                sanitize(c.NomeFantasia || p.Cliente || ""), // E
                sanitize(c.Telefone || ""), // F
                sanitize(c.Email || p.ClienteEmail || ""), // G
                valorTotal, // H
                sanitize(p.Vendedor || ""), // I
                sanitize(`Pedido ${p.Codigo}${p.NumeroNFe ? ' / NF Nº ' + p.NumeroNFe : ''}`), // J
                sanitize(clienteCpf), // K
                displayNovo,          // L (Envia '46106 ou 0)
                sanitize(respNovo),   // M
                serialRetirada !== "" ? (valorTotal * 0.5) : valorTotal, // N
                displayRetirada,      // O (Envia '46106 ou 0)
                sanitize(respRetirada), // P
                serialRetirada !== "" ? (valorTotal * 0.5) : 0,          // Q
                `${dataVenda.getMonth() + 1}/${dataVenda.getFullYear()}` // R
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
        secureLog(`Erro Crítico na execução. Verifique as credenciais e conexão.`, true);
        process.exit(1);
    }
}

run();
