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

// Converte "25/03/2026" para Objeto Date
function parseDataBR(str) {
    if (!str || typeof str !== 'string') return null;
    const partes = str.split('/');
    if (partes.length !== 3) return null;
    return new Date(partes[2], partes[1] - 1, partes[0]);
}

async function run() {
    try {
        const { SIGE_TOKEN, SIGE_USER, SIGE_APP, GOOGLE_TOKEN, SPREADSHEET_ID, ERP_SPREADSHEET_ID } = process.env;
        const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };
        const sigeHeaders = { "Authorization-Token": SIGE_TOKEN, "User": SIGE_USER, "App": SIGE_APP, "Content-Type": "application/json" };

        // 1. BUSCAR DADOS DO ERP (Apenas colunas necessárias para reduzir volume de dados)
        // Buscamos o range que contém as colunas até AH (34 colunas)
        secureLog("Solicitando colunas específicas do ERP (otimizando tráfego)...");
        const resErp = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${ERP_SPREADSHEET_ID}/values/ERP!A:AH`, { headers: gHeaders });
        const erpRows = resErp.data.values || [];
        
        // Índices das colunas (Base 0): D=3, G=6, N=13, Q=16, T=19, AH=33
        const COL = { CPF: 3, TIPO: 6, RESP: 13, CHAVE: 16, DATA: 19, MESANO: 33 };

        // 2. BUSCAR PEDIDOS SIGE (DIA ANTERIOR)
        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dataBusca = ontem.toISOString().split('T')[0];

        const resSige = await axios.get("https://api.sigecloud.com.br/request/Pedidos/Pesquisar", {
            headers: sigeHeaders,
            params: { status: "Pedido Faturado", dataInicial: dataBusca, dataFinal: dataBusca, filtrarPor: 3, pageSize: 100 }
        });

        const pedidos = resSige.data || [];
        if (pedidos.length === 0) return secureLog("Nenhum pedido para processar.");

        const rowsFinal = [];

        for (const p of pedidos) {
            let c = {};
            const clienteCpf = p.ClienteCNPJ || "";
            
            // Busca detalhes do cliente para Celular/Telefone
            if (clienteCpf) {
                try {
                    const resP = await axios.get("https://api.sigecloud.com.br/request/Pessoas/Pesquisar", {
                        headers: sigeHeaders, params: { cpfcnpj: clienteCpf }
                    });
                    if (resP.data?.length > 0) c = resP.data[0];
                } catch (e) { secureLog(`Erro cliente ${p.Codigo}`); }
            }

            const dataVenda = new Date(p.DataFaturamento || p.Data);
            const dataLimite6Meses = new Date(dataVenda);
            dataLimite6Meses.setMonth(dataLimite6Meses.getMonth() - 6);

            const getMesAno = (date, offset) => {
                const d = new Date(date);
                d.setMonth(d.getMonth() + offset);
                return `${String(d.getMonth() + 1).padStart(2, '0')}/${d.getFullYear()}`;
            };

            const mesesInteresse = [getMesAno(dataVenda, -2), getMesAno(dataVenda, -1), getMesAno(dataVenda, 0)];

            let dataNovoServico = "";
            let dataRetirada = "";

            // BUSCA DE BAIXO PARA CIMA (Otimizada para parar em 6 meses)
            for (let i = erpRows.length - 1; i >= 1; i--) {
                const r = erpRows[i];
                if (!r[COL.DATA]) continue;

                const dataErpObj = parseDataBR(r[COL.DATA]);
                if (!dataErpObj) continue;

                // CRITÉRIO DE PARADA: Se a data da linha for anterior a 6 meses da venda, para de olhar esta venda
                if (dataErpObj < dataLimite6Meses) break;
                
                // Se a data for futura à venda, ignora e continua descendo
                if (dataErpObj > dataVenda) continue;

                if (r[COL.CPF] === clienteCpf) {
                    const mesAnoLinha = r[COL.MESANO];
                    const tipoLinha = r[COL.TIPO];

                    if (!dataNovoServico && tipoLinha === "Novo Serviço" && mesesInteresse.includes(mesAnoLinha)) {
                        dataNovoServico = r[COL.DATA];
                    }
                    if (!dataRetirada && tipoLinha === "Retirada" && mesesInteresse.includes(mesAnoLinha)) {
                        dataRetirada = r[COL.DATA];
                    }
                }
                
                // Se já achou os dois, pode parar a busca para este pedido
                if (dataNovoServico && dataRetirada) break;
            }

            // INDEX MATCH das colunas M e P
            const buscarAgendador = (dataEncontrada) => {
                if (!dataEncontrada) return "Sem vendedor";
                const chaveAlvo = dataEncontrada + clienteCpf;
                // Busca novamente no ERP a linha que tem essa chave na coluna Q (16)
                const linha = erpRows.find(r => r[COL.CHAVE] === chaveAlvo);
                return linha ? linha[COL.RESP] : "Sem vendedor";
            };

            const respM = buscarAgendador(dataNovoServico);
            const respP = buscarAgendador(dataRetirada);
            
            const valorTotal = p.ValorFinal || 0;
            const valN = dataRetirada !== "" ? valorTotal * 0.5 : valorTotal;
            const valQ = dataRetirada !== "" ? valorTotal * 0.5 : 0;
            const valR = `${dataVenda.getMonth() + 1}/${dataVenda.getFullYear()}`;

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
                dataNovoServico, // L
                sanitize(respM), // M
                valN, // N
                dataRetirada, // O
                sanitize(respP), // P
                valQ, // Q
                valR  // R
            ]);
        }

        // Envio para aba Faturamento
        if (rowsFinal.length > 0) {
            secureLog(`Salvando ${rowsFinal.length} registros...`);
            await axios.post(
                `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Faturamento:append?valueInputOption=USER_ENTERED`,
                { values: rowsFinal }, { headers: gHeaders }
            );
        }
        secureLog("Finalizado.");
    } catch (err) {
        secureLog(`Erro: ${err.message}`, true);
        process.exit(1);
    }
}

run();
