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

// Converte Data do JS para Número Serial do Excel (ex: 46106)
function dateToExcelSerial(date) {
    const returnDateTime = 25569.0 + (date.getTime() - (date.getTimezoneOffset() * 60 * 1000)) / (1000 * 60 * 60 * 24);
    return Math.floor(returnDateTime);
}

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

        secureLog("Baixando dados do ERP...");
        const resErp = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${ERP_SPREADSHEET_ID}/values/ERP!A:AH`, { headers: gHeaders });
        const erpRows = resErp.data.values || [];
        
        // Mapeamento: D=3 (CPF), G=6 (Tipo), N=13 (Resp), Q=16 (Chave), T=19 (Data), AH=33 (MM/YYYY)
        const COL = { CPF: 3, TIPO: 6, RESP: 13, CHAVE: 16, DATA: 19, MESANO: 33 };

        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dataBusca = ontem.toISOString().split('T')[0];

        const resSige = await axios.get("https://api.sigecloud.com.br/request/Pedidos/Pesquisar", {
            headers: sigeHeaders,
            params: { status: "Pedido Faturado", dataInicial: dataBusca, dataFinal: dataBusca, filtrarPor: 3, pageSize: 100 }
        });

        const pedidos = resSige.data || [];
        if (pedidos.length === 0) return secureLog("Sem pedidos.");

        const rowsFinal = [];

        for (const p of pedidos) {
            let c = {};
            const clienteCpf = (p.ClienteCNPJ || "").replace(/\D/g, ""); // Apenas números para comparar
            
            if (clienteCpf) {
                try {
                    const resP = await axios.get("https://api.sigecloud.com.br/request/Pessoas/Pesquisar", {
                        headers: sigeHeaders, params: { cpfcnpj: p.ClienteCNPJ }
                    });
                    if (resP.data?.length > 0) c = resP.data[0];
                } catch (e) {}
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

            let rawDataNovoServico = "";
            let rawDataRetirada = "";

            // Busca reversa no ERP
            for (let i = erpRows.length - 1; i >= 1; i--) {
                const r = erpRows[i];
                if (!r[COL.DATA]) continue;

                const dataErpObj = parseDataBR(r[COL.DATA]);
                if (!dataErpObj || dataErpObj < dataLimite6Meses) {
                    if (dataErpObj && dataErpObj < dataLimite6Meses) break; 
                    continue;
                }
                
                if (dataErpObj > dataVenda) continue;

                const erpCpfLimpo = (r[COL.CPF] || "").replace(/\D/g, "");
                if (erpCpfLimpo === clienteCpf) {
                    const tipo = (r[COL.TIPO] || "").trim();
                    const mesAno = r[COL.MESANO];

                    if (!rawDataNovoServico && tipo === "Novo Serviço" && mesesInteresse.includes(mesAno)) {
                        rawDataNovoServico = r[COL.DATA];
                    }
                    if (!rawDataRetirada && tipo === "Retirada" && mesesInteresse.includes(mesAno)) {
                        rawDataRetirada = r[COL.DATA];
                    }
                }
                if (rawDataNovoServico && rawDataRetirada) break;
            }

            // Simulação do CORRESP/ÍNDICE (Chave é Data + CPF original da linha)
            const buscarResp = (dataEncontrada) => {
                if (!dataEncontrada) return "Sem vendedor";
                // Procura a linha onde a Coluna Q (Chave) bate exatamente
                const chaveAlvo = dataEncontrada + p.ClienteCNPJ;
                const match = erpRows.find(r => r[COL.CHAVE] === chaveAlvo);
                return match ? match[COL.RESP] : "Sem vendedor";
            };

            const respM = buscarResp(rawDataNovoServico);
            const respP = buscarResp(rawDataRetirada);

            // Conversão para Número Serial para as colunas L e O (estilo 46106)
            const excelL = rawDataNovoServico ? dateToExcelSerial(parseDataBR(rawDataNovoServico)) : "";
            const excelO = rawDataRetirada ? dateToExcelSerial(parseDataBR(rawDataRetirada)) : "";

            const valorTotal = p.ValorFinal || 0;
            const valN = rawDataRetirada !== "" ? valorTotal * 0.5 : valorTotal;
            const valQ = rawDataRetirada !== "" ? valorTotal * 0.5 : 0;
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
                sanitize(p.ClienteCNPJ || ""), // K
                excelL, // L (Número Serial 46106)
                sanitize(respM), // M
                valN, // N
                excelO, // O (Número Serial 46106)
                sanitize(respP), // P
                valQ, // Q
                valR  // R
            ]);
        }

        if (rowsFinal.length > 0) {
            await axios.post(
                `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Faturamento:append?valueInputOption=USER_ENTERED`,
                { values: rowsFinal }, { headers: gHeaders }
            );
        }
        secureLog("Sucesso.");
    } catch (err) {
        secureLog(`Erro: ${err.message}`, true);
        process.exit(1);
    }
}
run();
