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

// Helper para formatar mês/ano (MM/YYYY) para as buscas
function getMonthYear(date, offset = 0) {
    const d = new Date(date);
    d.setMonth(d.getMonth() + offset);
    const m = (d.getMonth() + 1).toString().padStart(2, '0');
    return `${m}/${d.getFullYear()}`;
}

async function run() {
    try {
        const { 
            SIGE_TOKEN, SIGE_USER, SIGE_APP, 
            GOOGLE_TOKEN, SPREADSHEET_ID, ERP_SPREADSHEET_ID 
        } = process.env;

        const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };

        // 1. Buscar dados da Planilha ERP (Últimos 6 meses) para cruzamento
        secureLog("Buscando dados da aba ERP para cruzamento...");
        const resErp = await axios.get(
            `https://sheets.googleapis.com/v4/spreadsheets/${ERP_SPREADSHEET_ID}/values/ERP!A:AH`, 
            { headers: gHeaders }
        );
        const erpData = resErp.data.values || [];
        const erpHeaders = erpData[0] || [];
        
        // Mapeamento de índices da ERP (baseado na sua descrição de colunas)
        // D=3, G=6, N=13, Q=16, T=19, AH=33
        const erpRows = erpData.slice(1);

        // 2. Buscar dados do SIGE (Ontem)
        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dataBusca = ontem.toISOString().split('T')[0];
        
        secureLog(`Buscando pedidos SIGE: ${dataBusca}`);
        const resSige = await axios.get("https://api.sigecloud.com.br/request/Pedidos/Pesquisar", {
            headers: { "Authorization-Token": SIGE_TOKEN, "User": SIGE_USER, "App": SIGE_APP },
            params: { status: "Pedido Faturado", dataInicial: dataBusca, dataFinal: dataBusca, filtrarPor: 3 }
        });

        const pedidos = resSige.data || [];
        if (pedidos.length === 0) return secureLog("Sem dados.");

        // 3. Processar cada pedido com a lógica das colunas J-P
        const finalRows = pedidos.map(p => {
            const dataOriginal = p.DataFaturamento || p.Data || "";
            const dataObj = new Date(dataOriginal);
            
            // Coluna D: Data Formatada
            const colD_Data = dataObj.toLocaleDateString('pt-BR');
            const colA_CNPJ = p.ClienteCNPJ || "";
            const colF_Valor = p.ValorFinal || 0;

            // --- Lógica Interna para substituir MAXIFS ---
            const findMaxT = (tipoServico, mesesOffset) => {
                const mesesAlvo = mesesOffset.map(offset => getMonthYear(dataObj, offset));
                let maxT = 0;
                erpRows.forEach(erp => {
                    const dataERP = erp[19] || 0; // Coluna T
                    const cnpjERP = erp[3];       // Coluna D
                    const mesAnoERP = erp[33];    // Coluna AH
                    const tipoERP = erp[6];       // Coluna G
                    
                    if (cnpjERP === colA_CNPJ && 
                        mesesAlvo.includes(mesAnoERP) && 
                        tipoERP === tipoServico && 
                        parseFloat(dataERP) <= parseFloat(dataOriginal)) {
                        maxT = Math.max(maxT, parseFloat(dataERP));
                    }
                });
                return maxT;
            };

            // --- Cálculos das Colunas ---
            
            // Coluna J: MAXIFS (Novo Serviço -2, -1, 0 meses)
            const colJ = findMaxT("Novo Serviço", [-2, -1, 0]);

            // Coluna K: INDEX/MATCH (Responsável Agendamento via J+A)
            const matchKeyK = colJ + colA_CNPJ;
            const erpMatchK = erpRows.find(r => (r[16] || "") === matchKeyK); // Procura na Q (16)
            const colK = erpMatchK ? erpMatchK[13] : "Sem vendedor"; // Pega da N (13)

            // Coluna M: MAXIFS (Retirada -2, -1, 0 meses)
            const colM = findMaxT("Retirada", [-2, -1, 0]);

            // Coluna L: =SE(M<>0; F*0,5; F)
            const colL = colM !== 0 ? colF_Valor * 0.5 : colF_Valor;

            // Coluna N: INDEX/MATCH (Responsável Agendamento via M+A)
            const matchKeyN = colM + colA_CNPJ;
            const erpMatchN = erpRows.find(r => (r[16] || "") === matchKeyN);
            const colN = erpMatchN ? erpMatchN[13] : "Sem vendedor";

            // Coluna O: =SE(M<>0; F*0,5; 0)
            const colO = colM !== 0 ? colF_Valor * 0.5 : 0;

            // Coluna P: Mês/Ano (M/YYYY)
            const colP = `${dataObj.getMonth() + 1}/${dataObj.getFullYear()}`;

            return [
                sanitize(colA_CNPJ),          // A
                sanitize(p.Cliente || ""),    // B
                sanitize(p.ClienteEmail || ""),// C
                colD_Data,                    // D (Formatada BR)
                `Pedido ${p.Codigo}`,         // E
                colF_Valor,                   // F
                sanitize(p.Vendedor || ""),   // G
                sanitize(p.StatusSistema || ""),// H
                p.Codigo,                     // I
                colJ,                         // J (Calculado)
                sanitize(colK),               // K (Calculado)
                colL,                         // L (Calculado)
                colM,                         // M (Calculado)
                sanitize(colN),               // N (Calculado)
                colO,                         // O (Calculado)
                colP                          // P (Calculado)
            ];
        });

        // 4. Enviar para Google Sheets
        secureLog(`Enviando ${finalRows.length} linhas para Faturamento...`);
        await axios.post(
            `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Faturamento:append?valueInputOption=USER_ENTERED`,
            { values: finalRows },
            { headers: gHeaders }
        );

        secureLog("Sucesso total.");

    } catch (err) {
        secureLog(`Erro: ${err.message}`, true);
        process.exit(1);
    }
}

run();
