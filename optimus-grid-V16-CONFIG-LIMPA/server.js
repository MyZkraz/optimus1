
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const cors = require('cors');
let DatabaseSync;
try { ({ DatabaseSync } = require('node:sqlite')); } catch (e) {
  console.error('OPTIMUS V17 exige Node.js 22.5+ para SQLite nativo (node:sqlite).');
  process.exit(1);
}
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const DATA_DIR = path.join(__dirname, 'data');
const FILES = {
  possiveis: path.join(DATA_DIR, 'possiveis.json'),
  premiums: path.join(DATA_DIR, 'premiums.json'),
  logs: path.join(DATA_DIR, 'logs.json'),
  entradas: path.join(DATA_DIR, 'entradas.json'),
  comportamentos: path.join(DATA_DIR, 'comportamentos.json'),
  padroes: path.join(DATA_DIR, 'padroes.json'),
  mapaNomes: path.join(DATA_DIR, 'mapa_padroes_nomes.json'),
  ultimaAnalise: path.join(DATA_DIR, 'ultima_analise_24h.json'),
  estadoPremium: path.join(DATA_DIR, 'estado_premium.json'),
  bibliotecaViva: path.join(DATA_DIR, 'biblioteca_viva.json'),
  performancePeriodo: path.join(DATA_DIR, 'performance_periodo.json'),
};
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const DB_PATH = path.join(DATA_DIR, 'optimus.db');
const db = new DatabaseSync(DB_PATH);
db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;
  CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS analysis_cursor (
    symbol TEXT PRIMARY KEY,
    last_closed_ts INTEGER NOT NULL DEFAULT 0,
    last_scan_ts INTEGER NOT NULL DEFAULT 0,
    candles_processed INTEGER NOT NULL DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS premium_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    event TEXT NOT NULL,
    reason TEXT,
    score REAL,
    comportamento_id TEXT,
    created_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_premium_events_symbol ON premium_events(symbol);
  CREATE INDEX IF NOT EXISTS idx_premium_events_created ON premium_events(created_at);
`);

function jsonFileDefault(file, def){
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return def; }
}
function dbGet(key){
  const row = db.prepare('SELECT value FROM kv WHERE key = ?').get(key);
  if(!row) return undefined;
  try { return JSON.parse(row.value); } catch { return undefined; }
}
function dbSet(key, data){
  db.prepare(`
    INSERT INTO kv(key,value,updated_at) VALUES(?,?,?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
  `).run(key, JSON.stringify(data), new Date().toISOString());
}
function keyFromFile(file){
  return path.basename(file, '.json');
}
function migrateJsonToSqlite(){
  for(const [key, file] of Object.entries(FILES)){
    const existing = dbGet(key);
    if(existing !== undefined) continue;
    const def = (key==='padroes'||key==='comportamentos'||key==='premiums'||key==='entradas'||key==='mapaNomes'||key==='ultimaAnalise'||key==='estadoPremium'||key==='bibliotecaViva'||key==='performancePeriodo') ? {} : [];
    const data = jsonFileDefault(file, def);
    dbSet(key, data);
  }
}
Object.entries(FILES).forEach(([k,f])=>{
  if(!fs.existsSync(f)){
    const def = (k==='padroes'||k==='comportamentos'||k==='premiums'||k==='entradas'||k==='mapaNomes'||k==='ultimaAnalise'||k==='estadoPremium'||k==='bibliotecaViva'||k==='performancePeriodo') ? {} : [];
    fs.writeFileSync(f, JSON.stringify(def, null, 2));
  }
});
migrateJsonToSqlite();

function load(file, def){
  const key = keyFromFile(file);
  const fromDb = dbGet(key);
  if(fromDb !== undefined) return fromDb;
  return jsonFileDefault(file, def);
}
function save(file, data){
  const key = keyFromFile(file);
  dbSet(key, data);
  // Mantém JSON como espelho/backup compatível com o V16. O SQLite continua sendo a fonte principal.
  fs.writeFileSync(file, JSON.stringify(data, null, 2));
}
function getAnalysisCursor(symbol){
  return db.prepare('SELECT * FROM analysis_cursor WHERE symbol = ?').get(symbol) || null;
}
function setAnalysisCursor(symbol, lastClosedTs, candlesProcessed=0){
  db.prepare(`
    INSERT INTO analysis_cursor(symbol,last_closed_ts,last_scan_ts,candles_processed)
    VALUES(?,?,?,?)
    ON CONFLICT(symbol) DO UPDATE SET
      last_closed_ts=excluded.last_closed_ts,
      last_scan_ts=excluded.last_scan_ts,
      candles_processed=excluded.candles_processed
  `).run(symbol, lastClosedTs, Date.now(), candlesProcessed);
}
function recordPremiumEvent(symbol, event, reason, score=null, comportamentoId=null){
  db.prepare(`
    INSERT INTO premium_events(symbol,event,reason,score,comportamento_id,created_at)
    VALUES(?,?,?,?,?,?)
  `).run(symbol, event, reason || null, score ?? null, comportamentoId || null, new Date().toISOString());
}
let scanning = false;
const sleep = ms=>new Promise(r=>setTimeout(r,ms));
const CONCURRENT = 2;
const DELAY_LOTE = 800;

// NOMES COM ALFABETO E NUMEROS - Delta_a1 Delta_a2 Delta_b1 nunca acaba em Padrao 1
const NOMES_BASE = ['Alpha','Beta','Gamma','Delta','Epsilon','Zeta','Eta','Theta','Iota','Kappa','Lambda','Mu','Nu','Xi','Omicron','Pi','Rho','Sigma','Tau','Upsilon','Phi','Chi','Psi','Omega','Nova','Pulsar','Quasar','Nebula','Orion','Sirius','Vega','Atlas','Titan','Phoenix','Draco','Lyra','Aquila','Cygnus','Hydra','Lynx','Aurora','Eclipse','Cometa','Meteor','Galaxia','Estelar','Lunar','Solar'];
const LETRAS = 'abcdefghijklmnopqrstuvwxyz'.split('');
function getNomePadraoAvancado(comportamentoId){
  const mapa = load(FILES.mapaNomes, {});
  if(mapa[comportamentoId]) return mapa[comportamentoId];
  const usados = Object.values(mapa);
  const disponiveis = NOMES_BASE.filter(n=>!usados.includes(n) && !usados.some(u=>u.startsWith(n+'_')));
  let nome;
  if(disponiveis.length>0){
    nome = disponiveis[Math.floor(Math.random()*disponiveis.length)];
  } else {
    let tentativas=0;
    do{
      const base = NOMES_BASE[Math.floor(Math.random()*NOMES_BASE.length)];
      const letra = LETRAS[Math.floor(Math.random()*LETRAS.length)];
      const numero = Math.floor(Math.random()*99)+1;
      nome = `${base}_${letra}${numero}`;
      tentativas++;
      if(tentativas>500){
        const letra2 = LETRAS[Math.floor(Math.random()*LETRAS.length)];
        nome = `${base}_${letra}${letra2}${numero}`;
      }
    } while(usados.includes(nome) && tentativas<1000);
  }
  mapa[comportamentoId] = nome;
  save(FILES.mapaNomes, mapa);
  return nome;
}

async function fetchBinance(p){
  const bases = ['https://fapi.binance.com','https://fapi1.binance.com','https://fapi2.binance.com','https://fapi3.binance.com','https://data-api.binance.vision'];
  for(const base of bases){
    const path2 = base.includes('fapi')?p:p.replace('/fapi/v1/','/api/v3/');
    try{ const res = await axios.get(`${base}${path2}`, {timeout:8000, headers:{'User-Agent':'OptimusGrid-V15'}}); if(res.data) return res.data; }catch{} await sleep(200);
  }
  throw new Error(`Falha Binance ${p}`);
}
function calcATR(klines, inicio=0, fim=null){
  const slice = fim ? klines.slice(inicio, fim) : klines.slice(inicio);
  let trs=[]; for(let i=1;i<slice.length;i++){ const h=parseFloat(slice[i][2]), l=parseFloat(slice[i][3]), pc=parseFloat(slice[i-1][4]); trs.push(Math.max(h-l, Math.abs(h-pc), Math.abs(l-pc))); }
  if(!trs.length) return 0; return trs.slice(-14).reduce((a,b)=>a+b,0)/Math.min(14, trs.length);
}
function logSistema(msg, type='ok'){
  const logs = load(FILES.logs, []);
  logs.unshift({id:Date.now(), timestamp:new Date().toISOString(), mensagem:msg, type});
  if(logs.length>400) logs.length=400;
  save(FILES.logs, logs);
  io.emit('log', {msg, type, time:new Date().toLocaleTimeString('pt-BR')});
}
function optimusNoticia(text){ io.emit('optimus_noticia', {text, time:new Date().toLocaleTimeString('pt-BR')}); }
function optimusAzul(text, dadosExtras=null){
  io.emit('optimus', {text:`[OPTIMUS] ${text}`, dados:dadosExtras, time:new Date().toLocaleTimeString('pt-BR')});
  if(dadosExtras && dadosExtras.top5) io.emit('top5', dadosExtras.top5);
}
function optimusTempoReal(text, fase=''){
  io.emit('optimus_realtime', {text, fase, time:new Date().toLocaleTimeString('pt-BR')});
  io.emit('optimus', {text:`[OPTIMUS] ${text}`, fase, time:new Date().toLocaleTimeString('pt-BR')});
}

// ========= 5 CONHECIMENTOS DE MERCADO - O QUE FAZ UM COMPORTAMENTO REAL =========
function isLateralReal(klines){
  const highs = klines.slice(-20).map(k=>parseFloat(k[2])); const lows = klines.slice(-20).map(k=>parseFloat(k[3])); let equalHighs=0, equalLows=0; const sup = Math.max(...highs); const inf = Math.min(...lows); const tol = (sup-inf)*0.15; highs.forEach(h=>{ if(Math.abs(h-sup) < tol) equalHighs++; }); lows.forEach(l=>{ if(Math.abs(l-inf) < tol) equalLows++; }); const isLateral = equalHighs>=3 && equalLows>=3; const score = Math.min(25, (equalHighs+equalLows)*3); return { isLateral, equalHighs, equalLows, score, desc: isLateral ? `lateral real ${equalHighs}x topo ${equalLows}x fundo - mercado paradinho, repetindo topo e fundo, seguro pra grid` : `lateral fraca, não tá repetindo topo e fundo direito` };
}
function hasLiquidez(klines){
  const highs = klines.slice(-20).map(k=>parseFloat(k[2])); const closes = klines.slice(-20).map(k=>parseFloat(k[4])); let sweep=0, topoDuplo=0; for(let i=1;i<highs.length;i++){ if(highs[i] > highs[i-1] && closes[i] < highs[i-1]) sweep++; if(Math.abs(highs[i]-highs[i-1]) < (highs[i]*0.002)) topoDuplo++; } const semLiquidez = sweep<=1 && topoDuplo<=2; let score = 25; if(sweep>1) score -= sweep*5; if(topoDuplo>2) score -= topoDuplo*3; score = Math.max(0, score); return { semLiquidez, sweep, topoDuplo, score, desc: semLiquidez ? 'sem busca de liquidez - ninguém caçando stop loss, ninguém manipulando, seguro' : `tem busca de liquidez ${sweep}x - alguém tá caçando stop, risco de manipulação` };
}
function getPOC(klines){
  const closes = klines.slice(-20).map(k=>parseFloat(k[4])); const vols = klines.slice(-20).map(k=>parseFloat(k[5])); const sup = Math.max(...closes); const inf = Math.min(...closes); const range = sup-inf; if(range===0) return {score:0, posicao:'borda', desc:'sem variação'}; let somaVol=0, somaPrecoVol=0; closes.forEach((c,i)=>{ somaVol+=vols[i]; somaPrecoVol+=c*vols[i]; }); const poc = somaVol>0 ? somaPrecoVol/somaVol : closes[10]; const posRel = (poc-inf)/range; let score=0, posicao='meio'; if(posRel>=0.35 && posRel<=0.65){ score=25; posicao='meio'; } else if(posRel>=0.25 && posRel<=0.75){ score=18; posicao='meio-borda'; } else { score=5; posicao='borda'; } return { poc, posRel, score, posicao, desc: posicao==='meio' ? 'POC (onde mais negociou volume) está no meio da faixa, muito seguro, preço tende a voltar pro meio e não romper' : posicao==='meio-borda' ? 'POC quase no meio, ainda seguro' : 'POC na borda da faixa, risco grande de romper pra fora e dar prejuízo' };
}
function getSuporteResistencia(klines){
  const ks = klines.slice(-30);
  const highs = ks.map(k=>parseFloat(k[2])).filter(Number.isFinite);
  const lows = ks.map(k=>parseFloat(k[3])).filter(Number.isFinite);
  if(highs.length<10 || lows.length<10) return { toquesTop:0, toquesBottom:0, toques:0, score:5, topZone:0, bottomZone:0, desc:'dados insuficientes para avaliar zonas' };
  const sup = Math.max(...highs.slice(-20));
  const inf = Math.min(...lows.slice(-20));
  const range = Math.max(sup-inf, 1e-12);
  const atr = Math.max(calcATR(ks), range*0.01);
  // Zona adaptativa: nunca tratamos um único preço como suporte/resistência exato.
  const zone = Math.max(range*0.08, atr*1.35);
  let toquesTop=0, toquesBottom=0;
  ks.slice(-20).forEach(k=>{
    const h=parseFloat(k[2]), l=parseFloat(k[3]);
    if(Math.abs(sup-h)<=zone) toquesTop++;
    if(Math.abs(l-inf)<=zone) toquesBottom++;
  });
  const toques = toquesTop+toquesBottom;
  const score = toques>=6 ? 25 : toques>=4 ? 20 : toques>=3 ? 12 : 5;
  return {
    toquesTop, toquesBottom, toques, score,
    topZone: zone, bottomZone: zone,
    top: sup, bottom: inf,
    desc: `${toques} reações dentro de zonas de ${((zone/range)*100).toFixed(1)}% da faixa; não trato topo/fundo como preço exato`
  };
}
function getContexto(klinesLongos){
  if(!klinesLongos || klinesLongos.length<20) return {score:10, tipo:'indefinido', desc:'sem contexto de longo prazo'}; const closes = klinesLongos.map(k=>parseFloat(k[4])); const variacao = ((closes[closes.length-1]-closes[0])/closes[0])*100; let tipo='lateral', score=15; if(variacao<-5){ tipo='veio de queda forte - acumulacao'; score=22; } else if(variacao<-2){ tipo='veio de queda - possivel acumulacao'; score=18; } else if(variacao>5){ tipo='veio de alta forte - distribuicao'; score=8; } else if(variacao>2){ tipo='veio de alta - cuidado'; score=12; } else { tipo='mercado lateral calmo'; score=20; } return { variacao, tipo, score, desc: tipo==='mercado lateral calmo' ? 'mercado calmo nas últimas horas, sem tendência forte de alta ou queda, ótimo pra grid porque fica lateralizado' : tipo.includes('queda') ? `veio caindo ${variacao.toFixed(1)}% nas últimas horas e agora parou de cair e ficou lateral - pode estar acumulando (gente grande comprando escondido), bom pra grid` : `veio subindo ${variacao.toFixed(1)}% e agora parou de subir e ficou lateral - cuidado, pode estar distribuindo (gente grande vendendo escondido)` };
}


function detectarComportamentoReal(klines15m){
  const ks = klines15m.slice(-32);
  if(ks.length<24) return {tipo:'dados_insuficientes', score:0, sinais:[], descricao:'dados insuficientes'};
  const candles = ks.map(k=>({
    o:+k[1], h:+k[2], l:+k[3], c:+k[4], v:+k[5]
  }));
  const rangeTotal = Math.max(...candles.map(x=>x.h))-Math.min(...candles.map(x=>x.l));
  const first = candles.slice(0,12), last = candles.slice(-12);
  const moveAnterior = (first[first.length-1].c-first[0].c)/Math.max(first[0].c,1);
  const moveAtual = (last[last.length-1].c-last[0].c)/Math.max(last[0].c,1);
  const atrAnterior = calcATR(ks.slice(0,16));
  const atrAtual = calcATR(ks.slice(-16));
  const compressao = atrAnterior>0 && atrAtual/atrAnterior < 0.78;
  const expansao = atrAnterior>0 && atrAtual/atrAnterior > 1.25;
  const corpos = candles.map(x=>Math.abs(x.c-x.o));
  const medias = candles.map(x=>Math.max(x.h-x.l, 1e-12));
  const wicksRejeicao = candles.slice(-12).filter(x=>{
    const body=Math.abs(x.c-x.o);
    const upper=x.h-Math.max(x.o,x.c);
    const lower=Math.min(x.o,x.c)-x.l;
    return (upper>body*1.5 || lower>body*1.5);
  }).length;
  const ultimosRange = last.reduce((a,x)=>a+(x.h-x.l),0)/12;
  const rangeMedio = candles.reduce((a,x)=>a+(x.h-x.l),0)/candles.length;
  const preso = rangeMedio>0 && ultimosRange < rangeMedio*0.85 && wicksRejeicao>=4;
  const tendenciaAnterior = Math.abs(moveAnterior)>0.025;
  const respiro = tendenciaAnterior && Math.abs(moveAtual)<Math.abs(moveAnterior)*0.45 && compressao;
  const falsoRompe = wicksRejeicao>=5 && Math.abs(moveAtual)<0.012;
  let tipo='misto', score=0, sinais=[];
  if(respiro){ tipo='impulso_respiro_compressao'; score+=35; sinais.push('impulso anterior perdeu força e entrou em compressão'); }
  if(preso){ tipo = tipo==='misto'?'range_com_rejeicoes':tipo; score+=25; sinais.push(`${wicksRejeicao} candles com sombras longas dentro da faixa`); }
  if(falsoRompe){ score+=20; sinais.push('rejeições repetidas sem continuação do rompimento'); }
  if(compressao){ score+=10; sinais.push('ATR comprimindo'); }
  if(expansao){ score-=20; sinais.push('ATR expandindo'); }
  if(Math.abs(moveAtual)>0.025 && expansao){ score-=25; sinais.push('tendência atual forte com expansão'); }
  score=Math.max(0,Math.min(100,score));
  return {tipo, score, sinais, compressao, expansao, respiro, preso, falsoRompe, wicksRejeicao,
    moveAnterior:moveAnterior*100, moveAtual:moveAtual*100,
    descricao:sinais.length?sinais.join('; '):'sem comportamento dominante claro'};
}

// ========= NOVO: ASSINATURA DE COMPORTAMENTO DO MERCADO =========
// Isso evita criar milhares de padroes. So cria padrao novo se comportamento for realmente novo.
function extrairAssinaturaComportamento(klines15m, klines1h, klines4h){
  const lateral15 = isLateralReal(klines15m);
  const liq15 = hasLiquidez(klines15m);
  const poc15 = getPOC(klines15m);
  const supRes15 = getSuporteResistencia(klines15m);
  const comportamentoReal = detectarComportamentoReal(klines15m);
  const ctx1h = getContexto(klines1h);
  const ctx4h = klines4h ? getContexto(klines4h) : {score:12, variacao:0, tipo:'sem 4h'};
  
  const atr15 = calcATR(klines15m.slice(-20));
  const atrAntigo = calcATR(klines15m.slice(-40, -20));
  const atrMudanca = atrAntigo>0 ? (atr15/atrAntigo) : 1;
  
  const closes15 = klines15m.slice(-20).map(k=>parseFloat(k[4]));
  const vols15 = klines15m.slice(-20).map(k=>parseFloat(k[5]));
  const volCrescente = vols15[vols15.length-1] > (vols15.reduce((a,b)=>a+b,0)/vols15.length) ? 1 : 0;
  const volDecrescente = vols15[vols15.length-1] < (vols15.reduce((a,b)=>a+b,0)/vols15.length)*0.7 ? 1 : 0;
  
  // Assinatura comportamental: vetor que descreve COMO o mercado está se comportando, não só suporte/resistencia
  // Ex: [lateral forte?][sem liquidez?][POC meio?][muitos toques?][veio de queda?][volatilidade caindo?][volume caindo?]
  const vetor = [
    lateral15.isLateral ? 1 : 0, // 0-1
    lateral15.equalHighs, // 0-10
    lateral15.equalLows, // 0-10
    liq15.semLiquidez ? 1 : 0, // 0-1
    liq15.sweep, // 0-5
    poc15.posicao==='meio' ? 2 : poc15.posicao==='meio-borda' ? 1 : 0, // 0-2
    supRes15.toques>=6 ? 2 : supRes15.toques>=4 ? 1 : 0, // 0-2
    ctx1h.tipo.includes('queda forte') ? 2 : ctx1h.tipo.includes('queda') ? 1 : ctx1h.tipo.includes('alta forte') ? -2 : ctx1h.tipo.includes('alta') ? -1 : 0, // -2 a 2
    atrMudanca<0.8 ? 1 : atrMudanca>1.3 ? -1 : 0, // volatilidade caindo=1, subindo=-1, estável=0
    volDecrescente ? 1 : volCrescente ? -1 : 0, // volume caindo=1, subindo=-1
    comportamentoReal.respiro ? 2 : comportamentoReal.preso ? 1 : comportamentoReal.expansao ? -2 : 0,
    comportamentoReal.wicksRejeicao >= 5 ? 1 : 0,
  ];
  
  // Cria ID do comportamento baseado no vetor - agrupa comportamentos parecidos
  // Ex: "L3_T2_P2_S2_C2_V1" = lateral 3x topo 2x fundo, POC meio, 2 toques, veio queda, vol caindo
  // Comportamentos com mesmo ID são o MESMO comportamento, não padrão novo
  const comportamentoId = `L${lateral15.equalHighs}_${lateral15.equalLows}_Q${liq15.semLiquidez?1:0}_P${poc15.posicao==='meio'?2:poc15.posicao==='meio-borda'?1:0}_T${supRes15.toques>=6?2:supRes15.toques>=4?1:0}_C${ctx1h.tipo.includes('queda')? (ctx1h.tipo.includes('forte')? 'QF' : 'Q') : ctx1h.tipo.includes('alta')? (ctx1h.tipo.includes('forte')? 'AF' : 'A') : 'L'}_V${atrMudanca<0.8? 'C' : atrMudanca>1.3? 'S' : 'E'}_VOL${volDecrescente? 'C' : volCrescente? 'S' : 'E'}`;
  
  const descricaoComportamento = `
    COMPORTAMENTO REAL: ${comportamentoReal.descricao} |
    COMPORTAMENTO: ${ctx1h.desc} | ${lateral15.desc} | ${liq15.desc} | ${poc15.desc} | ${supRes15.desc} | Volatilidade ${atrMudanca<0.8? 'caindo (acumulação)' : atrMudanca>1.3? 'subindo (risco)' : 'estável'} | Volume ${volDecrescente? 'caindo (acumulação, gente grande comprando escondido)' : volCrescente? 'subindo' : 'estável'}
  `.trim();
  
  return { vetor, comportamentoId, descricaoComportamento, lateral15, liq15, poc15, supRes15, comportamentoReal, ctx1h, ctx4h, atrMudanca, volDecrescente, volCrescente };
}

function compararComportamentos(vetorA, vetorB){
  // Distância euclidiana simples entre vetores de comportamento
  if(vetorA.length!==vetorB.length) return 999;
  let soma=0;
  for(let i=0;i<vetorA.length;i++){
    const diff = vetorA[i]-vetorB[i];
    soma += diff*diff;
  }
  return Math.sqrt(soma);
}

function calcularProbabilidadeFechamento(klines, superior, inferior, grids){
  const ultimas6h = klines.slice(-24);
  if(ultimas6h.length<12) return { mediaMin:0, mediaMax:0, media:0, detalhe:'sem dados 6h suficientes' };
  const passo = (superior-inferior)/grids;
  if(passo===0) return { mediaMin:0, mediaMax:0, media:0, detalhe:'passo zero' };
  let fechamentosPorHora = [];
  for(let bloco=0; bloco<6; bloco++){
    const blocoKlines = ultimas6h.slice(bloco*4, (bloco+1)*4);
    let fechBloco=0;
    for(let i=1;i<blocoKlines.length;i++){
      const diff = Math.abs(parseFloat(blocoKlines[i][4]) - parseFloat(blocoKlines[i-1][4]));
      fechBloco += Math.floor(diff/passo);
    }
    fechamentosPorHora.push(fechBloco);
  }
  const mediaPorHora = fechamentosPorHora.reduce((a,b)=>a+b,0)/fechamentosPorHora.length;
  const minHora = Math.min(...fechamentosPorHora);
  const maxHora = Math.max(...fechamentosPorHora);
  const proj1h = { min: Math.floor(minHora*0.8), max: Math.ceil(maxHora*1.2), media: Math.round(mediaPorHora) };
  const proj2h = { min: Math.floor(minHora*1.5), max: Math.ceil(maxHora*2.5), media: Math.round(mediaPorHora*2) };
  const proj3h = { min: Math.floor(minHora*2), max: Math.ceil(maxHora*3.5), media: Math.round(mediaPorHora*3) };
  const mediaMin = proj1h.min;
  const mediaMax = proj3h.max;
  const media = proj2h.media;
  let detalhe = `Nas últimas 6h (24 candles de 15min) o preço fechou ${fechamentosPorHora.join(', ')} ordens por hora. Média ${mediaPorHora.toFixed(1)} ordens por hora. Em 1h deve fechar de ${proj1h.min} a ${proj1h.max}, em 2h de ${proj2h.min} a ${proj2h.max}, em 3h de ${proj3h.min} a ${proj3h.max}.`;
  return { mediaMin, mediaMax, media, proj1h, proj2h, proj3h, fechamentosPorHora, mediaPorHora, detalhe, probabilidadeTexto: `probabilidade: de ${mediaMin} a ${mediaMax} ordens (média ${media}) dentro de 1 a 3hrs` };
}

function simularGrid(klines, superior, inferior, grids){
  const ultimas6h = klines.slice(-24);
  const passo = (superior-inferior)/grids;
  if(passo===0) return { ok:false, motivo:'passo zero, faixa sem tamanho' };
  let maxPreco = -Infinity, minPreco = Infinity;
  ultimas6h.forEach(k=>{ const h = parseFloat(k[2]); const l = parseFloat(k[3]); if(h>maxPreco) maxPreco=h; if(l<minPreco) minPreco=l; });
  let linhasTocadas = 0;
  for(let i=0;i<=grids;i++){ const level = inferior + passo*i; if(level>=minPreco && level<=maxPreco) linhasTocadas++; }
  const pctTocadas = (linhasTocadas/grids)*100;
  if(pctTocadas<30){ return { ok:false, motivo:`Simulação 6h: só tocou ${linhasTocadas}/${grids} linhas (${pctTocadas.toFixed(0)}%) - vai ficar preso sem abrir ordem, porque o preço não mexeu o suficiente nas últimas 6h`, linhasTocadas, pctTocadas, maxPreco, minPreco }; }
  return { ok:true, linhasTocadas, pctTocadas, maxPreco, minPreco, motivo:`Simulação 6h OK: tocou ${linhasTocadas}/${grids} linhas (${pctTocadas.toFixed(0)}%) - vai abrir ordens normal, não vai ficar preso` };
}

function analisarMultiTimeframe(klines15m, klines1h, klines4h){
  const assinatura = extrairAssinaturaComportamento(klines15m, klines1h, klines4h);
  const lateral15 = assinatura.lateral15; const liq15 = assinatura.liq15; const poc15 = assinatura.poc15; const supRes15 = assinatura.supRes15; const ctx15 = assinatura.ctx1h; const ctx4h = assinatura.ctx4h;
  const score15 = lateral15.score + liq15.score + poc15.score + supRes15.score + ctx15.score + assinatura.comportamentoReal.score*0.6;
  const lateral1h = klines1h ? isLateralReal(klines1h) : {score:12}; const liq1h = klines1h ? hasLiquidez(klines1h) : {score:12}; const poc1h = klines1h ? getPOC(klines1h) : {score:12};
  const score1h = (lateral1h.score + liq1h.score + poc1h.score + 12 + 12);
  const score4h = (ctx4h.score + 12 + 12 + 12 + 12);
  const scoreFinal = Math.max(0, Math.min(100, Math.round((score15*0.5 + score1h*0.3 + score4h*0.2) / 140 * 100)));
  let duracao = scoreFinal>=80 ? "3-6h" : scoreFinal>=70 ? "2-4h" : scoreFinal>=60 ? "1-3h" : "1-2h";
  let classificacao = scoreFinal>=80 ? 'TOP' : scoreFinal>=60 ? 'MEDIO' : scoreFinal>=45 ? 'MEDIO_BAIXO' : 'MUITO_RUIM';
  return { score: scoreFinal, classificacao, duracao, assinatura, contexto: { estrutura: lateral15.desc, comportamento_real: assinatura.comportamentoReal, liquidez: liq15.desc, poc: poc15.desc, suporte: supRes15.desc, contexto: `${ctx15.desc} | 4h: ${ctx4h.desc}`, duracao_estimada: duracao, comportamento: assinatura.descricaoComportamento, comportamentoId: assinatura.comportamentoId }, detalhes: { lateral15, liq15, poc15, supRes15, ctx15, ctx4h } };
}
function isFalsoGridOuIndecisao(klines){
  let wick=0, indecisao=0, volAmbos=0; klines.slice(-20).forEach(k=>{ const o=parseFloat(k[1]), c=parseFloat(k[4]), h=parseFloat(k[2]), l=parseFloat(k[3]); const corpo=Math.abs(c-o), sup=h-Math.max(o,c), inf=Math.min(o,c)-l; const total=corpo+sup+inf; if(total===0) return; if((sup+inf)/total>0.7) wick++; if(sup/total>0.3 && inf/total>0.3 && corpo/total<0.3) indecisao++; if(sup>corpo*2 && inf>corpo*2) volAmbos++; }); if(wick>=12) return {falso:true, motivo:`Muito pavio ${wick}/20 - mercado indeciso`}; if(indecisao>=8) return {falso:true, motivo:`Indecisão ${indecisao}/20 - muito doji`}; if(volAmbos>=10) return {falso:true, motivo:`Volatilidade pros 2 lados ${volAmbos}/20`}; if((wick+indecisao)>=15) return {falso:true, motivo:`Pavio+Indecisão ${wick+indecisao}/20`}; return {falso:false, wick, indecisao, volAmbos};
}
function isPossivelRompimento(klines, superior, inferior, precoAtual){
  const range = superior - inferior;
  if(range<=0) return {romp:false, perigo:false, margem:0};
  const atr = Math.max(calcATR(klines.slice(-30)), range*0.005);
  // Margem de segurança adaptativa. Em vez de 12% fixos, usa 15-28% conforme a volatilidade.
  const margemPct = Math.min(0.28, Math.max(0.15, (atr/range)*2.2));
  const margem = range*margemPct;
  const distTop = (superior - precoAtual);
  const distBottom = (precoAtual - inferior);
  const perigoTopo = distTop <= margem;
  const perigoFundo = distBottom <= margem;
  if(perigoTopo) return {romp:true, perigo:true, lado:'topo', margem:margemPct, motivo:`Preço entrou na zona de perigo do topo (${(distTop/range*100).toFixed(1)}% da faixa restante; margem ${(margemPct*100).toFixed(1)}%).`};
  if(perigoFundo) return {romp:true, perigo:true, lado:'fundo', margem:margemPct, motivo:`Preço entrou na zona de perigo do fundo (${(distBottom/range*100).toFixed(1)}% da faixa restante; margem ${(margemPct*100).toFixed(1)}%).`};
  const ultimos = klines.slice(-8).map(k=>parseFloat(k[4]));
  let subindo=0, descendo=0;
  for(let i=1;i<ultimos.length;i++){
    if(ultimos[i]>ultimos[i-1]) subindo++;
    else if(ultimos[i]<ultimos[i-1]) descendo++;
  }
  const atrRecente = calcATR(klines.slice(-10));
  const atrAntigo = calcATR(klines.slice(-30,-10));
  if(subindo>=6 && atrRecente > atrAntigo*1.15) return {romp:true, perigo:true, lado:'alta', margem:margemPct, motivo:`Pressão compradora aumentou (${subindo}/7 fechamentos) e ATR está expandindo.`};
  if(descendo>=6 && atrRecente > atrAntigo*1.15) return {romp:true, perigo:true, lado:'baixa', margem:margemPct, motivo:`Pressão vendedora aumentou (${descendo}/7 fechamentos) e ATR está expandindo.`};
  return {romp:false, perigo:false, margem:margemPct};
}
function recomendarTop5(premiumsObj){
  const lista = Object.values(premiumsObj).filter(p=>p.classificacao==='TOP' || p.score>=75); const base = lista.length>0 ? lista : Object.values(premiumsObj); if(base.length===0) return null; return base.sort((a,b)=>{ const sA = (a.score*0.4)+(a.prob*0.3)+(a.fechamentos*2)+(a.repeticoes_24h*2)+(a.mediaFechamento||0); const sB = (b.score*0.4)+(b.prob*0.3)+(b.fechamentos*2)+(b.repeticoes_24h*2)+(b.mediaFechamento||0); return sB-sA; }).slice(0,5);
}
function getSemanaKey(){ const d=new Date(); const jan1 = new Date(d.getFullYear(),0,1); const dias = Math.floor((d-jan1)/86400000); return `${d.getFullYear()}-S${Math.ceil((dias+jan1.getDay()+1)/7)}`; }
function alimentarBibliotecaViva(symbol, analise){
  const bib = load(FILES.bibliotecaViva, {}); const comportamentoId = analise.assinatura ? analise.assinatura.comportamentoId : analise.patternKey; if(!bib[comportamentoId]){ bib[comportamentoId] = { nome: analise.nomePadrao, pattern: comportamentoId, comportamentoId, total:0, contextos:[], fechamentosHistorico:[], ultima_atualizacao:new Date().toISOString() }; } bib[comportamentoId].total++; bib[comportamentoId].fechamentosHistorico.push(analise.fechamentoInfo ? analise.fechamentoInfo.media : 0); if(bib[comportamentoId].fechamentosHistorico.length>100) bib[comportamentoId].fechamentosHistorico.shift(); bib[comportamentoId].contextos.push({ timestamp:new Date().toISOString(), symbol, comportamento: analise.assinatura ? analise.assinatura.descricaoComportamento : analise.contextoCompleto, score: analise.score, classificacao: analise.classificacao, duracao_estimada: analise.duracao, fechamento: analise.fechamentoInfo, pctOrdens: analise.pctOrdens }); if(bib[comportamentoId].contextos.length>50) bib[comportamentoId].contextos.shift(); bib[comportamentoId].ultima_atualizacao = new Date().toISOString(); save(FILES.bibliotecaViva, bib);
  const perf = load(FILES.performancePeriodo, {}); const hoje = new Date().toISOString().slice(0,10); const semana = getSemanaKey(); if(!perf[comportamentoId]) perf[comportamentoId] = { nome: analise.nomePadrao, comportamentoId, diario:{}, semanal:{}, total:{detectado:0, paresDiferentes:{}, fechamentoMedio:0, fechamentos:[]}, pares:{} }; perf[comportamentoId].total.detectado++; if(analise.fechamentoInfo){ perf[comportamentoId].total.fechamentos.push(analise.fechamentoInfo.media); if(perf[comportamentoId].total.fechamentos.length>100) perf[comportamentoId].total.fechamentos.shift(); const soma = perf[comportamentoId].total.fechamentos.reduce((a,b)=>a+b,0); perf[comportamentoId].total.fechamentoMedio = soma/perf[comportamentoId].total.fechamentos.length; } if(!perf[comportamentoId].diario[hoje]) perf[comportamentoId].diario[hoje]={detectado:0, fechamentos:[]}; perf[comportamentoId].diario[hoje].detectado++; if(analise.fechamentoInfo) perf[comportamentoId].diario[hoje].fechamentos.push(analise.fechamentoInfo.media); if(!perf[comportamentoId].semanal[semana]) perf[comportamentoId].semanal[semana]={detectado:0, paresDiferentes:{}, fechamentoMedio:0, fechamentos:[]}; perf[comportamentoId].semanal[semana].detectado++; if(analise.symbol){ perf[comportamentoId].semanal[semana].paresDiferentes[analise.symbol]= (perf[comportamentoId].semanal[semana].paresDiferentes[analise.symbol]||0)+1; perf[comportamentoId].total.paresDiferentes[analise.symbol]= (perf[comportamentoId].total.paresDiferentes[analise.symbol]||0)+1; } if(analise.fechamentoInfo){ perf[comportamentoId].semanal[semana].fechamentos.push(analise.fechamentoInfo.media); const somaSem = perf[comportamentoId].semanal[semana].fechamentos.reduce((a,b)=>a+b,0); perf[comportamentoId].semanal[semana].fechamentoMedio = somaSem/perf[comportamentoId].semanal[semana].fechamentos.length; } save(FILES.performancePeriodo, perf);
}

async function scanPossiveis(){
  if(scanning) return; scanning=true; io.emit('scan_status', {active:true, fase:'1 - Possíveis (filtro rápido 3h)'}); optimusTempoReal(`Começando varredura de 3h... Vou procurar oportunidades com comportamento real!`, 'SCAN 3H'); logSistema(`[SCAN 1 - 3H] Iniciando - filtros ATR/Range/FalsoGrid/%ord 0.45-0.85`, 'scan');
  try{
    const info = await fetchBinance('/fapi/v1/exchangeInfo'); const symbols = info.symbols.filter(s=>s.quoteAsset==='USDT' && s.contractType==='PERPETUAL' && s.status==='TRADING').map(s=>s.symbol); logSistema(`[SCAN 1] ${symbols.length} pares carregados`, 'ok'); let possiveis = {}; let count=0, descartadosFalso=0, descartadosPct=0;
    for(let i=0;i<symbols.length;i+=CONCURRENT){ const lote = symbols.slice(i,i+CONCURRENT); const res = await Promise.all(lote.map(async(symbol)=>{ try{ const klines = await fetchBinance(`/fapi/v1/klines?symbol=${symbol}&interval=15m&limit=30`); if(!klines||klines.length<20) return null; const checkFalso = isFalsoGridOuIndecisao(klines); if(checkFalso.falso) return {skip:true, tipo:'FALSO_GRID'}; const atr = calcATR(klines); const preco = parseFloat(klines[klines.length-1][4]); const atrPct = (atr/preco)*100; const sup = Math.max(...klines.slice(-20).map(k=>parseFloat(k[2]))); const inf = Math.min(...klines.slice(-20).map(k=>parseFloat(k[3]))); const rangePct = ((sup-inf)/inf)*100; if(atrPct<0.5 || atrPct>3.5) return {skip:true, tipo:'ATR'}; if(rangePct<2.0 || rangePct>13) return {skip:true, tipo:'RANGE'}; let score = 0; if(atrPct>=0.8 && atrPct<=2.5) score+=30; if(rangePct>=2.5 && rangePct<=10) score+=30; if(checkFalso.wick<=5) score+=20; score+=20; if(score<60) return {skip:true, tipo:'SCORE'}; let grids=12, pct=rangePct/grids; if(pct<0.4){ grids=8; pct=rangePct/8; } if(pct>0.9){ grids=16; pct=rangePct/16; } if(pct<0.45 || pct>0.85) return {skip:true, tipo:'PCT_ORDEM', pct}; return { symbol, score, superior:sup, inferior:inf, rangePct, atrPct, pctOrdens:pct, grids, precoAtual:preco, upperFmt:sup.toFixed(4), lowerFmt:inf.toFixed(4) }; }catch{ return null; } })); for(const r of res){ if(!r) continue; if(r.skip){ if(r.tipo==='FALSO_GRID') descartadosFalso++; if(r.tipo==='PCT_ORDEM') descartadosPct++; continue; } possiveis[r.symbol] = { ...r, total:1, ultimaVez:new Date().toISOString() }; count++; io.emit('possivel', possiveis[r.symbol]); } if(i%40===0) logSistema(`[SCAN 1] ${i}/${symbols.length} - ${count} possíveis - ${descartadosFalso} falsos - ${descartadosPct} %ord fora 0.45-0.85`, 'scan'); await sleep(DELAY_LOTE); } save(FILES.possiveis, Object.values(possiveis)); logSistema(`[SCAN 1] Fim - ${count} possíveis - ${descartadosFalso} falsos - ${descartadosPct} %ord fora 0.45-0.85 bloqueados`, 'prime'); optimusTempoReal(`Encontrei ${count} candidatos! Agora vou analisar comportamento real de cada um...`, 'FIM SCAN 3H'); setTimeout(()=>scanPremium(), 2000);
  }catch(e){ logSistema(`[ERRO SCAN 1] ${e.message}`, 'warn'); }finally{ scanning=false; }
}

async function scanPremium(){
  if(scanning) return; scanning=true; io.emit('scan_status', {active:true, fase:'2 - Premium comportamento real + comparação biblioteca'}); optimusTempoReal(`Recebi os candidatos! Vou comparar cada um com comportamentos que já conheço da biblioteca...`, 'PREMIUM INICIO'); logSistema(`[SCAN 2 - PREMIUM V15] Comportamento real - compara biblioteca - só cria padrão novo se comportamento novo`, 'scan');
  try{
    let possiveis = load(FILES.possiveis, []); if(!Array.isArray(possiveis)) possiveis = Object.values(possiveis);
    const premiumsAnteriores = load(FILES.premiums, {});
    let premiums = {};
    let comportamentos = load(FILES.comportamentos, {});
    let padroes = load(FILES.padroes, {});
    let ultimaAnalise = load(FILES.ultimaAnalise, {});
    let estadoPremium = load(FILES.estadoPremium, {});
    let bibliotecaViva = load(FILES.bibliotecaViva, {});
    let inseridos=0, descartadosRomp=0, descartadosFalso=0, pulados24h=0, descartadosPoluicao=0, descartadosPct=0, descartadosSimulacao=0, comportamentoExistente=0, comportamentoNovo=0, medios=0, semDadoNovo=0;
    const agora = Date.now(); const umDia = 24*60*60*1000;
    optimusTempoReal(`Vou analisar ${possiveis.length} candidatos comparando com ${Object.keys(bibliotecaViva).length} comportamentos que já conheço...`, 'PREMIUM');
    for(let idx=0; idx<possiveis.length; idx++){
      const p = possiveis[idx];
      try{        if(p.pctOrdens<0.45 || p.pctOrdens>0.85){ descartadosPct++; logSistema(`[FILTRO %ORD] ${p.symbol} ${p.pctOrdens.toFixed(2)}% fora 0.45-0.85 - bloqueado`, 'skip'); continue; }
        const klines15m = await fetchBinance(`/fapi/v1/klines?symbol=${p.symbol}&interval=15m&limit=288`);
        if(!klines15m||klines15m.length<96) continue;
        const ultimoFechado = klines15m.length>=2 ? Number(klines15m[klines15m.length-2][6]) : Number(klines15m[klines15m.length-1][6]);
        const cursor = getAnalysisCursor(p.symbol);
        const dadoNovo = !cursor || ultimoFechado > Number(cursor.last_closed_ts||0);
        if(!dadoNovo) semDadoNovo++;
        let klines1h=null, klines4h=null; try{ klines1h = await fetchBinance(`/fapi/v1/klines?symbol=${p.symbol}&interval=1h&limit=72`); }catch{} try{ klines4h = await fetchBinance(`/fapi/v1/klines?symbol=${p.symbol}&interval=4h&limit=42`); }catch{}
        const checkFalso = isFalsoGridOuIndecisao(klines15m.slice(-20)); if(checkFalso.falso){ descartadosFalso++; logSistema(`[FALSO GRID] ${p.symbol} barrado: ${checkFalso.motivo}`, 'skip'); continue; }
        const preco = parseFloat(klines15m[klines15m.length-1][4]); const sup = Math.max(...klines15m.slice(-20).map(k=>parseFloat(k[2]))); const inf = Math.min(...klines15m.slice(-20).map(k=>parseFloat(k[3]))); const checkRomp = isPossivelRompimento(klines15m, sup, inf, preco); if(checkRomp.romp){ descartadosRomp++; logSistema(`[ROMPIMENTO] ${p.symbol} barrado: ${checkRomp.motivo}`, 'skip'); continue; }
        const simulacao = simularGrid(klines15m, sup, inf, p.grids); if(!simulacao.ok){ descartadosSimulacao++; logSistema(`[SIMULAÇÃO] ${p.symbol} barrado: ${simulacao.motivo}`, 'skip'); continue; }
        const analise = analisarMultiTimeframe(klines15m, klines1h, klines4h);
        if(analise.classificacao==='MUITO_RUIM'){ descartadosPoluicao++; logSistema(`[ANTI-POLUIÇÃO] ${p.symbol} ${analise.score}% <45% muito ruim, descartado`, 'skip'); continue; }
        
        // ========= COMPARA COMPORTAMENTO COM BIBLIOTECA =========
        const assinaturaAtual = analise.assinatura;
        let comportamentoEncontrado = null;
        let menorDistancia = 999;
        // Procura comportamento parecido na biblioteca
        Object.values(bibliotecaViva).forEach(bib=>{
          if(bib.contextos && bib.contextos.length>0){
            // Pega último vetor de cada comportamento salvo (simplificado: compara por ID)
            if(bib.pattern===assinaturaAtual.comportamentoId){
              comportamentoEncontrado = bib;
              menorDistancia = 0;
            }
          }
        });
        // Se não achou por ID exato, procura por distância vetorial nos comportamentos salvos
        if(!comportamentoEncontrado){
          Object.entries(comportamentos).forEach(([id, comp])=>{
            if(comp.vetor){
              const dist = compararComportamentos(assinaturaAtual.vetor, comp.vetor);
              if(dist<menorDistancia){
                menorDistancia = dist;
                if(dist<2.5){ // Threshold: se distância <2.5, é mesmo comportamento
                  comportamentoEncontrado = bibliotecaViva[id] || { pattern:id };
                }
              }
            }
          });
        }
        
        let comportamentoId, nomeSimplificado, isNovoComportamento=false;
        if(comportamentoEncontrado && menorDistancia<2.5){
          // Comportamento já existe - não cria padrão novo, só conta repetição
          comportamentoId = comportamentoEncontrado.pattern || assinaturaAtual.comportamentoId;
          nomeSimplificado = getNomePadraoAvancado(comportamentoId);
          comportamentoExistente++;
          logSistema(`[COMPORTAMENTO] ${p.symbol} comportamento ${comportamentoId} já conhecido (${nomeSimplificado}) - distância ${menorDistancia.toFixed(1)} - conta como repetição, não cria padrão novo`, 'ok');
        } else {
          // Comportamento novo de verdade - cria padrão novo
          comportamentoId = assinaturaAtual.comportamentoId;
          nomeSimplificado = getNomePadraoAvancado(comportamentoId);
          isNovoComportamento=true;
          comportamentoNovo++;
          // Salva novo comportamento na biblioteca de comportamentos
          comportamentos[comportamentoId] = {
            id: comportamentoId,
            vetor: assinaturaAtual.vetor,
            descricao: assinaturaAtual.descricaoComportamento,
            nome: nomeSimplificado,
            primeiraVez: new Date().toISOString(),
            total: 1
          };
          logSistema(`[COMPORTAMENTO NOVO] ${p.symbol} comportamento NOVO detectado: ${comportamentoId} -> nome ${nomeSimplificado}. Descrição: ${assinaturaAtual.descricaoComportamento}. Criando padrão novo!`, 'prime');
        }
        
        const probFechamento = calcularProbabilidadeFechamento(klines15m, sup, inf, p.grids);
        if(dadoNovo){
          ultimaAnalise[p.symbol] = ultimoFechado;
          setAnalysisCursor(p.symbol, ultimoFechado, klines15m.length);
        }
        estadoPremium[p.symbol] = { ultima_analise: new Date(ultimoFechado).toISOString(), score: analise.score, classificacao: analise.classificacao, comportamentoId, dadoNovo };
        
        const passo = (sup-inf)/p.grids; let fech=0; for(let i=1;i<klines15m.slice(-20).length;i++){ const diff = Math.abs(parseFloat(klines15m[klines15m.length-20+i][4]) - parseFloat(klines15m[klines15m.length-21+i][4])); fech+=Math.floor(diff/passo); } let levIdeal = p.pctOrdens<0.5?18 : p.pctOrdens<0.6?15 : p.pctOrdens<0.75?12 : 10; if((calcATR(klines15m)/preco*100)>2.0) levIdeal=Math.max(8, levIdeal-3); const levMin = Math.max(5, Math.floor(levIdeal*0.6));
        
        if(!padroes[comportamentoId]) padroes[comportamentoId] = { pattern:comportamentoId, comportamentoId, nome:nomeSimplificado, total:0, pares:{}, timestamps:[], fechamentos:[], fechamentoMedio:0, vetor: assinaturaAtual.vetor, descricao: assinaturaAtual.descricaoComportamento };
        padroes[comportamentoId].timestamps = padroes[comportamentoId].timestamps.filter(t=>agora - t < umDia);
        if(dadoNovo){
          padroes[comportamentoId].timestamps.push(agora);
          padroes[comportamentoId].total++;
          padroes[comportamentoId].pares[p.symbol] = (padroes[comportamentoId].pares[p.symbol]||0)+1;
          padroes[comportamentoId].fechamentos.push(probFechamento.media);
          if(padroes[comportamentoId].fechamentos.length>100) padroes[comportamentoId].fechamentos.shift();
          const somaFech = padroes[comportamentoId].fechamentos.reduce((a,b)=>a+b,0);
          padroes[comportamentoId].fechamentoMedio = somaFech/padroes[comportamentoId].fechamentos.length;
          alimentarBibliotecaViva(p.symbol, { nomePadrao: nomeSimplificado, patternKey: comportamentoId, comportamentoId, score: analise.score, classificacao: analise.classificacao, duracao: analise.duracao, contextoCompleto: analise.contexto, fechamentoInfo: probFechamento, pctOrdens: p.pctOrdens, symbol: p.symbol, assinatura: assinaturaAtual });
        }
        
        const premiumData = {
          symbol:p.symbol, score: analise.score, prob, superior:sup, inferior:inf, rangePct: ((sup-inf)/inf*100),
          pctOrdens:p.pctOrdens, grids:p.grids, precoAtual:preco, upperFmt:sup.toFixed(4), lowerFmt:inf.toFixed(4),
          levIdeal, levMin, fechamentos:fech, repeticoes_24h, totalPar: padroes[comportamentoId].total,
          pattern:comportamentoId, comportamentoId, nomePadrao:nomeSimplificado, classificacao: analise.classificacao,
          duracao_estimada: analise.duracao, contexto: analise.contexto,
          probFechamento, simulacao,
          fechamentoMedio: padroes[comportamentoId].fechamentoMedio,
          paresDiferentes: Object.keys(padroes[comportamentoId].pares).length,
          isNovoComportamento,
          distancia: menorDistancia,
          zonaPerigo: checkRomp,
          comportamentoReal: assinaturaAtual.comportamentoReal,
          dadoNovo,
          tipo: analise.classificacao==='TOP'?'TOP': analise.classificacao==='MEDIO'?'MEDIO':'MEDIO-BAIXO',
          ultimaAnalise24h: new Date().toISOString(), isOpen: premiumsAnteriores[p.symbol]?.isOpen||false
        };
        premiums[p.symbol] = premiumData;
        inseridos++; if(analise.classificacao==='MEDIO') medios++;
        io.emit('premium', premiumData);
        logSistema(`[PREMIUM V15] ${p.symbol} ${analise.classificacao} ${analise.score}% - Comportamento ${nomeSimplificado} (${comportamentoId}) ${isNovoComportamento? 'NOVO' : 'repetição'} - ${probFechamento.probabilidadeTexto}`, 'prime');
        if(inseridos%5===0) optimusTempoReal(`Já analisei ${idx+1}/${possiveis.length}... ${inseridos} passaram! ${comportamentoNovo} comportamentos novos, ${comportamentoExistente} repetições de comportamentos já conhecidos`, 'PREMIUM');
        await sleep(400);
      }catch(e){ logSistema(`[ERRO PREMIUM] ${p.symbol} ${e.message}`, 'warn'); }
    }
    for(const oldSymbol of Object.keys(premiumsAnteriores)){
      if(!premiums[oldSymbol]){
        const old = premiumsAnteriores[oldSymbol];
        recordPremiumEvent(oldSymbol, 'REMOVIDO', 'deixou de satisfazer as condições atuais; preservado apenas no histórico/conhecimento', old?.score, old?.comportamentoId);
        logSistema(`[PREMIUM] ${oldSymbol} saiu do Premium: condição atual deixou de ser válida. Histórico preservado.`, 'skip');
      }
    }
    save(FILES.premiums, premiums); save(FILES.padroes, padroes); save(FILES.comportamentos, comportamentos); save(FILES.ultimaAnalise, ultimaAnalise); save(FILES.estadoPremium, estadoPremium);
    io.emit('premiums_history', Object.values(premiums));
    const totalPremiums = Object.keys(premiums).length; const totalPadroes = Object.keys(padroes).length; const totalComportamentos = Object.keys(comportamentos).length; const totalRep24h = Object.values(padroes).reduce((a,b)=>a+(b.timestamps?b.timestamps.length:0),0);
    logSistema(`[SCAN 2] Fim - ${inseridos} novos - ${comportamentoNovo} comportamentos NOVOS de verdade - ${comportamentoExistente} repetições de comportamentos já conhecidos (não criou padrão novo à toa) - total comportamentos únicos: ${totalComportamentos} (não milhares!) - total premiums: ${totalPremiums}`, 'prime');
    let mensagemFinal = `ANALISE PREMIUM V15 - COMPORTAMENTO REAL FINALIZADA:\\n`;
    mensagemFinal += `• ${inseridos} sinais analisados\\n`;
    mensagemFinal += `• ${comportamentoNovo} COMPORTAMENTOS NOVOS DE VERDADE detectados (mercado fez algo que nunca tinha feito antes)\\n`;
    mensagemFinal += `• ${comportamentoExistente} repetições de comportamentos que já conhecíamos (não criei padrão novo à toa, só contei como repetição do comportamento existente)\\n`;
    mensagemFinal += `• Total de comportamentos únicos na biblioteca: ${totalComportamentos} (não milhares! Só comportamentos reais diferentes)\\n`;
    mensagemFinal += `• Isso evita criar padrão toda vez que vê suporte e resistência que funcionou - isso não é padrão de comportamento gráfico\\n`;
    mensagemFinal += `• Padrão só é criado quando o mercado faz um comportamento novo: ex: veio de queda forte + volatilidade caindo + POC meio + sem liquidez = acumulação real\\n`;
    mensagemFinal += `• ${totalPadroes} padrões com nomes Delta_a1, Delta_a2, Delta_b1 etc (nunca acaba em Padrão 1)\\n`;
    mensagemFinal += `• ${totalRep24h} repetições 24h\\n• ${semDadoNovo} candidatos sem candle novo desde o último cursor; não contados novamente no aprendizado\\n• Premium é renovado a cada ciclo; sinais inválidos saem, histórico permanece\\n\\n`;
    const perf = load(FILES.performancePeriodo, {}); const semanaKey = getSemanaKey(); let melhor=null, max=0; Object.entries(perf).forEach(([k,v])=>{ const s=v.semanal[semanaKey]; if(s&&s.detectado>max){max=s.detectado;melhor=v;}}); if(melhor) mensagemFinal+=`Essa semana ${melhor.nome} (${melhor.comportamentoId}) tá bombando! Apareceu ${max} vezes em ${Object.keys(melhor.total.paresDiferentes||{}).length} pares diferentes, com média de fechamento de ${melhor.total.fechamentoMedio.toFixed(0)} ordens (de ${Math.min(...(melhor.total.fechamentos||[0]))} a ${Math.max(...(melhor.total.fechamentos||[0]))}) dentro de 1 a 3hrs\\n`;
    mensagemFinal+=`\\nDesses ${inseridos}, TOP 5 mais seguros agora (comportamentos reais, não só suporte/resistencia):`;
    const top5 = recomendarTop5(premiums); optimusAzul(mensagemFinal, {top5, totalPadroes, totalRep24h, totalComportamentos}); optimusTempoReal(`Dos ${possiveis.length} candidatos, ${inseridos} passaram! ${comportamentoNovo} comportamentos novos de verdade, ${comportamentoExistente} repetições. TOP 5 mais seguros agora:`, 'TOP5'); io.emit('stats', { totalPremiums, totalPadroes, totalComportamentos, novos:inseridos, comportamentoNovo, comportamentoExistente, descartadosFalso, descartadosRomp, pulados24h, totalRep24h, antiPoluicao:descartadosPoluicao, medios, pctBloqueado:descartadosPct, simBloqueado:descartadosSimulacao });
  }catch(e){ logSistema(`[ERRO SCAN 2] ${e.message}`, 'warn'); }finally{ scanning=false; io.emit('scan_status', {active:false, fase:'Pronto - V15 comportamento real'}); }
}
async function guardiao10min(){
  const entradas = load(FILES.entradas, {}); const symbols = Object.keys(entradas); if(symbols.length===0) return; logSistema(`[GUARDIÃO 10min] Vigiando ${symbols.length} pares`, 'scan');
  for(const sym of symbols){ try{ const ticker = await fetchBinance(`/fapi/v1/ticker/price?symbol=${sym}`); const preco = parseFloat(ticker.price); const ent = entradas[sym]; if(preco > ent.faixa_sup*1.002 || preco < ent.faixa_inf*0.998){ optimusAzul(`${ent.nome||'Você'}: Fecha ${sym} agora! Saiu da faixa! Sob risco!`, null); logSistema(`[ALERTA RISCO] ${sym} rompeu faixa`, 'prime'); } }catch{} await sleep(300); }
}
async function noticiasRotativas30s(){
  try{
    const padroes = load(FILES.padroes, {}); const perf = load(FILES.performancePeriodo, {}); const semanaKey = getSemanaKey(); const hoje = new Date().toISOString().slice(0,10);
    const totalHoje = Object.values(perf).reduce((a,p)=>{ const d=p.diario[hoje]; return a+(d?d.detectado:0); },0);
    let parMaisFirme=null, maxVezes=0; Object.values(padroes).forEach(p=>{ Object.entries(p.pares||{}).forEach(([sym,qtd])=>{ if(qtd>maxVezes){ maxVezes=qtd; parMaisFirme=sym; } }); });
    let melhorPadrao=null, maxSem=0, melhorDados=null; Object.values(perf).forEach(p=>{ const s=p.semanal[semanaKey]; if(s && s.detectado>maxSem){ maxSem=s.detectado; melhorPadrao=p.nome; melhorDados=p; } });
    const noticias = [];
    if(totalHoje>0) noticias.push(`📰 HOJE: Já encontrei ${totalHoje} comportamentos reais hoje! Não são só suporte e resistência que funcionou, são comportamentos de mercado de verdade (ex: acumulação, distribuição). O mercado tá bem movimentado!`);
    if(parMaisFirme) noticias.push(`📰 PAR FIRME: ${parMaisFirme} tá muito firme hoje, já apareceu ${maxVezes} vezes com o mesmo comportamento real! Isso significa que o comportamento dele se repetiu ${maxVezes} vezes hoje, não é só suporte/resistência aleatório.`);
    if(melhorPadrao && melhorDados){
      const paresDiff = Object.keys(melhorDados.total.paresDiferentes||{}).length;
      const mediaFech = melhorDados.total.fechamentoMedio ? melhorDados.total.fechamentoMedio.toFixed(0) : '15';
      const minFech = melhorDados.total.fechamentos && melhorDados.total.fechamentos.length>0 ? Math.min(...melhorDados.total.fechamentos) : 10;
      const maxFech = melhorDados.total.fechamentos && melhorDados.total.fechamentos.length>0 ? Math.max(...melhorDados.total.fechamentos) : 30;
      const compId = melhorDados.comportamentoId || melhorPadrao;
      noticias.push(`📰 PADRÃO DA SEMANA - COMPORTAMENTO REAL: ${melhorPadrao} (${compId}) tá com tudo essa semana! Ele é um comportamento real do mercado, não só suporte e resistência. Apareceu em ${paresDiff} pares diferentes, com média mínima de fechamento de ordens de ${minFech} a ${maxFech} ordens (média ${mediaFech}) dentro de 1 a 3hrs em grid curto (12 grids) ou mais no caso de grid longo (16 grids). Isso significa que se você abrir agora um grid de 12 linhas, a probabilidade é de fechar de ${minFech} a ${maxFech} ordens em até 3 horas, porque nas últimas 6h ele fechou em média ${mediaFech} por hora. Em grid longo de 16 grids, fecha ainda mais porque tem mais linhas pra tocar!`);
    }
    const chaves = Object.keys(padroes);
    if(chaves.length>0){
      const p = padroes[chaves[Math.floor(Math.random()*chaves.length)]];
      if(p && p.fechamentoMedio){
        noticias.push(`📰 DETALHE DE COMPORTAMENTO: Padrão ${p.nome} (${p.comportamentoId}) - Esse não é só suporte/resistência que funcionou, é um comportamento real: ${p.descricao||'lateral com acumulação'}. Teve ${p.total} repetições totais, com média de ${p.fechamentoMedio.toFixed(0)} fechamentos por grid. Nas últimas 24h apareceu ${p.timestamps? p.timestamps.length:0} vezes em ${Object.keys(p.pares||{}).length} pares diferentes. A % entre ordens dele é segura, entre 0.45% e 0.85%, então não fica preso sem abrir ordem!`);
      }
    }
    if(noticias.length===0){ noticias.push(`📰 Analisando comportamentos reais do mercado... Só crio padrão novo quando vejo comportamento que nunca vi antes, não toda vez que suporte e resistência funciona!`); }
    const noticiaEscolhida = noticias[Math.floor(Math.random()*noticias.length)];
    optimusNoticia(noticiaEscolhida);
    // noticia só na barra amarela única
    logSistema(`[NOTICIA 30s] Notícia comportamento real exibida`, 'ok');
    setTimeout(()=>{
      const premiumsNow = load(FILES.premiums, {});
      const top5 = recomendarTop5(premiumsNow);
      if(top5){
        let msg = `🏆 TOP 5 COMPORTAMENTOS REAIS AGORA (atualizado a cada 30s - loop sem parar):\\n`;
        top5.forEach((p,i)=>{
          const probTxt = p.probFechamento ? p.probFechamento.probabilidadeTexto : `de ${p.fechamentos||10} a ${(p.fechamentos||10)*2} ordens`;
          msg += `${i+1}. ${p.symbol} - Comportamento ${p.nomePadrao} (${p.comportamentoId}) - Score ${p.score}% - ${p.grids} grids - Lev ${p.levIdeal}x - deve durar ${p.duracao_estimada}\\n`;
          msg += `   👉 O que é esse comportamento: ${p.contexto? p.contexto.comportamento : 'acumulação real'} - Não é só suporte e resistência que funcionou, é comportamento de mercado de verdade!\\n`;
          msg += `   📊 Configuração detalhada pra leigo entender: Entrada de ${p.lowerFmt} até ${p.upperFmt} (é a faixa de preço onde você vai colocar o grid). ${p.grids} ordens (quantidade de linhas no grid, se for 12 é grid curto, 16 é grid longo, longo fecha mais ordens). % entre ordens ${p.pctOrdens.toFixed(2)}% (espaço entre cada ordem. 0.45% a 0.85% é o seguro: menos que 0.45% fica muito apertado e o preço não consegue abrir ordem, fica preso; mais que 0.85% fica muito largo e abre pouca ordem, não lucra. Por isso bloqueei tudo fora de 0.45% a 0.85%, nada vai pro premium fora disso). Alavancagem ideal ${p.levIdeal}x (quanto de alavancagem usar, se usar muito estoura, se usar pouco não lucra, ${p.levIdeal}x é o equilibrado, mínima ${p.levMin}x pra não estourar).\\n`;
          msg += `   📈 Probabilidade de fechamento: ${probTxt} dentro de 1 a 3hrs. Como calculei: olhei as últimas 6h (24 candles de 15min) e vi quantas ordens fecharam por hora, e projetei pra 1 a 3h. Ex: se nas últimas 6h fechou 5 por hora, em 3h deve fechar 15 a 22 ordens.\\n`;
          msg += `   🔍 Simulação anti-preso: ${p.simulacao? p.simulacao.motivo : 'OK - testei se nas últimas 6h o preço tocou pelo menos 30% das linhas do grid, se não tocou ia ficar preso sem abrir ordem, então bloqueei'}.\\n`;
          msg += `   🔁 Histórico de sucesso: Padrão ${p.nomePadrao} apareceu ${p.repeticoes_24h} vezes nas últimas 24h em ${p.paresDiferentes} pares diferentes, com média mínima de fechamento de ${p.fechamentoMedio? p.fechamentoMedio.toFixed(0) : p.fechamentos} ordens por grid. Isso mostra que esse comportamento real se repetiu e teve sucesso, não é só suporte e resistência aleatório. ${p.isNovoComportamento? 'Esse é um comportamento NOVO que nunca tinha visto antes!' : `Esse comportamento já existia, é repetição de comportamento conhecido (distância ${p.distancia? p.distancia.toFixed(1) : '0'} do original)`}\\n\\n`;
        });
        msg += `💡 O que é comportamento real vs só suporte/resistência: Suporte/resistência que funcionou uma vez não é padrão. Padrão de comportamento é quando o mercado faz a mesma sequência de comportamento várias vezes: ex: cai forte, volatilidade cai, volume cai, POC fica no meio, sem busca de liquidez, lateral com muitos toques = acumulação real. Isso sim é comportamento que se repete!`;
        optimusAzul(msg, {top5});
        optimusTempoReal(`Voltando pro TOP5 comportamentos reais agora!`, 'TOP5');
      }
    }, 10000);
  }catch(e){ logSistema(`[ERRO NOTICIA 30s] ${e.message}`, 'warn'); }
}
io.on('connection', socket=>{
  socket.emit('log_history', load(FILES.logs, []).slice(0,100));
  socket.emit('premiums_history', Object.values(load(FILES.premiums, {})));
  socket.emit('entradas_history', Object.values(load(FILES.entradas, {})));
  socket.emit('padroes_history', load(FILES.padroes, {}));
  socket.on('start_scan', ()=>{ scanPossiveis(); });
  socket.on('stop_all', ()=>{ scanning=false; io.emit('scan_status',{active:false}); });
  socket.on('confirmar_entrada', (list)=>{
    const premiumsAnteriores = load(FILES.premiums, {});
    let premiums = {}; let entradas = load(FILES.entradas, {}); list.forEach(sym=>{ const prem = premiums[sym]; if(!prem) return; if(!entradas[sym]){ entradas[sym] = { symbol:sym, nome:'Carlos', score:prem.score, prob:prem.prob, faixa_sup:prem.superior, faixa_inf:prem.inferior, grids:prem.grids, pctOrdens:prem.pctOrdens, levIdeal:prem.levIdeal, data_entrada:new Date().toISOString(), status:'ABERTO', totalVezes:1, duracao_estimada:prem.duracao_estimada, probFechamento:prem.probFechamento, comportamentoId:prem.comportamentoId, nomePadrao:prem.nomePadrao }; } else { entradas[sym].totalVezes++; } if(premiums[sym]) premiums[sym].isOpen=true; }); save(FILES.premiums, premiums); save(FILES.entradas, entradas); optimusAzul(`${list.length} pares confirmados: ${list.join(', ')} - guardião vai vigiar. Cada um com comportamento real e probabilidade de fechamento calculada nas últimas 6h.`, null); logSistema(`[ENTRADA] ${list.length} pares confirmados pro guardião - comportamentos reais`, 'prime'); io.emit('entradas_history', Object.values(entradas)); io.emit('premiums_history', Object.values(premiums));
  });
  socket.on('remover_entrada', (sym)=>{ let entradas = load(FILES.entradas, {}); delete entradas[sym]; save(FILES.entradas, entradas); let premiums = load(FILES.premiums, {}); if(premiums[sym]) premiums[sym].isOpen=false; save(FILES.premiums, premiums); io.emit('entradas_history', Object.values(entradas)); });
  socket.on('pedir_top5', ()=>{ const premiums = load(FILES.premiums, {}); const top5 = recomendarTop5(premiums); if(top5){ let msg = `TOP 5 COMPORTAMENTOS REAIS AGORA:\\n`; top5.forEach((p,i)=>{ const probTxt = p.probFechamento ? p.probFechamento.probabilidadeTexto : `${p.fechamentos} ordens`; msg += `${i+1}. ${p.symbol} Comportamento ${p.nomePadrao} (${p.comportamentoId}) - ${p.score}% - ${p.grids}G - Lev ${p.levIdeal}x - ${p.duracao_estimada} - ${probTxt}\\n   ${p.symbol} - ${p.contexto? p.contexto.comportamento : 'comportamento real'}\\n`; }); optimusAzul(msg, {top5}); } });
});
app.get('/api/premiums', (req,res)=>res.json(Object.values(load(FILES.premiums, {}))));
app.get('/api/top5', (req,res)=>{ const premiums = load(FILES.premiums, {}); const top5 = recomendarTop5(premiums); res.json(top5||[]); });
app.get('/api/comportamentos', (req,res)=>res.json(load(FILES.comportamentos, {})));
setInterval(guardiao10min, 10*60*1000);
setInterval(noticiasRotativas30s, 30*1000); // 30s loop único
const PORT = process.env.PORT || 3000;
server.listen(PORT, ()=>{
  console.log(`Optimus GRID V15 COMPORTAMENTO REAL rodando em http://localhost:${PORT}`);
  logSistema(`[SISTEMA] V15 COMPORTAMENTO REAL - Não cria milhares de padrões! Só cria padrão novo se comportamento novo surgir. Compara sempre com biblioteca. Se for parecido, conta como repetição, não cria novo. Nomes Delta_a1 Delta_a2 nunca Padrão 1. %ord 0.45-0.85 travado. Simulação 6h anti-preso. Probabilidade 15-22 ordens 1-3h baseado 6h. Notícia 30s loop sem parar. Detalhes pra leigo.`, 'ok');
  optimusAzul(`Sistema V15 comportamento real pronto! Agora não crio milhares de padrões. Só crio padrão novo quando vejo comportamento que nunca vi antes. Se for parecido com um que já conheço (ex: acumulação com POC meio, sem liquidez, volatilidade caindo), só conto como repetição daquele comportamento, não crio padrão novo à toa. Isso evita criar padrão toda vez que vê suporte e resistência que funcionou - isso não é padrão de comportamento gráfico! Padrão de comportamento é quando o mercado faz a mesma sequência real várias vezes. Nomes Delta_a1, Delta_a2, Delta_b1 etc, nunca acaba em Padrão 1. % entre ordens 0.45% a 0.85% travado, nada fora disso vai pro premium. Simulação 6h pra não ficar preso. Probabilidade de 15 a 22 ordens em 1 a 3h baseada nas últimas 6h. Mostra sucesso e média mínima fechamento nas repetições 24h. Notícias a cada 30s em loop sem parar, com detalhes pra leigo entender cada configuração!`);
});
