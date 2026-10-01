const fs = require('fs');
const path = require('path');
const { DatabaseSync } = require('node:sqlite');

const root = __dirname;
const data = path.join(root, 'data');
const dbPath = path.join(data, 'optimus.db');

function assert(ok, msg){ if(!ok) throw new Error(msg); }

console.log('OPTIMUS V17 self-test');
console.log('Node:', process.version);
assert(Number(process.versions.node.split('.')[0]) >= 22, 'Node 22+ é obrigatório.');

if(!fs.existsSync(data)) fs.mkdirSync(data, {recursive:true});

const db = new DatabaseSync(dbPath);
db.exec(`
  PRAGMA journal_mode = WAL;
  CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS analysis_cursor(symbol TEXT PRIMARY KEY,last_closed_ts INTEGER NOT NULL DEFAULT 0,last_scan_ts INTEGER NOT NULL DEFAULT 0,candles_processed INTEGER NOT NULL DEFAULT 0);
  CREATE TABLE IF NOT EXISTS premium_events(id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT NOT NULL,event TEXT NOT NULL,reason TEXT,score REAL,comportamento_id TEXT,created_at TEXT NOT NULL);
`);

const files = ['biblioteca_viva.json','estado_premium.json','comportamentos.json','ultima_analise_24h.json','padroes.json','possiveis.json','performance_periodo.json','entradas.json','premiums.json','logs.json','mapa_padroes_nomes.json'];
for(const f of files) assert(fs.existsSync(path.join(data,f)), `Arquivo ausente: ${f}`);

function dbSet(k,v){ db.prepare(`INSERT INTO kv(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`).run(k,JSON.stringify(v),new Date().toISOString()); }
function dbGet(k){ const r=db.prepare('SELECT value FROM kv WHERE key=?').get(k); return r ? JSON.parse(r.value) : undefined; }

for(const f of files){
  const key=f.replace(/\.json$/,'');
  if(dbGet(key)===undefined){
    dbSet(key,JSON.parse(fs.readFileSync(path.join(data,f),'utf8')));
  }
}
assert(dbGet('premiums') !== undefined, 'Migração de premiums não foi inicializada.');
assert(dbGet('biblioteca_viva') !== undefined, 'Migração da biblioteca não foi inicializada.');

db.prepare(`INSERT INTO analysis_cursor(symbol,last_closed_ts,last_scan_ts,candles_processed) VALUES(?,?,?,?) ON CONFLICT(symbol) DO UPDATE SET last_closed_ts=excluded.last_closed_ts`).run('__SELFTEST__',123,Date.now(),10);
const cursor=db.prepare('SELECT last_closed_ts FROM analysis_cursor WHERE symbol=?').get('__SELFTEST__');
assert(cursor && Number(cursor.last_closed_ts)===123,'Cursor incremental falhou.');

db.prepare('INSERT INTO premium_events(symbol,event,reason,score,comportamento_id,created_at) VALUES(?,?,?,?,?,?)')
  .run('__SELFTEST__','REMOVIDO','teste de integridade',80,'TEST_A1',new Date().toISOString());
const ev=db.prepare('SELECT COUNT(*) AS n FROM premium_events WHERE symbol=?').get('__SELFTEST__');
assert(Number(ev.n)>=1,'Registro de evento do Premium falhou.');

db.prepare('DELETE FROM analysis_cursor WHERE symbol=?').run('__SELFTEST__');
db.prepare('DELETE FROM premium_events WHERE symbol=?').run('__SELFTEST__');
db.close();

console.log('OK: SQLite, tabelas, migração JSON -> SQLite, cursor incremental e eventos do Premium.');
