'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || '0.0.0.0';
const ROOT = __dirname;
const DB_FILE = path.join(ROOT, 'ltl-db.json');
const INDEX_FILE = path.join(ROOT, 'index (1).html');
const clients = new Set();
const online = new Map();

function loadDb(){
  try { return JSON.parse(fs.readFileSync(DB_FILE,'utf8')); }
  catch(e){ return { state: {}, updatedAt: Date.now() }; }
}
let db = loadDb();
if (!db.state || typeof db.state !== 'object') db.state = {};

function saveDb(){
  const tmp = DB_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(db, null, 2), 'utf8');
  fs.renameSync(tmp, DB_FILE);
}
function send(res,status,body,type='application/json'){
  res.writeHead(status, {'Content-Type':type,'Cache-Control':'no-store','Access-Control-Allow-Origin':'*'});
  res.end(type==='application/json' ? JSON.stringify(body) : body);
}
function body(req){
  return new Promise((resolve,reject)=>{
    let raw=''; req.on('data',c=>{raw+=c;if(raw.length>20*1024*1024) req.destroy();});
    req.on('end',()=>{try{resolve(raw?JSON.parse(raw):{});}catch(e){reject(e);}}); req.on('error',reject);
  });
}
function broadcast(message, eventName){
  const payload = `event: ${eventName || 'message'}\ndata: ${JSON.stringify(message)}\n\n`;
  for(const res of clients){ try{res.write(payload);}catch(e){} }
}
function cleanupOnline(){
  const cutoff=Date.now()-60000;
  for(const [id,v] of online) if(v.lastSeen<cutoff) online.delete(id);
}
setInterval(()=>{cleanupOnline();broadcast({count:online.size},'online');},10000);

const server=http.createServer(async (req,res)=>{
  const url=new URL(req.url, `http://${req.headers.host||'localhost'}`);
  if(req.method==='OPTIONS'){
    res.writeHead(204, {'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,PUT,POST,OPTIONS','Access-Control-Allow-Headers':'Content-Type'}); return res.end();
  }
  if(url.pathname==='/api/state' && req.method==='GET') return send(res,200,db.state);
  if(url.pathname.startsWith('/api/state/') && req.method==='PUT'){
    const key=decodeURIComponent(url.pathname.slice('/api/state/'.length));
    if(!/^nexus_[A-Za-z0-9_]+$/.test(key) || key==='nexus_session' || key==='nexus_theme' || key==='nexus_view_mode' || key==='nexus_online') return send(res,400,{error:'invalid key'});
    try{
      const b=await body(req);
      if(b.value===null || typeof b.value==='undefined') delete db.state[key];
      else db.state[key]=b.value;
      db.updatedAt=Date.now(); saveDb();
      broadcast({key, value:db.state[key], deleted:!Object.prototype.hasOwnProperty.call(db.state,key)});
      return send(res,200,{ok:true});
    }catch(e){return send(res,400,{error:'bad json'});}
  }
  if(url.pathname==='/api/events' && req.method==='GET'){
    res.writeHead(200, {'Content-Type':'text/event-stream','Cache-Control':'no-cache','Connection':'keep-alive','Access-Control-Allow-Origin':'*'});
    res.write(': connected\n\n'); clients.add(res); res.on('close',()=>clients.delete(res));
    res.write(`event: online\ndata: ${JSON.stringify({count:online.size})}\n\n`); return;
  }
  if(url.pathname==='/api/online' && req.method==='GET'){cleanupOnline(); return send(res,200,{count:online.size, users:[...online.values()].map(x=>x.username)});}
  if(url.pathname==='/api/online/heartbeat' && req.method==='POST'){
    try{const b=await body(req); if(!b.clientId) return send(res,400,{error:'clientId required'}); online.set(String(b.clientId),{username:String(b.username||'Гость'),lastSeen:Date.now()}); cleanupOnline(); broadcast({count:online.size},'online'); return send(res,200,{ok:true,count:online.size});}
    catch(e){return send(res,400,{error:'bad json'});}
  }
  if(req.method==='GET' && (url.pathname==='/' || url.pathname==='/index.html' || url.pathname==='/index%20(1).html')){
    try{const html=fs.readFileSync(INDEX_FILE); res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store','Access-Control-Allow-Origin':'*'}); return res.end(html);}catch(e){return send(res,500,{error:e.message});}
  }
  if(req.method==='GET' && url.pathname==='/health') return send(res,200,{ok:true,online:online.size,updatedAt:db.updatedAt});
  return send(res,404,{error:'not found'});
});

server.listen(PORT,HOST,()=>console.log(`LTL backend: http://localhost:${PORT}`));
