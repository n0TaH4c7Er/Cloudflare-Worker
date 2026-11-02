// HTTP compatibility layer for the existing retry-safe binary relay core.
import {Buffer} from 'node:buffer';
import {EventEmitter} from 'node:events';
import {randomUUID,randomBytes} from 'node:crypto';

class HttpResponse extends EventEmitter {
 constructor(resolve){super();this.resolve=resolve;this.headers=new Headers();this.status=200;this.headersSent=false;this.destroyed=false;this.writableEnded=false;}
 setHeader(k,v){this.headers.set(k,v);}
 writeHead(code,headers={}){this.status=code;for(const [k,v]of Object.entries(headers))this.setHeader(k,v);}
 flushHeaders(){
  if(this.headersSent)return;this.headersSent=true;
  const stream=new ReadableStream({start:c=>{this.controller=c;},pull:()=>{queueMicrotask(()=>{if(!this.destroyed&&!this.writableEnded)this.emit('drain');});},cancel:()=>{this.destroyed=true;this.emit('close');}}, {highWaterMark:65536,size:b=>b.byteLength});
  this.resolve(new Response(this.status===204?null:stream,{status:this.status,headers:this.headers}));
 }
 write(data){if(this.destroyed||this.writableEnded)return false;this.flushHeaders();try{this.controller.enqueue(Buffer.isBuffer(data)?data:Buffer.from(data));return this.controller.desiredSize>0;}catch{this.destroyed=true;this.emit('close');return false;}}
 end(data){if(this.destroyed||this.writableEnded)return;if(data!==undefined)this.write(data);this.flushHeaders();this.writableEnded=true;try{this.controller.close();}catch{}this.emit('close');}
 abort(){if(this.destroyed)return;this.destroyed=true;try{this.controller?.error(Error('Request cancelled'));}catch{}this.emit('close');}
}
function requestAdapter(request){return {method:request.method,url:new URL(request.url).pathname+new URL(request.url).search,headers:Object.fromEntries(request.headers),async *[Symbol.asyncIterator](){const reader=request.body?.getReader();if(!reader)return;try{for(;;){const item=await reader.read();if(item.done)break;yield Buffer.from(item.value);}}finally{reader.releaseLock();}}};}

function upstreamFactory(ctx){return class Upstream extends EventTarget {
 constructor(url,protocols=[]){super();this.readyState=0;this.protocol='';this.connection=this.connect(url,protocols);ctx.waitUntil(this.connection);}
 async connect(url,protocols){
  const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),15000);
  try{
   const target=new URL(url);if(!['ws:','wss:'].includes(target.protocol))throw Error('WebSocket URL required');target.protocol=target.protocol==='wss:'?'https:':'http:';
   if(!Array.isArray(protocols)||protocols.some(x=>typeof x!=='string'||!/^[-!#$%&'*+.^_`|~0-9A-Za-z]+$/.test(x)))throw Error('Invalid WebSocket subprotocol');
   const headers=new Headers({Upgrade:'websocket'});if(protocols.length)headers.set('Sec-WebSocket-Protocol',protocols.join(', '));
   const response=await fetch(target,{headers,signal:controller.signal,redirect:'manual'}),ws=response.webSocket;
   if(response.status!==101||!ws)throw Error('Upstream refused WebSocket upgrade ('+response.status+')');
   if(this.readyState!==0){ws.accept();ws.close(1000,'Client cancelled');return;}
   this.socket=ws;this.protocol=response.headers.get('Sec-WebSocket-Protocol')||'';ws.binaryType='arraybuffer';
   for(const type of ['message','error','close'])ws.addEventListener(type,event=>{
    if(type==='close')this.readyState=3;
    const forwarded=type==='message'?new MessageEvent(type,{data:event.data}):new Event(type);
    if(type!=='message')for(const k of ['code','reason','wasClean','message'])if(event[k]!==undefined)Object.defineProperty(forwarded,k,{value:event[k]});
    this.dispatchEvent(forwarded);
   });
   ws.accept();this.readyState=1;this.dispatchEvent(new Event('open'));
  }catch(error){if(this.readyState===3)return;this.readyState=3;const event=new Event('error');Object.defineProperty(event,'message',{value:error.message});this.dispatchEvent(event);const closed=new Event('close');Object.assign(closed,{code:1006,reason:'Upstream connection failed',wasClean:false});this.dispatchEvent(closed);}
  finally{clearTimeout(timer);}
 }
 send(data){if(this.readyState!==1)throw Error('Socket is not open');this.socket.send(data);}
 close(){if(this.readyState===3)return;this.readyState=3;try{this.socket?.close(1000,'Client closed');}catch{}}
};}


// Validate Wisp v1 before accepting a proxy session, so a bad upstream permits
// the client to try its next Cloudflare endpoint or Render before sending TLS.
function wispUpstreamFactory(Upstream,ctx){return class WispUpstream extends EventTarget {
 constructor(url){
  super();this.readyState=0;this.protocol='';this.binaryType='arraybuffer';
  this.connection=new Promise(resolve=>this.resolveConnect=resolve);
  this.raw=new Upstream(url);this.timer=setTimeout(()=>this.fail('WISP initial handshake timed out'),22000);
  this.raw.addEventListener('message',event=>{
   if(this.readyState===3)return;
   if(this.readyState===0){
    const bytes=typeof event.data==='string'?null:new Uint8Array(event.data);
    if(!bytes||bytes.length!==9||bytes[0]!==3||new DataView(bytes.buffer,bytes.byteOffset,bytes.byteLength).getUint32(1,true)!==0||new DataView(bytes.buffer,bytes.byteOffset,bytes.byteLength).getUint32(5,true)===0){this.fail('WISP v1 initial handshake rejected');return;}
    clearTimeout(this.timer);this.readyState=1;this.dispatchEvent(new Event('open'));this.resolveConnect();
   }
   this.dispatchEvent(new MessageEvent('message',{data:event.data}));
  });
  this.raw.addEventListener('error',event=>this.fail(event.message||'WISP connection failed'));
  this.raw.addEventListener('close',event=>this.finish(event.code||1006,event.reason||'WISP upstream closed',!!event.wasClean));
  ctx.waitUntil(this.connection);
 }
 send(data){if(this.readyState!==1)throw Error('WISP socket is not open');this.raw.send(data);}
 fail(reason){if(this.readyState===3)return;const e=new Event('error');Object.defineProperty(e,'message',{value:reason});this.dispatchEvent(e);this.finish(1006,reason,false);}
 finish(code=1000,reason='',wasClean=true){
  if(this.readyState===3)return;this.readyState=3;clearTimeout(this.timer);this.raw.close();this.resolveConnect();
  const e=new Event('close');Object.assign(e,{code,reason,wasClean});this.dispatchEvent(e);
 }
 close(){this.finish();}
};}

// Every account uses ONE named object, so shared worlds and data sessions meet in
// the same stateful instance. State is transient: deploy/restarts disconnect games.
export class RelayHub {
 constructor(ctx,env){
  this.ctx=ctx;this.env=env;this.handler=createRelay(env,ctx);
  this.sql=ctx.storage.sql;
  this.sql.exec('CREATE TABLE IF NOT EXISTS relay_usage (day TEXT PRIMARY KEY, requests INTEGER NOT NULL)');
 }
 fetch(request){
  const day=new Date().toISOString().slice(0,10);
  const row=[...this.sql.exec('INSERT INTO relay_usage (day,requests) VALUES (?,1) ON CONFLICT(day) DO UPDATE SET requests=requests+1 RETURNING requests',day)][0];
  const used=Number(row.requests),cutoff=Math.min(90000,Math.max(1000,Number(this.env.NEW_SESSION_CUTOFF)||60000));
  const retryAfter=Math.max(1,Math.ceil((Date.parse(day+'T00:00:00Z')+86400000-Date.now())/1000));
  const path=new URL(request.url).pathname;
  if(request.method==='GET'&&path==='/health')return Response.json({ok:true,version:'2026-11-01.cloudflare-gust-pool3',protocol:2,binaryProtocol:3,sharedWorlds:true,httpWorlds:true,receiveStream:true,gustWisp:true,cloudmoonServers:true,pool:{acceptingNewSessions:used<cutoff,httpRequestsObserved:used,newSessionCutoff:cutoff,day,retryAfter}}, {headers:cors});
  if(request.method==='POST'&&['/sessions','/cloudmoon-servers'].includes(path)&&used>=cutoff)return Response.json({error:'Cloudflare quota reserve: new sessions use the fallback relay',retryAfter},{status:503,headers:cors});
  return new Promise(resolve=>{const res=new HttpResponse(resolve);request.signal.addEventListener('abort',()=>res.abort(),{once:true});Promise.resolve(this.handler(requestAdapter(request),res)).catch(error=>{if(!res.headersSent){res.writeHead(500,{'Content-Type':'application/json'});res.end(JSON.stringify({error:'Relay request failed'}));}else res.abort();});});
 }
}
const cors={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Headers':'Content-Type,X-Relay-Key,X-Relay-Ack','Access-Control-Allow-Methods':'GET,POST,DELETE,OPTIONS','Access-Control-Max-Age':'600','Cache-Control':'no-store'};
export default {async fetch(request,env){
 if(request.method==='OPTIONS'&&(!env.RELAY_KEY||!env.ALLOWED_TARGETS))return new Response(null,{status:204,headers:cors});
 if(!env.RELAY_KEY||!env.ALLOWED_TARGETS)return Response.json({error:'Configure RELAY_KEY and ALLOWED_TARGETS'},{status:503,headers:cors});
 if(request.method!=='OPTIONS'&&new URL(request.url).pathname!=='/health'&&request.headers.get('X-Relay-Key')!==env.RELAY_KEY)return Response.json({error:'Invalid relay key'},{status:401,headers:cors});
 return env.RELAY.get(env.RELAY.idFromName('math-practice-relay-v1')).fetch(request);
}};

function createRelay(env,ctx){
 const WebSocket=upstreamFactory(ctx);
 const WispUpstream=wispUpstreamFactory(WebSocket,ctx);
 const GUST_WISP_URL=env.GUST_WISP_URL||'wss://wisp.mercurywork.shop/';
const startedAt=Date.now();
// Eaglercraft shared-world relay protocol v1; no client-IP matching.

const sharedWorlds=new Map();
function ascii8(value=''){const data=Buffer.from(String(value),'latin1');if(data.length>255)throw Error('String too long');return Buffer.concat([Buffer.from([data.length]),data]);}
function ascii16(value=''){const data=Buffer.from(String(value),'latin1');if(data.length>65535)throw Error('String too long');const n=Buffer.alloc(2);n.writeUInt16BE(data.length);return Buffer.concat([n,data]);}
function packet(type,...parts){return Buffer.concat([Buffer.from([type]),...parts]);}
function reader(data){
 const b=Buffer.from(data);let offset=0;
 function take(n){if(offset+n>b.length)throw Error('Truncated signaling packet');const out=b.subarray(offset,offset+n);offset+=n;return out;}
 return {byte:()=>take(1)[0],str8:()=>take(take(1)[0]).toString('latin1'),str16:()=>take(take(2).readUInt16BE()).toString('latin1')};
}
// No STUN, TURN or native ICE is used in this HTTP-only edition.
const iceEntries=[];
function icePacket(){return Buffer.from([1,0,0]);}
class SharedWorldSocket extends EventTarget{
 constructor(){
  super();this.readyState=0;this.protocol='';this.role=null;this.room=null;this.peerId=null;this.finished=false;
  this.timeout=setTimeout(()=>this.error(3,'Shared-world handshake timed out'),15000);this.timeout.unref?.();
  queueMicrotask(()=>{if(this.readyState===0){this.readyState=1;this.dispatchEvent(new Event('open'));}});
 }
 message(data){if(this.readyState===1)this.dispatchEvent(new MessageEvent('message',{data}));}
 error(code,reason){this.message(packet(255,Buffer.from([code]),ascii16(reason)));this.close();}
 send(data){
  if(this.readyState!==1)return;
  try{
   if(typeof data==='string')throw Error('Binary signaling packets required');
   if(data.byteLength>131072)throw Error('Signaling packet too large');
   const r=reader(data),type=r.byte();
   if(!this.role){
    if(type!==0)throw Error('Expected shared-world handshake');
    const role=r.byte(),version=r.byte(),code=r.str8();
    if(version!==1){this.error(1,'Unsupported shared-world protocol');return;}
    clearTimeout(this.timeout);
    if(role===3){this.message(packet(105,Buffer.from([1]),ascii8('Math Practice shared worlds'),ascii8('MathPractice')));this.close();return;}
    if(role===4){this.message(packet(7,Buffer.from([0])));this.close();return;}
    if(role===1){
     if(sharedWorlds.size>=50){this.error(0,'Shared-world relay is full');return;}
     let join;do{join=randomBytes(4).toString('hex').slice(0,6).toUpperCase();}while(sharedWorlds.has(join));
     this.role='host';this.room={code:join,host:this,peers:new Map()};sharedWorlds.set(join,this.room);
     this.message(packet(0,Buffer.from([1,1]),ascii8(join)));this.message(icePacket());
     console.log('[shared-world] Hosted a world');return;
    }
    if(role===2){
     const room=sharedWorlds.get(code.toUpperCase());
     if(!room){this.error(5,'World not found. Check the code and keep the host online.');return;}
     if(room.peers.size>=16){this.error(0,'Too many pending world joins');return;}
     this.role='guest';this.room=room;this.peerId=randomBytes(8).toString('hex').toUpperCase();
     room.peers.set(this.peerId,this);
     this.message(packet(0,Buffer.from([2,1]),ascii8(this.peerId)));this.message(icePacket());
     room.host.message(packet(2,ascii8(this.peerId)));
     this.timeout=setTimeout(()=>{this.disconnect(2,'HTTP world connection timed out. Keep both pages open and check the relay connection.');},45000);this.timeout.unref?.();return;
    }
    throw Error('Unknown handshake role');
   }
   if(type===3||type===4){
    const id=r.str8(),payload=r.str16();
    if(this.role==='host'){
     const peer=this.room.peers.get(id);if(!peer)return;
     peer.message(packet(type,ascii8(''),ascii16(payload)));
    }else this.room.host.message(packet(type,ascii8(this.peerId),ascii16(payload)));
    return;
   }
   if(this.role==='guest'&&(type===5||type===6)){
    r.str8();this.room.host.message(packet(type,ascii8(this.peerId)));
    this.finished=true;this.disconnect(type===5?0:1,type===5?'Successful connection':'Peer connection failed');return;
   }
   if(this.role==='host'&&type===254){
    const id=r.str8(),code=r.byte(),reason=r.str16();
    this.room.peers.get(id)?.disconnect(code,reason);return;
   }
   throw Error('Unexpected signaling packet');
  }catch(error){this.error(2,error.message);}
 }
 disconnect(code,reason){
  if(this.role==='guest'){
   const message=packet(254,ascii8(this.peerId),Buffer.from([code]),ascii16(reason));
   this.message(message);if(!this.finished)this.room.host.message(message);this.finished=true;
  }
  this.close();
 }
 close(){
  if(this.readyState>=2)return;
  this.readyState=3;clearTimeout(this.timeout);
  if(this.role==='host'){
   sharedWorlds.delete(this.room.code);
   for(const peer of [...this.room.peers.values()])peer.disconnect(5,'Host stopped sharing the world');
   this.room.peers.clear();
  }else if(this.role==='guest'){
   this.room.peers.delete(this.peerId);
   if(!this.finished&&this.room.host.readyState===1)this.room.host.message(packet(254,ascii8(this.peerId),Buffer.from([255]),ascii16('Guest disconnected')));
  }
  const event=new Event('close');Object.assign(event,{code:1000,reason:'Shared-world signaling closed',wasClean:true});this.dispatchEvent(event);
 }
}


// HTTP-only game data links. Tokens travel through the shared-world signaling.
const httpWorldLinks=new Map();
const PEER_READY='__math_http_peer_ready_v1__',PEER_CONNECTED='__math_http_peer_connected_v1__';
class HttpWorldDataSocket extends EventTarget{
 constructor(role,token){
  super();
  if(!['offer','answer'].includes(role)||!/^[a-f0-9-]{36}$/i.test(token))throw Error('Invalid HTTP world link');
  let link=httpWorldLinks.get(token);
  if(role==='offer'){
   if(link)throw Error('World link already exists');
   if(httpWorldLinks.size>=100)throw Error('World data relay is full');
   link={token,offer:null,answer:null,ready:false,closed:false,timer:null};httpWorldLinks.set(token,link);
   link.timer=setTimeout(()=>this.close(),60000);link.timer.unref?.();
  }else if(!link||link.closed||!link.offer)throw Error('World host is no longer available');
  if(link[role])throw Error('World link endpoint already connected');
  this.link=link;this.role=role;this.readyState=0;this.protocol='';this.clientReady=false;link[role]=this;
  queueMicrotask(()=>{if(this.readyState===0){this.readyState=1;this.dispatchEvent(new Event('open'));}});
 }
 message(data){if(this.readyState===1)this.dispatchEvent(new MessageEvent('message',{data}));}
 send(data){
  if(this.readyState!==1)return;
  if(data===PEER_READY){
   this.clientReady=true;
   if(!this.link.ready&&this.link.offer?.clientReady&&this.link.answer?.clientReady){
    this.link.ready=true;clearTimeout(this.link.timer);
    this.link.offer.message(PEER_CONNECTED);this.link.answer.message(PEER_CONNECTED);
   }
   return;
  }
  if(!this.link.ready)throw Error('World data arrived before negotiation finished');
  const other=this.link[this.role==='offer'?'answer':'offer'];
  if(!other||other.readyState!==1)throw Error('Other player disconnected');
  other.message(data);
 }
 close(){
  const link=this.link;if(this.readyState>=2)return;
  link.closed=true;clearTimeout(link.timer);httpWorldLinks.delete(link.token);
  for(const endpoint of [link.offer,link.answer]){
   if(!endpoint||endpoint.readyState>=2)continue;
   endpoint.readyState=3;
   const event=new Event('close');Object.assign(event,{code:1000,reason:'HTTP world connection closed',wasClean:true});endpoint.dispatchEvent(event);
  }
 }
}


const VERSION='2026-11-01.cloudflare-gust-pool3';
const targets=new Set((env.ALLOWED_TARGETS||'').split(',').map(x=>x.trim()).filter(Boolean).map(x=>x==='*'?x:new URL(x).href));
const instanceId=randomUUID(),closedSessions=new Map();
const key=env.RELAY_KEY,sessions=new Map(),limit=2097152,queueLimit=8*1024*1024;
if(!targets.size||!key)throw Error('Set ALLOWED_TARGETS and RELAY_KEY.');
function dispose(s,reason='Client closed session'){
 if(s.disposed)return;s.disposed=true;
 console.log('[relay]',s.id,reason,'peakQueueBytes='+s.peakBytes,'receivePauses='+(s.pauseCount||0));
 closedSessions.set(s.id,{reason,time:Date.now()});
 if(closedSessions.size>1000)closedSessions.delete(closedSessions.keys().next().value);
 try{if(s.paused)s.ws.resume?.();s.ws.close();}catch{}
 sessions.delete(s.id);s.wake?.();wakeSenders(s);s.streamResponse?.end();
}
function applyReceivePressure(s){
 if(typeof s.ws.pause!=='function'||s.ws.readyState!==1)return;
 if(!s.paused&&s.bytes>=4*1024*1024){
  s.paused=true;s.pauseCount=(s.pauseCount||0)+1;s.ws.pause();
  if(s.pauseCount===1)console.log('[relay]',s.id,'Pausing upstream until browser acknowledges world data');
 }else if(s.paused&&s.bytes<=1024*1024){s.paused=false;s.ws.resume();}
}
function binaryEvent(e){
 const types={open:1,message:e.bytes!==undefined?2:3,error:4,close:5};
 const payload=e.type==='message'?(e.bytes!==undefined?Buffer.from(e.bytes):Buffer.from(e.text,'utf8')):Buffer.from(JSON.stringify(e));
 const out=Buffer.allocUnsafe(13+payload.length);out.writeUInt32BE(9+payload.length,0);out[4]=types[e.type];out.writeDoubleBE(e.id,5);payload.copy(out,13);return out;
}
const binaryHeartbeat=Buffer.alloc(13);binaryHeartbeat.writeUInt32BE(9,0);
function decodeBatch(bytes){
 if(bytes.length<14||bytes.readUInt32BE(0)!==0x4d504233)throw Error('Invalid binary batch');
 const received=bytes.readDoubleBE(4),count=bytes.readUInt16BE(12),packets=[];let at=14;
 if(count<1||count>256)throw Error('Invalid binary packet count');
 for(let i=0;i<count;i++){
  if(at+13>bytes.length)throw Error('Truncated binary batch');
  const type=bytes[at],seq=bytes.readDoubleBE(at+1),size=bytes.readUInt32BE(at+9);at+=13;
  if(type>1||at+size>bytes.length)throw Error('Invalid binary packet');
  const data=bytes.subarray(at,at+size);packets.push(type?{seq,text:data.toString('utf8')}:{seq,bytes:data});at+=size;
 }
 if(at!==bytes.length)throw Error('Trailing binary batch data');return {received,packets,pipeline:true};
}
function push(s,e){
 if(s.disposed)return;e.id=++s.eventSeq;
 const wire=s.binary?binaryEvent(e):JSON.stringify(e)+'\n',size=Buffer.byteLength(wire);
 if(size>4*1024*1024||s.bytes+size>queueLimit||[...sessions.values()].reduce((n,x)=>n+x.bytes,0)+size>24*1024*1024){dispose(s,'Cloudflare relay receive buffer full; reconnect with a stable connection');return;}
 Object.defineProperties(e,{_wire:{value:wire},_bytes:{value:size}});
 s.bytes+=size;s.peakBytes=Math.max(s.peakBytes||0,s.bytes);s.events.push(e);applyReceivePressure(s);s.pump?.();s.wake?.();
}
function acknowledge(s,value=0){
 const ack=Number(value);
 if(!Number.isSafeInteger(ack)||ack<0||ack>s.eventSeq)throw Error('Invalid receive acknowledgement');
 if(ack>s.acked){
  let count=0;while(count<s.events.length&&s.events[count].id<=ack)s.bytes-=s.events[count++]._bytes;
  s.events.splice(0,count);s.bytes=Math.max(0,s.bytes);s.acked=ack;applyReceivePressure(s);
 }
}
function wakeSenders(s){for(const wake of s.sendWaiters||[])wake();s.sendWaiters?.clear();}
async function waitForSendTurn(s,first){
 const deadline=Date.now()+6500;
 while(first>s.seq+1){
  if(s.ws.readyState!==1)throw Error('Upstream socket is not open');
  if(Date.now()>=deadline)throw Object.assign(Error('Outgoing packet gap timed out'),{status:503});
  if((s.sendWaiters?.size||0)>=8)throw Object.assign(Error('Too many pending send requests'),{status:429});
  await new Promise(resolve=>{
   s.sendWaiters??=new Set();
   const finish=()=>{clearTimeout(timer);s.sendWaiters.delete(finish);resolve();};
   const timer=setTimeout(finish,Math.max(1,deadline-Date.now()));s.sendWaiters.add(finish);
  });
 }
}

const handler=async(req,res)=>{
 res.setHeader('Access-Control-Allow-Origin','*');
 res.setHeader('Access-Control-Allow-Headers','Content-Type,X-Relay-Key,X-Relay-Ack');
 res.setHeader('Access-Control-Allow-Methods','GET,POST,DELETE,OPTIONS');
 res.setHeader('Cache-Control','no-store');
 res.setHeader('Access-Control-Max-Age','600');
 const reply=(code,data)=>{res.writeHead(code,{'Content-Type':'application/json'});res.end(JSON.stringify(data));};
 if(req.method==='GET'&&req.url==='/health'){reply(200,{ok:true,version:VERSION,protocol:2,binaryProtocol:3,instanceId,uptimeSeconds:Math.floor((Date.now()-startedAt)/1000),sharedWorlds:true,worldTransport:'https-streaming',webRTC:false,receiveStream:true,gustWisp:true,cloudmoonServers:true});return;}
 if(req.method==='OPTIONS'){res.writeHead(204);res.end();return;}
 if(req.headers['x-relay-key']!==key){reply(401,{error:'Invalid relay key'});return;}
 try{
  const path=new URL(req.url,'http://relay').pathname;let body={};
  if(req.method==='POST'){let size=0,chunks=[];for await(const c of req){size+=c.length;if(size>limit){reply(413,{error:'Too large'});return;}chunks.push(c);}const raw=Buffer.concat(chunks);body=req.headers['content-type']?.split(';')[0]==='application/octet-stream'?decodeBatch(raw):JSON.parse(raw.toString()||'{}');}
  if(path==='/cloudmoon-servers'&&req.method==='POST'){
   const controller=new AbortController();
   res.on('close',()=>controller.abort());
   try{reply(200,await cloudMoonServers(body,controller.signal));}
   catch(error){if(!res.writableEnded&&!res.destroyed)reply(502,{error:'CloudMoon public server list is unavailable'});}
   return;
  }
  if(path==='/sessions'&&req.method==='POST'){
   if(!['shared-world','world-data','gust-wisp'].includes(body.kind)&&!targets.has('*')&&!targets.has(body.url)){reply(403,{error:'Server URL is not allowed'});return;}
   if(sessions.size>=12){reply(503,{error:'Relay full'});return;}
   const ws=body.kind==='gust-wisp'?new WispUpstream(GUST_WISP_URL):body.kind==='world-data'?new HttpWorldDataSocket(body.role,body.link):body.kind==='shared-world'?new SharedWorldSocket():new WebSocket(body.url,body.protocols||[],{perMessageDeflate:false,handshakeTimeout:15000,maxPayload:32*1024*1024}),s={id:randomUUID(),ws,events:[],bytes:0,last:Date.now(),seq:0,eventSeq:0,acked:0,binary:body.binaryTransport===true};
   sessions.set(s.id,s);ws.binaryType='arraybuffer';console.log('[relay]',s.id,'Connecting to',body.url);
   ws.addEventListener('open',()=>push(s,{type:'open',protocol:ws.protocol}));
   ws.addEventListener('message',e=>push(s,typeof e.data==='string'?{type:'message',text:e.data}:s.binary?{type:'message',bytes:Buffer.from(e.data)}:{type:'message',base64:Buffer.from(e.data).toString('base64')}));
   ws.addEventListener('error',e=>{console.error('[relay]',s.id,'Upstream error:',e.message||e.error?.message||'Unknown error');push(s,{type:'error',reason:e.message||'Upstream connection failed'});});
   ws.addEventListener('close',e=>{s.endReason='Game server closed connection ('+e.code+')'+(e.reason?': '+e.reason:'');console.log('[relay]',s.id,'Upstream closed:',e.code,e.reason);push(s,{type:'close',code:e.code,reason:e.reason,wasClean:e.wasClean});});
   if(ws.connection){await ws.connection;if(ws.readyState!==1){dispose(s,'Upstream connection failed before opening');reply(503,{error:'Cloudflare upstream connection failed; try fallback',retryAfter:60});return;}}
   reply(201,{id:s.id,protocol:s.binary?3:2,binaryTransport:s.binary,version:VERSION,sharedWorlds:true,httpWorlds:true,receiveStream:true,gustWisp:true,cloudmoonServers:true});return;
  }
  const m=path.match(/^\/sessions\/([a-f0-9-]+)(\/send|\/ack|\/stream|\/status)?$/),s=m&&sessions.get(m[1]);
  if(!s){const ended=m&&closedSessions.get(m[1]);reply(ended?410:404,{error:ended?ended.reason:'Session not found; the relay may have restarted',instanceId});return;}s.last=Date.now();
  if(req.method==='GET'&&m[2]==='/status'){reply(200,{queuedBytes:s.bytes,peakQueueBytes:s.peakBytes||0,paused:!!s.paused,receivePauses:s.pauseCount||0,instanceId});return;}
  if(req.method==='DELETE'){dispose(s);reply(200,{});return;}
  if(req.method==='POST'&&m[2]==='/ack'){acknowledge(s,body.received);reply(200,{received:s.acked});return;}
  if(req.method==='POST'&&m[2]==='/send'){
   acknowledge(s,body.received);
   const packets=body.packets===undefined?[body]:body.packets;
   if(!Array.isArray(packets)||!packets.length||packets.length>256)throw Error('Invalid packet batch');
   if(s.ws.readyState!==1){reply(409,{error:s.endReason||'Upstream socket is not open'});return;}
   for(let i=0;i<packets.length;i++){
    const packet=packets[i];
    if(!Number.isSafeInteger(packet.seq)||packet.seq<1||(i>0&&packet.seq!==packets[i-1].seq+1)){reply(409,{error:'Packet order mismatch'});return;}
    if(typeof packet.text!=='string'&&typeof packet.base64!=='string'&&!Buffer.isBuffer(packet.bytes))throw Error('Missing packet');
   }
   if(packets[0].seq>s.seq+1&&!body.pipeline){reply(409,{error:'Packet order mismatch'});return;}
   await waitForSendTurn(s,packets[0].seq);
   for(const packet of packets){
    if(packet.seq<=s.seq)continue;
    if(packet.seq!==s.seq+1){reply(409,{error:'Packet order mismatch'});return;}
    s.ws.send(typeof packet.text==='string'?packet.text:packet.bytes??Buffer.from(packet.base64,'base64'));s.seq=packet.seq;
   }
   wakeSenders(s);reply(200,{ack:s.seq});return;
  }
  if(req.method==='GET'&&m[2]==='/stream'){
   acknowledge(s,req.headers['x-relay-ack']||0);
   s.streamResponse?.end();
   res.writeHead(200,{'Content-Type':s.binary?'application/octet-stream':'application/x-ndjson','Cache-Control':'no-store, no-transform','X-Accel-Buffering':'no'});
   res.socket?.setNoDelay(true);res.flushHeaders();s.streamResponse=res;
   let next=s.acked+1,blocked=false;
   const pump=()=>{
    if(blocked||res.destroyed||res.writableEnded)return;
    let index=Math.max(0,next-(s.events[0]?.id||next));
    while(index<s.events.length){
     const event=s.events[index++];next=event.id+1;
     if(!res.write(event._wire)){blocked=true;return;}
    }
   };
   s.pump=pump;res.on('drain',()=>{blocked=false;pump();});
   const heartbeat=setInterval(()=>{
    if(res.destroyed)return;s.last=Date.now();
    if(!blocked)res.write(s.binary?binaryHeartbeat:'{}\n');
   },15000);heartbeat.unref?.();
   res.on('close',()=>{clearInterval(heartbeat);if(s.streamResponse===res){s.streamResponse=null;s.pump=null;}});
   pump();return;
  }
  if(req.method==='GET'&&!m[2]){
   if(s.polling){reply(409,{error:'Concurrent poll'});return;}
   acknowledge(s,req.headers['x-relay-ack']||0);
   s.polling=true;
   try{
    if(!s.events.length)await new Promise(resolve=>{
     const finish=()=>{clearTimeout(timer);s.wake=null;res.removeListener('close',finish);resolve();};
     const timer=setTimeout(finish,20000);s.wake=finish;res.once('close',finish);
    });
    if(res.destroyed)return;
    let count=0,bytes=0;
    while(count<s.events.length&&(count===0||bytes<1024*1024)){bytes+=s.events[count++]._bytes;}
    if(s.binary){res.writeHead(200,{'Content-Type':'application/octet-stream'});res.end(Buffer.concat(s.events.slice(0,count).map(e=>e._wire)));}else reply(200,{events:s.events.slice(0,count)});
   }finally{s.polling=false;}return;
  }
  reply(405,{error:'Unsupported method'});
 }catch(e){console.error('[relay] Request failed:',e.message);if(!res.headersSent)reply(e.status||400,{error:e.message});else res.end();}
};
let sweeper=null;
function clean(){
 for(const session of sessions.values())if(Date.now()-session.last>60000)dispose(session,'Session idle for 60 seconds');
 for(const [id,closed]of closedSessions)if(Date.now()-closed.time>120000)closedSessions.delete(id);
}
function schedule(){if(sweeper!==null||!sessions.size)return;sweeper=setTimeout(()=>{sweeper=null;clean();schedule();},10000);}
return async(req,res)=>{clean();try{return await handler(req,res);}finally{schedule();}};
}


// Public, read-only server discovery. No client-supplied destination, credentials,
// cookies, login bodies or account mutations are forwarded by this route.
async function cloudMoonServers(body={}, signal) {
 const hosts=['https://api.prod.cloudmoonapp.com','https://api.cloudmoon.cloudbatata.com','https://api.prod.geometry.today','https://hrz5zfjq02.execute-api.us-east-1.amazonaws.com','https://api.prod.viewoncloud.com'];
 if(typeof body.query!=='string'||body.query.length>4096)throw Error('Invalid server-list query');
 const incoming=new URLSearchParams(body.query),query=new URLSearchParams();
 for(const name of ['device_type','site','query_uuid','device_id']){
  const value=incoming.get(name);if(value!==null&&value.length<=200)query.set(name,value);
 }
 query.set('device_type','web');query.set('site','cm');
 const headers=new Headers({Accept:'application/json',Origin:'https://web.cloudmoonapp.com',Referer:'https://web.cloudmoonapp.com/'});
 for(const name of ['x-user-language','x-user-locale']){
  const value=body.headers?.[name];if(typeof value==='string'&&value.length<=100&&!/[\r\n]/.test(value))headers.set(name,value);
 }
 for(const host of hosts){
  if(signal?.aborted)throw signal.reason||Error('Request cancelled');
  try{
   const combined=signal?AbortSignal.any([signal,AbortSignal.timeout(6000)]):AbortSignal.timeout(6000);
   const response=await fetch(host+'/game/server_list?'+query,{method:'GET',headers,redirect:'manual',signal:combined});
   if(!response.ok){await response.body?.cancel();continue;}
   const reader=response.body.getReader(),chunks=[];let size=0;
   for(;;){const {value,done}=await reader.read();if(done)break;size+=value.length;if(size>128*1024){await reader.cancel();throw Error('Server list too large');}chunks.push(value);}
   const bytes=new Uint8Array(size);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length;}
   const data=JSON.parse(new TextDecoder().decode(bytes));
   if(data.code!==0||!Array.isArray(data.data?.list))continue;
   return data;
  }catch(error){if(signal?.aborted)throw error;}
 }
 throw Error('CloudMoon public server list is unavailable');
}

