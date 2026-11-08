// Forward only this project's save operations; Supabase still verifies the user's
// bearer token and enforces Storage RLS. Never use a service-role key here.
const CORS={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,DELETE,OPTIONS','Access-Control-Allow-Headers':'Content-Type,X-Relay-Key,Authorization,apikey,x-upsert,Cache-Control','Access-Control-Max-Age':'600','Cache-Control':'no-store'};
const MAX=5*1024*1024,BUCKET='math-practice-saves';
let active=0;
const json=(status,message)=>Response.json({error:message},{status,headers:CORS});
export async function limitedBytes(body,limit=MAX){
 const chunks=[];let size=0;const reader=body?.getReader();if(!reader)return new Uint8Array();
 try{for(;;){const {value,done}=await reader.read();if(done)break;size+=value.length;if(size>limit){await reader.cancel();throw Error('Payload too large');}chunks.push(value);}}
 finally{reader.releaseLock();}
 const bytes=new Uint8Array(size);let offset=0;for(const c of chunks){bytes.set(c,offset);offset+=c.length;}return bytes;
}
export function allowedSavePath(path,method){
 if(method==='POST'&&(path==='/auth/v1/signup'||path==='/auth/v1/token?grant_type=refresh_token'))return true;
 if(method==='DELETE'&&path==='/storage/v1/object/'+BUCKET)return true;
 const prefix=method==='GET'?'/storage/v1/object/authenticated/'+BUCKET+'/':method==='POST'?'/storage/v1/object/'+BUCKET+'/':null;
 if(!prefix||!path.startsWith(prefix))return false;
 const key=path.slice(prefix.length),uuid='[a-fA-F0-9-]{36}';
 return new RegExp('^'+uuid+'/'+uuid+'/(?:head\\.json|snapshots/'+uuid+'/[0-9]+\\.bin)$').test(key);
}
export async function handleSaveProxy(request,config,upstreamFetch=fetch){
 if(request.method==='OPTIONS')return new Response(null,{status:204,headers:CORS});
 if(!config.RELAY_KEY||request.headers.get('x-relay-key')!==config.RELAY_KEY)return json(401,'Invalid relay key');
 const url=new URL(request.url),path=url.pathname.slice('/save-proxy'.length)+url.search;
 if(!url.pathname.startsWith('/save-proxy/')||!allowedSavePath(path,request.method))return json(403,'Save operation is not allowed');
 let project;try{project=new URL(config.SUPABASE_URL);if(project.protocol!=='https:'||!project.hostname.endsWith('.supabase.co')||project.port||project.username||project.password||project.pathname!=='/'||project.search||project.hash)throw Error();}catch{return json(503,'Configure the Supabase project HTTPS origin');}
 const key=config.SUPABASE_PUBLISHABLE_KEY;
 if(!key||key.startsWith('sb_secret_'))return json(503,'Configure a Supabase publishable key');
 if(request.headers.get('apikey')!==key)return json(403,'Supabase project key mismatch');
 if(path.startsWith('/storage/')&&!/^Bearer [A-Za-z0-9._-]+$/.test(request.headers.get('authorization')||''))return json(401,'User authorization required');
 if(active>=4)return json(503,'Save relay busy');
 active++;
 try{
  if(Number(request.headers.get('content-length'))>MAX)return json(413,'Save chunk too large');
  const body=['POST','DELETE'].includes(request.method)?await limitedBytes(request.body,path.startsWith('/auth/')?65536:MAX):undefined;
  if(request.method==='DELETE'){
   let data;try{data=JSON.parse(new TextDecoder().decode(body));}catch{return json(400,'Invalid delete request');}
   if(!Array.isArray(data.prefixes)||data.prefixes.length>2048||data.prefixes.some(p=>typeof p!=='string'||!allowedSavePath('/storage/v1/object/'+BUCKET+'/'+p,'POST')||p.endsWith('/head.json')))return json(403,'Only snapshot parts can be removed');
  }
  const headers=new Headers({apikey:key});
  for(const name of ['authorization','content-type','x-upsert']){const value=request.headers.get(name);if(value)headers.set(name,value);}
  const response=await upstreamFetch(new URL(path,project),{method:request.method,headers,body,redirect:'manual',signal:AbortSignal.any([request.signal,AbortSignal.timeout(20000)])});
  if(response.status>=300&&response.status<400){await response.body?.cancel();return json(502,'Save redirect refused');}
  const bytes=await limitedBytes(response.body);
  return new Response([204,205,304].includes(response.status)?null:bytes,{status:response.status,headers:{...CORS,'Content-Type':response.headers.get('content-type')||'application/octet-stream','X-Save-Proxy':'1'}});
 }catch(error){return json(error.message==='Payload too large'?413:502,'Save relay request failed; local saves are retained');}
 finally{active--;}
}

export const SAVE_DEFAULTS={"SUPABASE_URL": "https://bdmfkgfpqngvprvchhju.supabase.co", "SUPABASE_PUBLISHABLE_KEY": "sb_publishable_GjlumFqHhbC0snMXMnMOcw_EAPx1qLA"};
