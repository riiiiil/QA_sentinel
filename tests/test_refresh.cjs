// Deterministic stream/timer tests: idle, active, editing, reconnect and races.
const assert=require('node:assert/strict');
const CollectionRefresh=require('../qa_sentinel/static/refresh.js');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const snapshot=(revision=0,collecting=false,server='a',mode='live')=>({mode,server_id:server,completed_revision:revision,collecting,collection:{total:2,completed:collecting?1:2}});
function harness(){
 const h={timers:new Map(),streams:[],loads:0,polls:0,progress:[],connections:[],allowed:true,latest:snapshot(),nextTimer:0};
 h.controller=new CollectionRefresh({
  openStream(){const listeners={};const stream={closed:false,addEventListener(name,fn){listeners[name]=fn;},close(){this.closed=true;},send(data){listeners.collection({data:JSON.stringify(data)});}};h.streams.push(stream);return stream;},
  setTimer(fn,ms){assert.equal(ms,3000);const id=++h.nextTimer;h.timers.set(id,fn);return id;},clearTimer:id=>h.timers.delete(id),
  canRefresh:()=>h.allowed,canPoll:()=>true,
  reload:async()=>{h.loads++;return h.latest;},readProgress:async()=>{h.polls++;return h.latest;},
  onProgress:data=>h.progress.push(data),onConnection:ok=>h.connections.push(ok),
 });
 h.start=()=>h.controller.baseline(h.latest);
 h.send=data=>{h.latest={...data,mode:'live'};h.streams.at(-1).send(data);};
 h.fire=async()=>{const [id,fn]=h.timers.entries().next().value;h.timers.delete(id);await fn();};
 return h;
}
(async()=>{
 let h=harness();h.start();
 for(let i=0;i<10;i++)h.send(snapshot());
 await tick();assert.equal(h.loads,0);assert.equal(h.polls,0);assert.equal(h.timers.size,0);assert.equal(h.streams.length,1);
 h.send(snapshot(0,true));h.send(snapshot(0,true));assert.equal(h.timers.size,1);
 await h.fire();assert.equal(h.polls,1);assert.equal(h.loads,0);assert.equal(h.timers.size,1);
 h.send(snapshot(1));await tick();assert.equal(h.loads,1);assert.equal(h.timers.size,0);
 h.send(snapshot(1));await tick();assert.equal(h.loads,1);h.controller.close();

 h=harness();h.start();h.allowed=false;h.send(snapshot(1));h.send(snapshot(2));await tick();assert.equal(h.loads,0);
 h.allowed=true;await h.controller.resume();assert.equal(h.loads,1);assert.equal(h.controller.applied.completed_revision,2);h.controller.close();

 h=harness();h.start();h.send(snapshot(0,true));assert.equal(h.timers.size,1);h.streams[0].onerror();assert.equal(h.timers.size,0);
 h.send(snapshot(1));await tick();assert.equal(h.loads,1);assert(h.connections.includes(false));assert(h.connections.includes(true));
 h.send(snapshot(0,false,'b'));await tick();assert.equal(h.loads,2);assert.equal(h.controller.applied.server_id,'b');h.controller.close();

 h=harness();h.start();h.send(snapshot(0,true));let release;
 h.controller.readProgress=()=>{h.polls++;return new Promise(resolve=>release=resolve);};
 const poll=h.fire();assert.equal(h.timers.size,0);h.send(snapshot(0,true));assert.equal(h.timers.size,0);
 h.send(snapshot(1));await tick();assert.equal(h.loads,1);release(snapshot(0,true));await poll;
 assert.equal(h.controller.active,false);assert.equal(h.timers.size,0);h.controller.close();

 h=harness();h.start();h.send(snapshot(0,true));let finish;
 h.controller.readProgress=()=>new Promise(resolve=>finish=resolve);
 const pending=h.fire();const before=h.progress.length;
 h.controller.baseline(snapshot(0,false,'a','demo'));finish(snapshot(0,true));await pending;
 assert.equal(h.progress.length,before);assert.equal(h.timers.size,0);assert.equal(h.streams[0].closed,true);h.controller.close();

 h=harness();h.start();let completeLoad;
 h.controller.reload=()=>{h.loads++;if(h.loads===1)return new Promise(resolve=>completeLoad=resolve);return Promise.resolve(h.latest);};
 h.send(snapshot(1));h.send(snapshot(2));h.send(snapshot(2));completeLoad(snapshot(1));await tick();await tick();
 assert.equal(h.loads,2);assert.equal(h.controller.applied.completed_revision,2);assert.equal(h.controller.pending,null);h.controller.close();

 h=harness();h.start();h.controller.reload=async()=>{h.loads++;return null;};h.send(snapshot(1));await tick();assert.equal(h.loads,1);assert(h.controller.pending);
 h.controller.reload=async()=>{h.loads++;return h.latest;};await h.controller.resume();assert.equal(h.loads,2);assert.equal(h.controller.pending,null);h.controller.close();

 // Recovery reads only local progress; no automatic collection restart.
 h=harness();h.latest=snapshot(0,true);h.start();h.controller.disconnect();
 assert.equal(h.timers.size,0);assert(h.streams[0].closed);
 h.latest=snapshot(1,false);await h.controller.reconnect();await tick();
 assert.equal(h.polls,1);assert.equal(h.loads,1);assert.equal(h.controller.active,false);assert.equal(h.timers.size,0);h.controller.close();
 h=harness();h.start();let resolveCheck;
 h.controller.readProgress=()=>{h.polls++;return new Promise(resolve=>resolveCheck=resolve);};
 const one=h.controller.reconcile(),two=h.controller.reconcile();assert.strictEqual(one,two);assert.equal(h.polls,1);
 resolveCheck(snapshot(0,true));await one;assert.equal(h.timers.size,1);h.controller.close();
 h=harness();h.start();h.controller.readProgress=async()=>{h.polls++;throw Error('local connection lost');};
 assert.equal(await h.controller.reconcile(),null);assert.equal(h.timers.size,0);assert(h.connections.includes(false));
 h.controller.readProgress=async()=>{h.polls++;return snapshot(0,false);};await h.controller.reconcile();assert(h.connections.at(-1));h.controller.close();
 h=harness();h.start();let resolveStale;
 h.controller.readProgress=()=>new Promise(resolve=>resolveStale=resolve);
 const stale=h.controller.reconcile(),progressBefore=h.progress.length;
 h.controller.baseline(snapshot(0,false,'a','demo'));resolveStale(snapshot(0,true));await stale;
 assert.equal(h.progress.length,progressBefore);assert.equal(h.timers.size,0);h.controller.close();
 console.log('PASS: no idle polling; collection-only 3s progress; completion refresh; deferred editing; reconnect; mode and request races.');
})().catch(error=>{console.error(error);process.exitCode=1;});
