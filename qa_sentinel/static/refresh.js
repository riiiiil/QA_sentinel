/* Collection-driven refresh. Idle tabs keep one notification stream, no polling. */
(function(root){
 'use strict';
 class CollectionRefresh {
  constructor(options){
   Object.assign(this,options);
   this.setTimer=options.setTimer||((fn,ms)=>setTimeout(fn,ms));this.clearTimer=options.clearTimer||(id=>clearTimeout(id));
   this.mode=null;this.generation=0;this.source=null;this.timer=null;this.polling=false;this.active=false;this.refreshing=false;this.pending=null;
   this.applied={server_id:null,completed_revision:0};
   this.checking=null;
  }
  baseline(snapshot){
   this.applied={server_id:snapshot.server_id,completed_revision:snapshot.completed_revision||0};
   if(this.pending&&this.pending.server_id===snapshot.server_id&&this.pending.completed_revision<=this.applied.completed_revision)this.pending=null;
   this.watch(snapshot.mode);
   this.active=this.mode==='live'&&Boolean(snapshot.collecting);
   this.arm();
  }
  watch(mode,force=false){
   if(mode===this.mode&&!force)return;
   this.generation++;this.source?.close();this.source=null;this.stopTimer();this.active=false;this.pending=null;this.mode=mode;
   if(mode!=='live')return;
   const generation=this.generation;
   const stream=this.openStream('/api/collection/events');this.source=stream;
   stream.addEventListener('collection',event=>{
    if(generation!==this.generation)return;
    try{const snapshot=JSON.parse(event.data);this.onConnection(true);this.receive(snapshot);}catch(error){this.onConnection(false);}
   });
   stream.onerror=()=>{if(generation===this.generation){this.active=false;this.stopTimer();this.onConnection(false);}};
  }
  receive(snapshot){
   if(this.mode!=='live')return;
   if(snapshot.server_id===this.applied.server_id&&(snapshot.completed_revision||0)<this.applied.completed_revision)return;
   this.active=Boolean(snapshot.collecting);this.onProgress(snapshot);
   if(snapshot.server_id!==this.applied.server_id||(snapshot.completed_revision||0)>this.applied.completed_revision){
    const key=snapshot.server_id+':'+(snapshot.completed_revision||0);
    if(this.pending?.key!==key)this.pending={...snapshot,key};
    void this.flush();
   }
   this.arm();
  }
  stopTimer(){if(this.timer!==null)this.clearTimer(this.timer);this.timer=null;}
  arm(){
   if(!this.active||this.mode!=='live'||this.canPoll?.()===false){this.stopTimer();return;}
   if(this.timer!==null||this.polling)return;
   const generation=this.generation;
   this.timer=this.setTimer(async()=>{
    this.timer=null;this.polling=true;
    try{const snapshot=await this.readProgress();if(generation===this.generation)this.receive(snapshot);}
    catch(error){if(generation===this.generation){this.active=false;this.onConnection(false);}}
    finally{this.polling=false;this.arm();}
   },3000);
  }
  async flush(){
   if(!this.pending||this.refreshing)return;
   if(!this.canRefresh()){this.onDeferred?.();return;}
   const pending=this.pending,generation=this.generation;this.refreshing=true;
   let result;
   try{
    result=await this.reload();
    if(result&&generation===this.generation){this.baseline(result);if(this.pending===pending)this.pending=null;}
   }finally{
    this.refreshing=false;
    // Coalesce notifications that arrived during the state request.
    if(result&&this.pending&&this.canRefresh())void this.flush();
   }
  }
  resume(){this.arm();return this.flush();}
  reconcile(){
   if(this.mode!=='live'||this.canPoll?.()===false)return Promise.resolve(null);
   if(!this.source)this.watch('live',true);
   if(this.checking?.generation===this.generation)return this.checking.promise;
   const check={generation:this.generation};this.checking=check;
   check.promise=(async()=>{
    try{
     const snapshot=await this.readProgress();
     if(check.generation!==this.generation)return null;
     this.onConnection(true);this.receive(snapshot);await this.flush();return snapshot;
    }catch(error){
     if(check.generation===this.generation){this.active=false;this.stopTimer();this.onConnection(false);}
     return null;
    }finally{if(this.checking===check)this.checking=null;}
   })();
   return check.promise;
  }
  disconnect(){this.generation++;this.source?.close();this.source=null;this.stopTimer();this.active=false;this.checking=null;this.onConnection(false);}
  reconnect(){if(this.mode!=='live')return Promise.resolve(null);this.watch('live',true);return this.reconcile();}
  close(){this.generation++;this.source?.close();this.source=null;this.stopTimer();this.mode=null;this.active=false;this.pending=null;this.checking=null;}
 }
 if(typeof module!=='undefined'&&module.exports)module.exports=CollectionRefresh;
 else root.CollectionRefresh=CollectionRefresh;
})(globalThis);
