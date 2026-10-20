'use strict';
// Pure snapshot calculation. Drawing and range changes never request new data.
(function(root){
 const QUARTER_HOUR=15*60*1000,MAX_BINS=96;
 function build({posts,sources,startAt,at,rangeHours=0}){
  const start=Date.parse(startAt),end=Date.parse(at);
  if(!Number.isFinite(start)||!Number.isFinite(end)||start>end)return {error:'집계 시작 시각과 기준 시각을 확인해 주세요.'};
  const displayStart=rangeHours>0?Math.max(start,end-rangeHours*3600000):start;
  let step=QUARTER_HOUR;
  const binCount=()=>Math.max(1,Math.ceil((end-start)/step)-Math.floor((displayStart-start)/step));
  while(binCount()>MAX_BINS)step*=2;
  const first=Math.floor((displayStart-start)/step),count=binCount();
  const bins=Array.from({length:count},(_,i)=>({start:Math.max(displayStart,start+(first+i)*step),end:Math.min(end,start+(first+i+1)*step),newCount:0,cumulative:0}));
  const active=new Set(sources.filter(s=>s.enabled!==false).map(s=>s.id));
  let baseline=0,total=0;
  for(const post of posts){
   const t=Date.parse(post.published_at);
   if(!Number.isFinite(t)||t<start||t>end||post.in_scope===false||!post.keywords?.length||(post.feedback_status||'active')!=='active'||!active.has(post.source_id))continue;
   total++;
   if(t<displayStart){baseline++;continue;}
   const index=Math.min(count-1,Math.floor((t-start)/step)-first);
   bins[index].newCount++;
  }
  let cumulative=baseline;
  for(const bin of bins){cumulative+=bin.newCount;bin.cumulative=cumulative;}
  return {start,end,displayStart,step,bins,baseline,total,visibleTotal:total-baseline};
 }
 const api={build};
 if(typeof module==='object'&&module.exports)module.exports=api;
 else root.SentinelChart=api;
})(typeof globalThis!=='undefined'?globalThis:this);
