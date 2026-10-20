'use strict';
const assert=require('node:assert/strict');
const {build}=require('../qa_sentinel/static/chart.js');
const start=Date.parse('2026-10-20T00:00:00Z'),minute=60000;
const sources=[{id:'one',enabled:true},{id:'off',enabled:false}];
const post=(minutes,extra={})=>({published_at:new Date(start+minutes*minute).toISOString(),source_id:'one',keywords:['결제','팅김'],feedback_status:'active',...extra});
const options={sources,startAt:new Date(start).toISOString(),at:new Date(start+45*minute).toISOString(),posts:[post(1),post(14),post(15),post(20),post(29)]};
let data=build(options);
assert.deepEqual(data.bins.map(b=>b.newCount),[2,3,0]);
assert.deepEqual(data.bins.map(b=>b.cumulative),[2,5,5]);
assert.equal(data.total,5,'A post matching two keywords must count once');
assert.equal(data.step,15*minute);
// Left boundaries belong to the next bucket; the snapshot end is inclusive once.
data=build({...options,posts:[post(0),post(15),post(30),post(45),post(-1),post(46)]});
assert.deepEqual(data.bins.map(b=>b.newCount),[1,1,2]);assert.equal(data.total,4);
// A narrower viewport preserves accumulation from before the viewport.
data=build({...options,at:new Date(start+120*minute).toISOString(),posts:[post(1),post(15),post(65),post(75),post(120)],rangeHours:1});
assert.equal(data.displayStart,start+60*minute);assert.equal(data.baseline,2);
assert.deepEqual(data.bins.map(b=>b.newCount),[1,1,0,1]);
assert.deepEqual(data.bins.map(b=>b.cumulative),[3,4,4,5]);assert.equal(data.visibleTotal,3);assert.equal(data.total,5);
const excluded=['non_issue','in_progress','resolved'].map(status=>post(10,{feedback_status:status}));
data=build({...options,posts:[post(10),...excluded,post(10,{source_id:'off'}),post(10,{source_id:'deleted'}),post(10,{in_scope:false}),post(10,{keywords:[]})]});
assert.equal(data.total,1);assert.equal(data.bins.at(-1).cumulative,1);
excluded[0].feedback_status='active';
assert.equal(build({...options,posts:[post(10),...excluded]}).total,2,'Reactivation recalculates history');
// The start may include a lookback before program launch; never trim it to 24h.
data=build({...options,startAt:new Date(start-12*60*minute).toISOString(),posts:[post(-720),post(-1),post(1)]});
assert.equal(data.displayStart,start-720*minute);assert.equal(data.total,3);
// Collection snapshots may have no elapsed time or no posts.
data=build({...options,at:options.startAt,posts:[]});assert.equal(data.bins.length,1);assert.equal(data.total,0);
data=build({...options,at:options.startAt,posts:[post(0)]});assert.equal(data.bins[0].newCount,1);
// Month-long sessions must remain bounded and retain every eligible post.
const longPosts=Array.from({length:1000},(_,i)=>post(i*43));
data=build({...options,at:new Date(start+30*86400000).toISOString(),posts:longPosts});
assert(data.step>15*minute);assert(data.bins.length<=96);assert.equal(data.total,1000);
assert.equal(data.bins.reduce((sum,b)=>sum+b.newCount,0),1000);assert.equal(data.bins.at(-1).cumulative,1000);
assert(build({...options,startAt:'invalid'}).error);
assert(build({...options,at:new Date(start-1).toISOString()}).error);
console.log('PASS: 신규 2/3/0 → 누적 2/5/5; 시간 경계; 범위 이전 누적 유지; 비활성 제외·복원; 과거 포함; 빈/0초 집계; 장시간 96구간 이내.');
