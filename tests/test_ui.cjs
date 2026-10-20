// Logic/event checks with a minimal DOM model; this is not browser visual QA.
const fs=require('fs'),vm=require('vm'),assert=require('assert');
const path=require('path');
let code=fs.readFileSync(path.join(__dirname,'../qa_sentinel/static/app.js'),'utf8');
code=code.slice(0,code.lastIndexOf('// Start the application.'));
const nodes=new Map(),documentEvents=new Map();
function classes(){const set=new Set();return{add:x=>set.add(x),remove:x=>set.delete(x),contains:x=>set.has(x),toggle(x){set.has(x)?set.delete(x):set.add(x);}};}
function node(selector){
 if(nodes.has(selector))return nodes.get(selector);
 const events=new Map();
 const value={id:selector.startsWith('#')?selector.slice(1):'',dataset:{},classList:classes(),innerHTML:'',value:selector==='#source-kind'?'dcinside':'',open:false,
 addEventListener(type,listener){if(!events.has(type))events.set(type,[]);events.get(type).push(listener);},
 querySelector:node,querySelectorAll(){return[];},
 showModal(){this.open=true;this.nativeModalCalls=(this.nativeModalCalls||0)+1;},
 close(){this.open=false;for(const listener of events.get('close')||[])listener({target:this});},
 getBoundingClientRect(){return{left:100,right:500,top:100,bottom:600};},
 dispatch(type,event){for(const listener of events.get(type)||[])listener(event);}
 };nodes.set(selector,value);return value;
}
const document={documentElement:{classList:classes()},querySelector:node,querySelectorAll(){return[];},addEventListener(type,listener){if(!documentEvents.has(type))documentEvents.set(type,[]);documentEvents.get(type).push(listener);}};
const context={assert,console,SentinelChart:require('../qa_sentinel/static/chart.js'),document,localStorage:{getItem(){return'demo';},setItem(){}},window:{addEventListener(){},scrollTo(){}},location:{hash:'#feedback'},setTimeout,clearTimeout};
vm.createContext(context);
vm.runInContext(code,context);
(async()=>{
 await vm.runInContext(`(async()=>{
 assert.strictEqual(range,0);assert.strictEqual(historyFilter,'all');
 state={started_at:'2026-10-17T11:00:00Z',scope:{start_at:'2026-10-17T11:00:00Z'},at:'2026-10-17T12:00:00Z',offline_test:false,settings:{lookback_minutes:0,warning:3,danger:5,running:true},sources:[{id:'dc',name:'갤러리',kind:'dcinside'},{id:'arca',name:'아카라이브',kind:'arcalive'}],keywords:[{name:'결제',terms:['결제'],enabled:true},{name:'팅김',terms:['팅김'],enabled:true}],events:[],detection:{level:'normal',count:0,keywords:[]},collecting:false,posts:[
 {id:1,title:'결제 환불 문의',body:'내용',source_id:'dc',source_name:'갤러리',published_at:'2026-10-17T11:59:00Z',keywords:['결제']},
 {id:2,title:'팅김 문의',body:'내용',source_id:'arca',source_name:'아카라이브',published_at:'2026-10-17T11:59:00Z',keywords:['팅김']},
 {id:3,title:'결제와 팅김 문의',body:'내용',source_id:'dc',source_name:'갤러리',published_at:'2026-10-17T11:59:00Z',keywords:['결제','팅김']} ]};
 keyword='결제';channel='dc';search='환불';filter='all';assert.deepStrictEqual(filteredPosts().map(p=>p.id),[1]);
 const picker=feedbackCard();assert(picker.includes('id="keyword-filter"'));assert(picker.includes('value="결제" selected'));assert(picker.includes('모든 키워드'));
 channel='';search='';keyword='팅김';assert.deepStrictEqual(filteredPosts().map(p=>p.id),[2,3]);
 keyword='';assert.strictEqual(filteredPosts().length,3);
 search='내용';assert.strictEqual(filteredPosts().length,0);search='';
 mode='live';assert(heading('테스트','').includes('15분마다'));mode='demo';
 mode='live';state.collection={started_at:'2026-10-17T11:55:00Z',finished_at:'2026-10-17T11:56:00Z',outcome:'completed'};
 assert(heading('테스트','').includes('최근 수집 종료'));assert(!heading('테스트','').includes('마지막 화면 갱신'));
 state.collecting=true;assert(collectionTimeText().includes('수집 진행 중'));state.collecting=false;mode='demo';
 drawChart();assert(document.querySelector('#chart').innerHTML.includes('chart-cumulative-line'));assert(document.querySelector('#chart').innerHTML.includes('data-cumulative-total="3"'));assert(dashboardView().includes('이번 실행 전체'));
 assert.strictEqual(currentPosts().length,3);
 state.posts[0].feedback_status='non_issue';state.posts[1].feedback_status='in_progress';state.posts[2].feedback_status='resolved';
 assert.strictEqual(currentPosts().length,0);assert.strictEqual(feedbackPosts().length,3);
 for(const status of ['non_issue','in_progress','resolved']){reviewFilter=status;assert.strictEqual(filteredPosts().length,1);}
 reviewFilter='all';state.posts[0].feedback_status='active';assert.strictEqual(currentPosts().length,1);
 state.posts[0].in_scope=false;assert.strictEqual(currentPosts().length,0);assert.strictEqual(feedbackPosts().length,3);
 historyFilter='session';assert.strictEqual(feedbackPosts().length,2);
 historyFilter='all';assert.strictEqual(feedbackPosts().length,3);assert(postDetectionBadge(state.posts[0]).includes('범위 밖'));
 historyFilter='all';for(const p of state.posts){p.feedback_status='active';p.in_scope=true;}
 assert(feedbackCard().includes('id="review-filter"'));assert(feedbackCard().includes('id="history-filter"'));
 assert(settingsView().includes('name="lookback_minutes"'));assert(settingsView().includes('max="720"'));assert(!settingsView().includes('name="window_minutes"'));
 assert(detectionSlide({name:'결제',count:5,level:'danger'},0,1).includes('이번 실행 누적'));
 sourceModal();assert.strictEqual(modal.nativeModalCalls,1);assert.strictEqual(modal.dataset.lockBackdrop,'true');assert(document.documentElement.classList.contains('modal-open'));
 assert(modal.innerHTML.includes('value="arcalive"'));assert(modal.innerHTML.includes('아카라이브'));
 assert(!modal.innerHTML.includes('name="body_selector"'));assert(modal.innerHTML.includes('목록 행 내부 작성 시각'));
 })()`,context);
 const modal=node('#modal');
 for(const type of ['click','pointerdown','touchmove','wheel']){
  let prevented=false,stopped=false;
  modal.dispatch(type,{target:modal,clientX:10,clientY:10,touches:type==='touchmove'?[{clientX:10,clientY:10}]:undefined,preventDefault(){prevented=true;},stopPropagation(){stopped=true;}});
  assert(prevented&&stopped,type+' outside channel modal must be blocked');assert(modal.open);
 }
 const inner=node('#source-url');let touched=false;
 modal.dispatch('pointerdown',{target:inner,clientX:150,clientY:150,preventDefault(){touched=true;},stopPropagation(){}});assert(!touched);
 for(const handler of documentEvents.get('click'))await handler({target:{closest(){return{disabled:false,dataset:{action:'close-modal'}};}}});
 assert(!modal.open);assert(!document.documentElement.classList.contains('modal-open'));
 vm.runInContext("openModal('다른 팝업','<p>내용</p>');",context);
 modal.dispatch('click',{target:modal,clientX:0,clientY:0});assert(!modal.open);
 node('#source-kind').value='arcalive';vm.runInContext('updateSourceKind();',context);assert(node('#source-url').placeholder.includes('arca.live'));assert(node('#html-selectors').hidden);
 node('#keyword-filter').value='팅김';
 for(const handler of documentEvents.get('change'))await handler({target:node('#keyword-filter')});
 assert.strictEqual(vm.runInContext('keyword',context),'팅김');
 assert.strictEqual(vm.runInContext('tablePage',context),1);
 vm.runInContext("state.keywords.push({name:'<script>',terms:['x'],enabled:true});keyword='<script>';",context);
 const html=vm.runInContext('feedbackCard()',context);assert(html.includes('&lt;script&gt;'));assert(!html.includes('<script>'));
 await vm.runInContext("api=async()=>({...state,keywords:[]});load(false);",context);assert.strictEqual(vm.runInContext('keyword',context),'');
 await vm.runInContext(`(async()=>{
 const savedLoad=load,savedState=state;let loads=0,reconnects=0;
 load=async()=>{loads++;};refreshController={reconnect:async()=>{reconnects++;}};
 state=null;loading=false;await recoverConnection();assert.strictEqual(loads,1);assert.strictEqual(reconnects,0);
 loading=true;await recoverConnection();assert.strictEqual(loads,1);
 loading=false;state=savedState;await recoverConnection();assert.strictEqual(reconnects,1);assert.strictEqual(loads,1);
 load=savedLoad;refreshController=null;
 })()`,context);
 console.log('PASS: native channel modal, title-only search, 15-minute collection label, three listing selectors, keyword dropdown + combined filters, Arca picker and deleted-keyword cleanup.');
})().catch(error=>{console.error(error);process.exitCode=1;});
