"""First population correctness/resource matrix, not repeated qualification."""
import pathlib,json,subprocess,hashlib,datetime,platform,os
BASE=pathlib.Path(__file__).resolve().parent
binary=pathlib.Path('/tmp/event-stream-round104-target/release/examples/performance_baseline')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
identity=sha(binary)
meta=dict(binary=str(binary),binary_sha256=identity,archive_sha256=sha(BASE/'round104-source.tar'),host=platform.platform(),load=os.getloadavg(),power=subprocess.check_output(['pmset','-g','batt'],text=True),started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),populations=[1000,10000,100000],cycles=2,producers=4,interval_us=5000,payload_bytes=128,features='all-features',scope='one fresh process per store/population; two cycles;800 aggregate offered events/s; sparse population activity; not repeated qualification')
(BASE/'round104-provenance.json').write_text(json.dumps(meta,indent=2)+'\n')
with (BASE/'round104-population.jsonl').open('x') as out:
 for population in [1000,10000,100000]:
  for store in ['memory','sqlite']:
   cmd=[str(binary),'--store',store,'--scenario','population_sustained','--streams',str(population),'--producers','4','--events',str(population//2),'--offer-interval-us','5000','--payload-bytes','128','--repetitions','1']
   print('START',population,store,flush=True)
   run=subprocess.run(cmd,text=True,capture_output=True,timeout=900)
   assert run.returncode==0,run.stderr+run.stdout
   records=list(map(json.loads,run.stdout.splitlines()));data={r['kind']:r for r in records};s=data['sample'];r=data['resources'];v=data['population_verification'];cycles=[r for r in records if r['kind']=='population_cycle']
   assert s['offered']==2*population==s['successful_receipts']+s['caller_rejected']+s['caller_failed']+s['generator_rejected']
   assert s['caller_failed']==s['runtime_failed']==0
   assert s['peak_generator_tasks']<=256 and s['peak_queued_appends']<=data['config']['max_queued_appends']
   assert r['shutdown_closed'] and r['shutdown_unresolved']==0
   assert len(cycles)==2 and sum(c['offered'] for c in cycles)==2*population
   for c in cycles:assert c['offered']==c['accepted']+c['runtime_rejected']+c['generator_rejected']+c['failed']
   assert v['accepted_receipts']==s['successful_receipts']
   assert v['successful_stream_coverage']+v['uncovered_streams']==population
   out.write(json.dumps(dict(population=population,store=store,command=cmd,stdout=run.stdout,stderr=run.stderr))+'\n');out.flush()
   print('DONE',population,store,s['successful_receipts'],v['successful_stream_coverage'],s['max_rss_bytes'],flush=True)
assert sha(binary)==identity
