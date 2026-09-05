import pathlib,json,subprocess,hashlib,datetime,platform,os
BASE=pathlib.Path(__file__).resolve().parent
binary=pathlib.Path('/tmp/event-stream-round102-target/release/examples/performance_baseline')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
identity=sha(binary);assert identity==json.loads((BASE/'round102-provenance.json').read_text())['binary_sha256']['bounded']
meta=dict(binary=str(binary),binary_sha256=identity,archive_sha256=sha(BASE/'round102-source.tar'),host=platform.platform(),load=os.getloadavg(),power=subprocess.check_output(['pmset','-g','batt'],text=True),started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),profile=dict(events=12000,interval_us=5000,producers=4,streams=4,payload_bytes=128),scope='Six one-minute fresh processes; bounded generator; no release capacity claim')
(BASE/'round103-provenance.json').write_text(json.dumps(meta,indent=2)+'\n')
with (BASE/'round103-sustained.jsonl').open('x') as out:
 for repeat in range(3):
  for store in ['memory','sqlite']:
   cmd=[str(binary),'--store',store,'--scenario','sustained','--streams','4','--producers','4','--events','12000','--offer-interval-us','5000','--payload-bytes','128','--repetitions','1']
   run=subprocess.run(cmd,text=True,capture_output=True,timeout=120)
   assert run.returncode==0,run.stderr+run.stdout
   data={r['kind']:r for r in map(json.loads,run.stdout.splitlines())};s=data['sample'];r=data['resources']
   assert s['offered']==48000==s['successful_receipts']+s['caller_rejected']+s['caller_failed']+s['generator_rejected']
   assert s['caller_failed']==s['runtime_failed']==0
   assert s['append_tasks']==48000-s['generator_rejected']
   assert s['successful_receipt_samples']==s['successful_receipts']
   assert s['all_outcome_latency_samples']==s['schedule_lateness_samples']==s['append_tasks']
   assert s['peak_generator_tasks']<=256
   assert s['peak_queued_appends']<=data['config']['max_queued_appends']
   assert r['shutdown_closed'] and r['shutdown_unresolved']==0
   out.write(json.dumps(dict(repeat=repeat,store=store,command=cmd,stdout=run.stdout,stderr=run.stderr))+'\n');out.flush()
   print(repeat,store,s['successful_receipts'],s['generator_rejected'],s['peak_generator_tasks'],s['successful_receipt_p99_ns'],flush=True)
assert sha(binary)==identity
