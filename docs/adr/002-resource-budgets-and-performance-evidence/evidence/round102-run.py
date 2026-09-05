import pathlib,json,subprocess,hashlib,datetime,platform,os
BASE=pathlib.Path(__file__).resolve().parent
BINS={'control':pathlib.Path('/tmp/event-stream-round100-target/release/examples/performance_baseline'),'bounded':pathlib.Path('/tmp/event-stream-round102-target/release/examples/performance_baseline')}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
identities={k:sha(v) for k,v in BINS.items()}
meta=dict(binaries={k:str(v) for k,v in BINS.items()},binary_sha256=identities,archive_sha256={k:sha(BASE/f'round{n}-source.tar') for k,n in [('control',100),('bounded',102)]},host=platform.platform(),load=os.getloadavg(),power=subprocess.check_output(['pmset','-g','batt'],text=True),features='all-features',started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),profile=dict(events=1000,interval_us=5000,producers=4,streams=4,payload_bytes=128),scope='three interleaved diagnostic samples per store/generator; no release gate; builds and tests paused')
(BASE/'round102-provenance.json').write_text(json.dumps(meta,indent=2)+'\n')
with (BASE/'round102-sustained.jsonl').open('x') as out:
 for repeat in range(3):
  for store in ['memory','sqlite']:
   for variant in (['control','bounded'] if repeat%2==0 else ['bounded','control']):
    cmd=[str(BINS[variant]),'--store',store,'--scenario','sustained','--streams','4','--producers','4','--events','1000','--offer-interval-us','5000','--payload-bytes','128','--repetitions','1']
    run=subprocess.run(cmd,text=True,capture_output=True,timeout=90)
    assert run.returncode==0,run.stderr+run.stdout
    data={r['kind']:r for r in map(json.loads,run.stdout.splitlines())}
    s=data['sample'];r=data['resources'];g=s.get('generator_rejected',0)
    assert s['offered']==4000==s['successful_receipts']+s['caller_rejected']+s['caller_failed']+g
    assert s['caller_failed']==s['runtime_failed']==0
    assert s['append_tasks']==4000-g
    assert s['successful_receipt_samples']==s['successful_receipts']
    assert s['all_outcome_latency_samples']==s['schedule_lateness_samples']==4000-g
    assert s['peak_queued_appends']<=data['config']['max_queued_appends']
    assert r['shutdown_closed'] and r['shutdown_unresolved']==0
    if variant=='bounded':assert s['peak_generator_tasks']<=256
    out.write(json.dumps(dict(repeat=repeat,store=store,variant=variant,command=cmd,stdout=run.stdout,stderr=run.stderr))+'\n');out.flush()
    print(repeat,store,variant,s['successful_receipts'],g,s['max_rss_bytes'],flush=True)
assert identities=={k:sha(v) for k,v in BINS.items()}
