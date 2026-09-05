import pathlib,json,subprocess,hashlib,datetime,platform,os
r=pathlib.Path.cwd();ev=r/'docs/adr/002-resource-budgets-and-performance-evidence/evidence';bins={'baseline':pathlib.Path('/tmp/event-stream-round93-snapshot-target/release/examples/performance_baseline'),'compact':pathlib.Path('/tmp/event-stream-round95-burst-target/release/examples/performance_baseline')};sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();identities={k:sha(v) for k,v in bins.items()};meta={'binaries':{k:str(v) for k,v in bins.items()},'binary_sha256':identities,'archives':{'baseline':'docs/adr/005-snapshots-and-consumer-recovery/evidence/round93-source.tar','compact':str((ev/'round95-source.tar').relative_to(r))},'features':['sqlite','snapshots','test-support'],'host':platform.platform(),'load':os.getloadavg(),'power':subprocess.check_output(['pmset','-g','batt'],text=True),'source_difference':['examples/performance_baseline.rs'],'scope':'Paired100k one-shot try_append tasks; library source identical; caller/harness optimization. No project builds/tests concurrent.'};meta['archive_sha256']={k:sha(r/v) for k,v in meta['archives'].items()};(ev/'round95-provenance.json').write_text(json.dumps(meta,indent=2)+'\n')
with (ev/'round95-paired.jsonl').open('x') as f:
 for rep in range(3):
  for store in (['memory','sqlite'] if rep%2==0 else ['sqlite','memory']):
   for variant in (['baseline','compact'] if rep%2==0 else ['compact','baseline']):
    cmd=[str(bins[variant]),'--store',store,'--scenario','agent_burst','--streams','100000','--events','1','--producers','1','--repetitions','1'];started=datetime.datetime.now(datetime.timezone.utc).isoformat()
    try:
     p=subprocess.run(cmd,capture_output=True,text=True,timeout=120);row={'repetition':rep,'variant':variant,'store':store,'command':cmd,'started_utc':started,'exit_code':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
    except subprocess.TimeoutExpired as e:row={'repetition':rep,'variant':variant,'store':store,'started_utc':started,'failure':'timeout120s','stdout':str(e.stdout),'stderr':str(e.stderr)}
    f.write(json.dumps(row)+'\n');f.flush();print(rep,store,variant,row.get('exit_code',row.get('failure')),flush=True)
 assert all(sha(bins[k])==h for k,h in identities.items())
 f.write(json.dumps({'kind':'completion','runs':12,'binaries_unchanged':True,'ended_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})+'\n')
