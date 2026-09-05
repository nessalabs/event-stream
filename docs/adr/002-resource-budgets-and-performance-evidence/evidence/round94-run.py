import pathlib,json,subprocess,hashlib,datetime,os,platform
root=pathlib.Path.cwd();out=root/'docs/adr/002-resource-budgets-and-performance/evidence'
# Resolve the documented ADR folder rather than assuming its suffix.
out=next((root/'docs/adr').glob('002-*'))/'evidence';out.mkdir(exist_ok=True)
binary=pathlib.Path('/tmp/event-stream-round93-snapshot-target/release/examples/performance_baseline');digest=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();identity=digest(binary)
meta={'binary':str(binary),'binary_sha256':identity,'source_archive':'docs/adr/005-snapshots-and-consumer-recovery/evidence/round93-source.tar','source_archive_sha256':digest(root/'docs/adr/005-snapshots-and-consumer-recovery/evidence/round93-source.tar'),'features':['sqlite','snapshots','test-support'],'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'load':os.getloadavg(),'host':platform.platform(),'power':subprocess.check_output(['pmset','-g','batt'],text=True),'scope':'One try_append task per stream released together, finite1024append queue; not sustained100kproducers. No project builds/tests concurrent.'}
(out/'round94-provenance.json').write_text(json.dumps(meta,indent=2)+'\n')
with (out/'round94-burst.jsonl').open('x') as f:
 for rep in range(3):
  sizes=[1000,10000,100000];sizes=sizes[rep:]+sizes[:rep]
  for size in sizes:
   for store in (['memory','sqlite'] if rep%2==0 else ['sqlite','memory']):
    cmd=[str(binary),'--store',store,'--scenario','agent_burst','--streams',str(size),'--events','1','--producers','1','--repetitions','1']
    started=datetime.datetime.now(datetime.timezone.utc).isoformat()
    try:
     p=subprocess.run(cmd,capture_output=True,text=True,timeout=120)
     row={'repetition':rep,'store':store,'tasks':size,'command':cmd,'started_utc':started,'exit_code':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
    except subprocess.TimeoutExpired as e:row={'repetition':rep,'store':store,'tasks':size,'started_utc':started,'failure':'timeout120s','stdout':str(e.stdout),'stderr':str(e.stderr)}
    f.write(json.dumps(row)+'\n');f.flush();print(rep,store,size,row.get('exit_code',row.get('failure')),flush=True)
 assert digest(binary)==identity
 f.write(json.dumps({'kind':'completion','runs':18,'binary_unchanged':True,'ended_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})+'\n')
