import pathlib,json,subprocess,hashlib,platform,datetime,os
BASE=pathlib.Path(__file__).resolve().parent
bins={v:pathlib.Path('/tmp/event-stream-round105-'+v+'-target/release/examples/retention_cleanup') for v in ['control','bounded']}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
identities={v:sha(p) for v,p in bins.items()}
meta=dict(binaries={v:str(p) for v,p in bins.items()},binary_sha256=identities,archive_sha256={v:sha(BASE/('round105-'+v+'-source.tar')) for v in bins},host=platform.platform(),power=subprocess.check_output(['pmset','-g','batt'],text=True),load=os.getloadavg(),started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),features='all-features',records=100000,floor=256,cleanup_row_limit=1,repetitions=5,scope='MemoryStore retention timestamp cleanup only; same public outcome and harness; no release budget qualification')
(BASE/'round105-provenance.json').write_text(json.dumps(meta,indent=2)+'\n')
with (BASE/'round105-cleanup.jsonl').open('x') as out:
 for repeat in range(5):
  for variant in (['control','bounded'] if repeat%2==0 else ['bounded','control']):
   cmd=[str(bins[variant])];result=subprocess.run(cmd,capture_output=True,text=True,timeout=30)
   assert result.returncode==0,result.stdout+result.stderr
   s=json.loads(result.stdout);assert s['records']==100000 and s['removed']==256 and s['verified_suffix']==99744 and s['cleanup_calls']==256
   s.update(variant=variant,repeat=repeat,command=cmd);out.write(json.dumps(s)+'\n');out.flush();print(variant,repeat,s['elapsed_ns'],s['cpu_us'],flush=True)
assert identities=={v:sha(p) for v,p in bins.items()}
