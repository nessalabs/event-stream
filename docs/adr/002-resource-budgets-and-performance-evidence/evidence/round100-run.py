"""Twelve fresh-process diagnostic samples. Run only with other builds idle."""
import datetime, hashlib, json, pathlib, platform, subprocess, tempfile, os
BASE = pathlib.Path(__file__).resolve().parent
BINARY = pathlib.Path('/tmp/event-stream-round100-target/release/examples/mixed_resource')
def digest(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def command(*args): return subprocess.check_output(args, text=True).strip()
identity = digest(BINARY)
provenance = dict(binary=str(BINARY), binary_sha256=identity,
    archive_sha256=digest(BASE/'round100-source.tar'), host=platform.platform(),
    cpu=command('sysctl','-n','machdep.cpu.brand_string'), power=command('pmset','-g','batt'),
    features='all-features', rust=command('rustc','-Vv'), load=os.getloadavg(),
    started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    comparison='diagnostic only; no release threshold or improvement claim',
    schedule='three repeats; alternate control/mixed order; 64KiB and 1MiB snapshots',
    offers=512, interval_us=1250, foreground_streams=1, max_generator_tasks=64)
(BASE/'round100-provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
with (BASE/'round100-mixed.jsonl').open('w') as output:
 for repeat in range(3):
  for size in [65536,1048576]:
   for mode in (['control','mixed'] if repeat%2==0 else ['mixed','control']):
    with tempfile.TemporaryDirectory(prefix='round100-mixed-') as parent:
     args=[str(BINARY),mode,str(size),str(pathlib.Path(parent)/'db')]
     result=subprocess.run(args,text=True,capture_output=True,timeout=60)
     if result.returncode:
      raise RuntimeError(result.stdout+result.stderr)
     sample=json.loads(result.stdout)
     assert sample['offered']==sample['accepted']+sample['runtime_rejected']+sample['generator_rejected']
     assert sample['receipt_samples']==sample['accepted']
     assert sample['lateness_samples']==sample['accepted']+sample['runtime_rejected']
     assert sample['peak_tasks']<=64 and sample['peak_runtime_queue']<=1024
     assert sample['replay_pages']==64 and sample['failures']==0
     assert sample['rss_samples']>0
     if mode=='mixed':
      assert sample['removed_records']==32 and sample['replica_tail']==33
      assert sample['overlap_receipts']>0
     sample.update(repeat=repeat,command=args,binary_sha256=identity)
     output.write(json.dumps(sample)+'\n');output.flush()
     print(mode,size,repeat,sample['accepted'],sample['receipt_p99_ns'],flush=True)
assert digest(BINARY)==identity
