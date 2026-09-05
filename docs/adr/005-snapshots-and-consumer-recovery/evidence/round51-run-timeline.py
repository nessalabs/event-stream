import datetime, hashlib, importlib.util, json, os, platform, subprocess
from pathlib import Path
base = Path(__file__).resolve().parent
source = Path('/tmp/event-stream-round51-snapshot-source')
binary = Path('/tmp/event-stream-round51-snapshot-target/release/examples/snapshot_resource')
archive = base / 'round51-timeline-source.tar'
def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()
spec = importlib.util.spec_from_file_location('collector', source / 'examples/run_snapshot_resource.py')
collector = importlib.util.module_from_spec(spec); spec.loader.exec_module(collector)
manifest = json.loads((source / 'SOURCE.json').read_text())
assert all(digest(source / name) == expected for name, expected in manifest.items())
identity = digest(binary)
meta = {'purpose':'Diagnostic chronology only; does not supersede round25 failed qualification', 'binary_sha256':identity, 'source_archive_sha256':digest(archive), 'runner_sha256':digest(Path(__file__)), 'features':['sqlite','snapshots','test-support'], 'platform':platform.platform(), 'load_average':os.getloadavg(), 'cpu':subprocess.check_output(['sysctl','-n','machdep.cpu.brand_string'],text=True).strip(), 'memory_bytes':int(subprocess.check_output(['sysctl','-n','hw.memsize'],text=True)), 'started_utc':collector.utc_now(), 'repetitions':3, 'snapshot_bytes':1048576, 'instrumented':True, 'quiet_window':'Both implementation agents confirmed no active build/test processes; source edits allowed'}
(base / 'round51-timeline-provenance.json').write_text(json.dumps(meta,indent=2)+'\n')
rows=[]
with (base / 'round51-timeline.jsonl.partial').open('x') as out:
 for repetition in range(3):
  modes=['foreground_verify_control','foreground_verify']
  if repetition % 2: modes.reverse()
  for mode in modes:
   args=['--store','sqlite','--snapshot-bytes','1048576','--mode',mode,'--instrumented','true']
   row=collector.run_cell(binary,args,len(rows)+1,repetition,60)
   rows.append(row);out.write(json.dumps(row)+'\n');out.flush();os.fsync(out.fileno())
 completion={'kind':'diagnostic_completion','samples':len(rows),'failures':sum(r['kind']=='snapshot_resource_failure' for r in rows),'binary_unchanged':digest(binary)==identity,'ended_utc':collector.utc_now()}
 out.write(json.dumps(completion)+'\n');out.flush();os.fsync(out.fileno())
assert completion['binary_unchanged']
print(json.dumps(completion))
for row in rows:
 print(json.dumps({key:row.get(key) for key in ['repetition','mode','append_p99_ns','append_max_ns','snapshot_phase_ns','overlapping_append_receipts','kind']}))
