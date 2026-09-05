import pathlib,json,statistics,hashlib,datetime
BASE=pathlib.Path(__file__).resolve().parent
ROOT=BASE.parents[3]
raw=BASE/'round100-mixed.jsonl'
rows=[json.loads(line) for line in raw.read_text().splitlines()]
provenance=json.loads((BASE/'round100-provenance.json').read_text())
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def median(group,key):return int(statistics.median(r[key] for r in group))
experiments=[]; summary=[]
for size in [65536,1048576]:
 for mode in ['control','mixed']:
  group=[r for r in rows if r['mode']==mode and r['snapshot_bytes']==size]
  assert len(group)==3
  config=dict(snapshot_bytes=size,mode=mode,offers=512,interval_us=1250,max_tasks=64,source_records=32,workers=2,features='all-features',host=provenance['host'],cpu=provenance['cpu'],cache_kib=4096,sampler_ms=1)
  fingerprint=hashlib.sha256(json.dumps(config,sort_keys=True).encode()).hexdigest()
  label=f'{mode} / {size//1024} KiB snapshot'
  metrics=dict(runtime_ns_median=median(group,'elapsed_ns'),cpu_us_median=median(group,'cpu_us'),memory_bytes_median=median(group,'rss_peak_bytes'),rust_live_bytes_median=median(group,'rust_live_peak_bytes'),rust_allocated_bytes_median=median(group,'allocated_bytes'),agents=1,accepted=median(group,'accepted'),rejected=median(group,'runtime_rejected')+median(group,'generator_rejected'),failed=0,delivered=None)
  experiments.append(dict(id=f'round100-{mode}-{size}',label=label,status='measured',comparison_key=dict(workload=f'mixed-recovery-diagnostic-{mode}',store='sqlite',durability='DELETE/FULL/ProcessRestart',payload_bytes=128,population=1,producers=1,streams=2,subscribers=0,concurrency=64,page_records=8,page_bytes=16384,measurement_phase='whole_scenario_including_open_setup_verification_and_reopen',instrumentation='System_GlobalAlloc_atomics_plus_1ms_RSS_sampler',config_fingerprint=fingerprint,memory_scope='sampled_process_including_harness_SQLite_and_profiler',cpu_scope='process_user_plus_system'),sample_count=3,metrics=metrics,provenance=dict(source_path=str(raw.relative_to(ROOT)),source_sha256=sha(raw),sample_key=json.dumps(dict(mode=mode,snapshot_bytes=size,repeats=[0,1,2],aggregation='median_per_metric',binary_sha256=provenance['binary_sha256'])),source_epoch=provenance['archive_sha256']),note='Short diagnostic, not steady-state or release qualification. 512 offers at 800/s on one foreground stream. Control skips maintenance. Latency is submission-to-receipt; scheduling delay is separate in raw data. Mixed and control have different workloads and are not chart-connected. No speedup claim.'))
  summary.append(dict(label=label,metrics=metrics,receipt_p99_ns_median=median(group,'receipt_p99_ns'),receipt_p99_ns_range=[min(r['receipt_p99_ns'] for r in group),max(r['receipt_p99_ns'] for r in group)],schedule_lateness_p99_ns_median=median(group,'schedule_lateness_p99_ns'),overlap_receipts=[r['overlap_receipts'] for r in group]))
(BASE/'round100-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
p=ROOT/'verification/evidence/home.json';home=json.loads(p.read_text());assert home['rounds'][-1]['number']==99
home['rounds'].append(dict(number=100,status='measured',build_id=provenance['binary_sha256'],experiments=experiments,summary='Twelve fresh-process mixed/control diagnostics with 64KiB and 1MiB snapshots: every run accepted all 512 offers and verified exact SQLite recovery; bounds and outcome accounting passed. Four measured Home cells retain raw provenance. Latency varies substantially, including controls; no improvement or release-capacity claim. Shared mixed workload is now a native/headless callback. Sustained scale, resource budgets and group-commit evaluation remain open.'))
home['generated_at_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
p.write_text(json.dumps(home,indent=2)+'\n')
print(json.dumps(summary,indent=2))
