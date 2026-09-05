import pathlib,json,hashlib,datetime
b=pathlib.Path(__file__).resolve().parent;root=b.parents[3]
raw=b/'round112-run.json';run=json.loads(raw.read_text());d={x['kind']:x for x in map(json.loads,run['stdout'].splitlines())};s=d['sample'];v=d['population_verification'];assert v['verified'];meta=json.loads((b/'round112-provenance.json').read_text());checks=json.loads((b/'round112-checks.json').read_text());assert all(checks.values())
config=dict(runtime=d['config'],population=d['population_config'],host=meta['host'],gates=json.loads((b/'round112-gates.json').read_text()))
e=dict(id='round112-sqlite-stability-screen',label='SQLite / 10k streams / ten cycles / stability screen',status='measured',sample_count=1,comparison_key=dict(workload='population-sustained-ten-cycle-stability-screen',store='sqlite',durability='DELETE/FULL/ProcessRestart',payload_bytes=128,population=10000,producers=4,streams=10000,subscribers=0,concurrency=256,page_records=256,page_bytes=2097152,measurement_phase='active_arrivals_and_commits_excludes_verification',instrumentation='System_GlobalAlloc_stage_probe_and_population_receipt_ledger',config_fingerprint=hashlib.sha256(json.dumps(config,sort_keys=True).encode()).hexdigest(),memory_scope='process_lifetime_peak_including_setup_harness_and_history',cpu_scope='process_user_plus_system_active_scope'),metrics=dict(runtime_ns_median=s['elapsed_ns'],cpu_us_median=s['cpu_user_us']+s['cpu_system_us'],memory_bytes_median=s['max_rss_bytes'],rust_live_bytes_median=None,rust_allocated_bytes_median=s['rust_allocated_bytes'],agents=10000,accepted=s['successful_receipts'],rejected=s['caller_rejected']+s['generator_rejected'],failed=s['caller_failed'],delivered=None),provenance=dict(source_path=str(raw.relative_to(root)),source_sha256=hashlib.sha256(raw.read_bytes()).hexdigest(),sample_key=json.dumps(dict(binary_sha256=meta['binary_sha256'],sample=1)),source_epoch=meta['archive_sha256']),note='Single sparse-load stability screen with predeclared diagnostic ceilings; not release SLO or long-running memory qualification. No subscribers/maintenance. Exact accepted history verified separately; shutdown resolved.')
p=root/'verification/evidence/home.json';home=json.loads(p.read_text());assert home['rounds'][-1]['number']==111;home['rounds'].append(dict(number=112,status='partial',build_id=meta['binary_sha256'],experiments=[e],summary='Frozen-runtime SQLite stability screen:10k streams, ten cycles,100k offers at800/s. All predeclared diagnostic checks pass with exact replay and resolved shutdown. One sample; CI, deployment recovery and long-running mixed stability remain open.'));home['generated_at_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p.write_text(json.dumps(home,indent=2)+'\n')
report=f'''# Round 112: bounded SQLite stability screen

The frozen individual-append runtime passes one longer sparse-load check. It
accepts all100,000 offers across10,000 streams and ten complete cycles at800
aggregate offers/s. Each stream is revisited about every12.5seconds. No runtime
batching or other production change was enabled.

Active duration: {s['elapsed_ns']/1e9:.3f} seconds. Receipt p99:
{s['successful_receipt_p99_ns']/1e6:.3f} ms. Process-lifetime peak RSS:
{s['max_rss_bytes']/2**20:.2f} MiB. Peak generator tasks:{s['peak_generator_tasks']}.
All accepted IDs, payloads and cursors replay exactly. Shutdown resolves all work.

[Gates](round112-gates.json) were saved before execution: zero rejected/failed
offers, full coverage, exact replay, bounded task/queue counts, resolved shutdown,
RSS below128MiB and receipt p99 below1second. The two resource ceilings are
coarse diagnostic screens, not customer SLOs. [All checks](round112-checks.json)
pass. This single sample does not prove repeatability or long-running stability.

[Raw output](round112-run.json), [provenance](round112-provenance.json), source
archive/hashes, collector and Home exporter are retained. Release build uses all
features and SQLite DELETE/FULL. No build/tests ran while sampling. Other host
activity was not controlled. RSS includes setup and harness allocations; history
remains retained, so this does not measure settled memory after cleanup. There
are no subscribers, maintenance or injected faults in this workload; prior fault
tests remain separate evidence. No wider optimization matrix was run.
'''
(b/'round112-report.md').write_text(report)
print(report)
