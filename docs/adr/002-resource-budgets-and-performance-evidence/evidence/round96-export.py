import pathlib, json, statistics, hashlib, datetime, shutil
root = pathlib.Path.cwd()
ev = root / 'docs/adr/002-resource-budgets-and-performance-evidence/evidence'
raw_path = ev / 'round96-sustained.jsonl'
raw = raw_path.read_bytes()
runs = [json.loads(line) for line in raw.splitlines()]
assert runs[-1] == {'kind': 'completion', 'runs': 12, 'binary_unchanged': True}
meta = json.loads((ev / 'round96-provenance.json').read_text())
groups = {}
for line, run in enumerate(runs[:-1], 1):
    assert run['exit_code'] == 0
    data = {item['kind']: item for item in map(json.loads, run['stdout'].splitlines())}
    sample, resources = data['sample'], data['resources']
    offered = meta['profiles'][run['profile']]['events'] * 4
    assert sample['offered'] == sample['append_tasks'] == offered
    assert sample['successful_receipts'] + sample['caller_rejected'] + sample['caller_failed'] == offered
    assert sample['runtime_accepted'] == sample['successful_receipts'] == sample['inserted']
    assert sample['runtime_failed'] == sample['caller_failed'] == 0
    assert sample['peak_queued_appends'] <= data['config']['max_queued_appends']
    assert sample['peak_queued_append_bytes'] <= data['config']['max_queued_append_bytes']
    assert sample['shutdown_queued_appends'] == sample['shutdown_admission_waiters'] == 0
    assert resources['shutdown_closed'] and resources['shutdown_unresolved'] == 0
    assert sample['all_outcome_latency_samples'] == sample['schedule_lateness_samples'] == offered
    assert sample['successful_receipt_samples'] == sample['successful_receipts']
    assert sample['rejected_samples'] == sample['caller_rejected']
    groups.setdefault((run['store'], run['profile']), []).append((line, data))
summary, experiments = [], []
for (store, profile), pairs in sorted(groups.items()):
    samples = [data['sample'] for _, data in pairs]
    assert len(samples) == 3
    median = lambda field: int(statistics.median(s[field] for s in samples))
    config = {'runtime': pairs[0][1]['config'], 'profile': meta['profiles'][profile], 'features': meta['features'], 'host': meta['host'], 'cpu': pairs[0][1]['environment']['cpu'], 'producers': 4, 'streams': 4, 'payload_bytes': 128}
    fingerprint = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    metrics = {'runtime_ns_median': median('elapsed_ns'), 'cpu_us_median': int(statistics.median(s['cpu_user_us'] + s['cpu_system_us'] for s in samples)), 'memory_bytes_median': median('max_rss_bytes'), 'rust_live_bytes_median': None, 'rust_allocated_bytes_median': median('rust_allocated_bytes'), 'agents': 4, 'accepted': median('runtime_accepted'), 'rejected': median('caller_rejected'), 'failed': 0, 'delivered': None}
    cell = {'store': store, 'profile': profile, 'offered': samples[0]['offered'], 'metrics': metrics, 'accepted_range': [min(s['runtime_accepted'] for s in samples), max(s['runtime_accepted'] for s in samples)], 'successful_receipt_p99_ns_median': median('successful_receipt_p99_ns'), 'schedule_lateness_p99_ns_median': median('schedule_lateness_p99_ns'), 'queue_wait_p99_ns_median': median('queue_wait_p99_ns'), 'store_service_p99_ns_median': median('store_service_p99_ns'), 'maximum_observed_queue': max(s['peak_queued_appends'] for s in samples)}
    summary.append(cell)
    experiments.append({'id': fingerprint[:16], 'label': f'{store} · scheduled {profile.replace("_", " ")} · four producers', 'status': 'measured', 'comparison_key': {'workload': 'scheduled_arrivals_' + profile, 'store': store, 'durability': 'ephemeral' if store == 'memory' else 'ProcessRestart_DELETE_FULL', 'payload_bytes': 128, 'population': 4, 'producers': 4, 'streams': 4, 'subscribers': 0, 'concurrency': samples[0]['offered'], 'page_records': 0, 'page_bytes': 2097152, 'measurement_phase': 'scheduled task creation, offers and receipt drain; excludes stream setup/shutdown', 'instrumentation': 'Rust_GlobalAlloc+stage_probe;preallocated_per_offer_tasks', 'memory_scope': 'process_lifetime_peak_including_harness', 'cpu_scope': 'process_user_plus_system_timed_scope', 'config_fingerprint': fingerprint}, 'sample_count': 3, 'metrics': metrics, 'provenance': {'source_path': str(raw_path.relative_to(root)), 'source_sha256': hashlib.sha256(raw).hexdigest(), 'sample_key': json.dumps({'lines': [line for line, _ in pairs], 'aggregation': 'median_per_metric', 'binary_sha256': meta['binary_sha256'], 'config': config}, sort_keys=True), 'source_epoch': meta['archive_sha256']}, 'note': 'Diagnostic scheduled arrivals on four streams, not a release gate or 100k-agent qualification. Caller latency excludes scheduling lateness; report records both. One task is allocated per offered request. RSS includes those tasks and profiling. No subscribers or mixed recovery operations.'})
(ev / 'round96-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
report = ['# Scheduled-arrival diagnostic', '', 'Twelve fresh processes use the archived round 95 release binary. Four producers offer 128-byte events to four streams. Each request has its own deadline. Slow writes do not postpone later deadlines.', '', 'These are short diagnostic runs. The 800/s profile schedules 4,000 requests across about five seconds. The 16,000/s profile schedules 16,384 requests across about one second. All processes account for every outcome, preserve queue limits and finish shutdown without unresolved work.', '', '| Store | Requested rate/s | Accepted / offered (median) | Receipt p99 ms | Scheduling p99 ms | Peak RSS MiB |', '| --- | ---: | ---: | ---: | ---: | ---: |']
for cell in summary:
    m = cell['metrics']
    report.append(f"| {cell['store']} | {cell['profile'].split('_')[0]} | {m['accepted']} / {cell['offered']} | {cell['successful_receipt_p99_ns_median']/1e6:.3f} | {cell['schedule_lateness_p99_ns_median']/1e6:.3f} | {m['memory_bytes_median']/2**20:.2f} |")
report += ['', 'Each latency value is the median of three per-run p99 values. Receipt latency starts when the task submits its request. Scheduling lateness measures how late the task woke relative to its intended deadline. Do not add these percentile values: the slowest requests may differ.', '', 'SQLite uses DELETE journal mode and FULL synchronization. Its overload result describes this adapter and configuration; it does not isolate engine execution from transaction, flush, queue and runtime costs. Four streams each allow 128 queued appends, so together they can reach 512 before the global 1,024 limit.', '', 'The harness allocates one task per future offer, including tasks waiting for their deadline. Peak RSS therefore includes load-generator memory. This short test does not establish long-running steady state, idle-agent memory, delivery latency, or 100,000 active producers. No numerical release latency budget was selected for these diagnostics.', '', 'The raw output also records store-service and queue-delay samples. Further investigation should isolate these costs before changing storage or scheduling.', '', '[Raw runs](round96-sustained.jsonl) · [Provenance](round96-provenance.json) · [Summary](round96-summary.json) · [Source archive](round95-source.tar)', '']
(ev / 'round96-report.md').write_text('\n'.join(report))
home_path = root / 'verification/evidence/home.json'
home = json.loads(home_path.read_text())
assert home['rounds'][-1]['number'] == 95
home['generated_at_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
home['rounds'].append({'number': 96, 'status': 'measured', 'build_id': meta['binary_sha256'], 'experiments': experiments, 'summary': 'Twelve fresh scheduled-arrival diagnostics on four streams. Both stores accept every request at 800/s. Higher offered load exposes SQLite overload. Outcomes, sampled latencies, bounded queues and resolved shutdown are checked. Four measured cells added; scheduling delay is reported separately. This is not sustained 100k-agent or full mixed-workload qualification.'})
home_path.write_text(json.dumps(home, indent=2) + '\n')
for name in ('run', 'export'):
    shutil.copyfile(f'/tmp/event-stream-round96-{name}.py', ev / f'round96-{name}.py')
print('\n'.join(report))
