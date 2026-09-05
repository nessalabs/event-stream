import pathlib,json,subprocess,hashlib,platform,datetime
b=pathlib.Path(__file__).resolve().parent
binary=pathlib.Path('/tmp/event-stream-round112-target/release/examples/performance_baseline')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
meta=dict(binary=str(binary),binary_sha256=sha(binary),archive_sha256=sha(b/'round112-source.tar'),gates_sha256=sha(b/'round112-gates.json'),host=platform.platform(),started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
(b/'round112-provenance.json').write_text(json.dumps(meta,indent=2)+'\n')
cmd=[str(binary),'--store','sqlite','--scenario','population_sustained','--streams','10000','--producers','4','--events','25000','--offer-interval-us','5000','--payload-bytes','128','--repetitions','1']
print('START: ten cycles, expected active duration125 seconds, hard timeout300 seconds',flush=True)
r=subprocess.run(cmd,capture_output=True,text=True,timeout=300)
(b/'round112-run.json').write_text(json.dumps(dict(command=cmd,returncode=r.returncode,stdout=r.stdout,stderr=r.stderr),indent=2)+'\n')
assert r.returncode==0,r.stderr
rows=list(map(json.loads,r.stdout.splitlines()));d={x['kind']:x for x in rows};s=d['sample'];v=d['population_verification'];c=[x for x in rows if x['kind']=='population_cycle'];g=json.loads((b/'round112-gates.json').read_text())
checks=dict(all_accepted=s['offered']==s['successful_receipts']==100000,no_rejected_or_failed=all(s[k]==0 for k in ['caller_rejected','generator_rejected','caller_failed','runtime_failed']),all_streams=v['successful_stream_coverage']==10000 and v['uncovered_streams']==0,receipt_ledger=v['accepted_receipts']==100000,cycles=len(c)==10 and all(x['offered']==x['accepted']==10000 for x in c),bounded_tasks=s['peak_generator_tasks']<=256,bounded_queue=s['peak_queued_appends']<=d['config']['max_queued_appends'],shutdown=d['resources']['shutdown_closed'] and d['resources']['shutdown_unresolved']==0,rss_screen=s['max_rss_bytes']<=g['rss_ceiling_bytes'],latency_screen=s['successful_receipt_p99_ns']<=g['receipt_p99_ceiling_ns'],binary_unchanged=sha(binary)==meta['binary_sha256'])
(b/'round112-checks.json').write_text(json.dumps(checks,indent=2)+'\n')
print(json.dumps(dict(checks=checks,rss_bytes=s['max_rss_bytes'],receipt_p99_ns=s['successful_receipt_p99_ns'],peak_generator_tasks=s['peak_generator_tasks'])),flush=True)
assert all(checks.values()),checks
