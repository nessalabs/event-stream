import pathlib, json, subprocess, hashlib, datetime, platform, os
root = pathlib.Path.cwd()
ev = root / 'docs/adr/002-resource-budgets-and-performance-evidence/evidence'
binary = pathlib.Path('/tmp/event-stream-round95-burst-target/release/examples/performance_baseline')
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
identity = sha(binary)
previous = json.loads((ev / 'round95-provenance.json').read_text())
assert identity == previous['binary_sha256']['compact']
meta = {'binary': str(binary), 'binary_sha256': identity, 'archive': previous['archives']['compact'], 'archive_sha256': previous['archive_sha256']['compact'], 'features': previous['features'], 'host': platform.platform(), 'load': os.getloadavg(), 'power': subprocess.check_output(['pmset', '-g', 'batt'], text=True), 'scope': 'Diagnostic scheduled arrivals, four producers on four streams. No release latency gate claimed. No builds or tests run concurrently. Ambient desktop remains open.', 'profiles': {'800_per_second': {'events': 1000, 'interval_us': 5000}, '16000_per_second': {'events': 4096, 'interval_us': 250}}}
(ev / 'round96-provenance.json').write_text(json.dumps(meta, indent=2) + '\n')
with (ev / 'round96-sustained.jsonl').open('x') as f:
    for rep in range(3):
        for store in (['memory', 'sqlite'] if rep % 2 == 0 else ['sqlite', 'memory']):
            for profile, config in meta['profiles'].items():
                cmd = [str(binary), '--store', store, '--scenario', 'sustained', '--streams', '4', '--producers', '4', '--events', str(config['events']), '--offer-interval-us', str(config['interval_us']), '--repetitions', '1']
                row = {'repetition': rep, 'store': store, 'profile': profile, 'command': cmd, 'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()}
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                    row.update(exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr)
                except subprocess.TimeoutExpired as error:
                    row.update(failure='timeout120s', stdout=str(error.stdout), stderr=str(error.stderr))
                f.write(json.dumps(row) + '\n')
                f.flush()
                print(rep, store, profile, row.get('exit_code', row.get('failure')), flush=True)
    assert sha(binary) == identity
    f.write(json.dumps({'kind': 'completion', 'runs': 12, 'binary_unchanged': True}) + '\n')
