# Evidence

The runner stores the latest bounded JSON report for each scenario in `runs/<scenario-id>.json`. These generated files are ignored by Git. The window reloads compatible reports at startup and exposes corrupt or stale evidence.

Copy evidence JSON from the window when a report should be reviewed or shared. Keep reviewed artifacts concise. Include the fixture, implementation identity, assertions, and measurement settings. Do not treat harness checks as event-store durability evidence.

### Reproducing Home comparison identities

`verification/export-warmed-home.py` computes configuration identity from the
raw JSONL environment and config rows. Canonical input is a JSON object with keys
`environment` and `config`. Remove only `kind`, `source_sha256`, `input_digest`
and `repetitions` from the environment. Keep the entire config row. Serialize
with Python JSON sorted keys, separators `(',', ':')`, and its default ASCII
escaping, then hash the UTF-8 bytes with SHA-256.

The Home metadata was regenerated using this explicit algorithm for every
historical experiment when adding round 4. Raw files, sample lines, measurements,
and source epochs were preserved. A matching store and population alone never
justify copying an earlier configuration hash. Different limits or host/compiler
settings must produce a separate comparison series.

Round 6 adds the isolated MiniSQLite diagnostic. Each experiment's `sample_key`
contains its full `config_fingerprint_input` and serialization rule. Hash that
object with sorted keys, compact separators, default ASCII escaping and SHA-256.
Its phases include connection setup, and the runtime phase includes registration
and shutdown. They are separate from warmed-drain measurements. For raw SQL,
`page_bytes: 0` means no configured SQL byte limit; the fixture has exactly 256
rows of 128 payload bytes. Peak RSS includes fixture setup. Allocation traffic
is cumulative Rust allocation, not retained memory or SQLite's C heap.

Rounds 7 and 8 are the controlled baseline and changed decoder in one paired
experiment. They are experiment iterations, not conversation turn numbers.
Each point summarizes three fresh processes. The same configuration key joins
the baseline and changed decoder; neither matches the earlier diagnostic or
warmed-drain series. Raw line references preserve all twelve samples.
