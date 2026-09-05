# Terminal integration proposal

Status: proposed companion boundary. No emulator dependency is selected or
installed by this proposal. [ADR 0009](adr.md) remains incomplete.

## Preserve the input before interpreting the screen

A terminal screen is a projection of ordered output bytes and resize operations.
Store those inputs so the application can reconstruct the screen later. The
stream library gives each committed input its order. The terminal companion
interprets the bytes; it does not allocate cursors or access database internals.

The application owns the PTY, process, file descriptor and source reads. It
serializes output and resize operations through one source coordinator. If two
independent readers submit operations concurrently, the library can preserve
commit order but cannot recover the order that the process actually observed.
An event timestamp does not solve that ambiguity.

Use versioned event schemas. Illustrative JSON below describes the shape; a
binary payload encoding can avoid base64 overhead in the actual companion.
The output payload is bytes, not UTF-8 text. An input chunk may end inside a
UTF-8 character or an escape sequence.

```json
{"schema":"terminal.output.v1","payload":{"bytes_base64":"aGVsbG8="}}
{"schema":"terminal.resize.v1","payload":{"columns":120,"rows":40}}
{"schema":"terminal.output.v1","payload":{"bytes_base64":"DXdvcmxk"}}
```

The application submits each resize at the position where it applied that
resize to the source. A replayed resize changes the projection only. It must
not resize the original PTY or signal the original process.

## Use a companion for screen interpretation

Keep a terminal emulator behind one adapter owned by the companion. The generic
incremental decoder seam is sufficient for framed application events, but a
screen interpreter also owns persistent screen state and terminal-response
policy. Those responsibilities do not belong in the generic event-history
bounded context.

A proposed companion API accepts a committed record, checks its schema and
advances one projection. It returns bounded dirty regions or an explicit error.
Consumers can request a bounded screen snapshot separately. Emitting the whole
screen after every small chunk would repeatedly copy unchanged cells.

```text
PTY reader -> output / resize events -> durable stream
                                          |
                                     committed replay
                                          |
                                 terminal companion
                                          |
                           bounded changed screen regions
```

The projection records the last applied cursor. It rejects a wrong incarnation,
a cursor gap and an unsupported required event schema. An exact replay of an
already applied cursor is handled by the projection's recovery contract, not by
executing terminal commands twice. Terminal projection state is separate from
provider-agent normalization.

## Bound memory before allocating it

Configuration specifies maximum rows, columns, screen cells, scrollback bytes,
input-chunk bytes, pending escape-sequence bytes and output-update bytes.
Validate `rows * columns` with checked arithmetic and against the cell limit
before a resize allocates memory. Reject a resize beyond the supported capacity.
Do not allocate first and then decide that the requested size was too large.

Use a ring or deque for bounded scrollback. Evicting the oldest scrollback line
must not shift the entire retained history. Count retained text capacity and
per-cell metadata, not only the number of visible characters. Wide characters,
combining sequences and hyperlinks can require additional state. Their costs
must fit the same explicit budget.

A malformed or never-terminated escape sequence cannot accumulate unlimited
input. The companion must expose an explicit supported policy: terminate the
projection with the source cursor and bounded diagnostic, or consume an
unsupported sequence under a documented bounded rule. It must not silently
claim that an incomplete interpretation is a faithful screen.

Do not give every idle terminal its own OS thread or timer. A caller drives
bounded work on its existing executor. A drive call limits input bytes and
output work. If more work remains, it yields and retains only budgeted state.
Any emulator that cannot provide those ownership and work boundaries needs a
separate, measured integration design before adoption.

## Replay must not produce external responses

Some terminal inputs request a response or an application action. Examples
include terminal identity/status queries and clipboard requests. The companion
separates a screen update from any requested outbound action.

Recovery and historical replay run with outbound actions disabled. They must
not write to a PTY, change a clipboard, open a URL or repeat an application
callback. A live application can explicitly accept a bounded response through
its own I/O policy. Projection code never obtains that I/O handle implicitly.

If live responses affect subsequent source output, record the relevant response
or interaction as part of the application's source protocol. Replaying only
output bytes reconstructs a display; it does not recreate an interactive
process or make external actions exactly once.

## Snapshot compatibility is explicit

A terminal snapshot identifies the companion schema and the exact emulator
state format/version. It includes the screen, cursor modes, pending decoder
state and configured geometry needed to resume interpretation. It excludes
pointers, threads, file descriptors and callbacks.

A different emulator version must either prove compatibility through fixtures
or reject the snapshot and replay retained input from a compatible boundary.
The application cannot assume a native emulator memory dump is a portable
snapshot. Raw source retention and snapshot retention follow ADRs 0005–0007;
a terminal companion cannot independently discard required recovery input.

## Candidate decision and acceptance tests

`libghostty-vt` remains a candidate named in the earlier discussion. Its API,
portability, licensing, snapshot support and resource behavior have not been
qualified in this repository. This proposal makes no claim about those current
capabilities. No dependency is selected merely because an FFI binding exists.

Revisit selection when the terminal companion is scheduled for implementation
and a pinned candidate can be compiled against this contract. Compare a pinned
candidate with transporting ordered bytes and resize events without server-side
screen interpretation. The latter supports opaque transport but does not claim
to provide a parsed screen.

Before selecting an implementation, require:

- Deterministic fixtures split at every byte boundary, including UTF-8, escape
  sequences, resize operations and malformed input. Every partition produces
  the same screen or the same documented error.
- Complete replay and snapshot-plus-suffix recovery produce the same screen,
  modes and pending parser state. Unsupported versions fail explicitly.
- Replay cannot call outbound action hooks. Live mode reports bounded response
  requests through the application-owned boundary.
- Measured idle memory, per-cell/scrollback memory, allocations, CPU per input
  MiB and worst drive duration under fixed limits. Include resize churn and
  long escape sequences. Report rejection cost separately.
- The default stream crate builds and runs without the companion or emulator.

These are acceptance requirements, not passing evidence. A companion selection
needs its own pinned dependency decision, runnable fixtures and measurements.
