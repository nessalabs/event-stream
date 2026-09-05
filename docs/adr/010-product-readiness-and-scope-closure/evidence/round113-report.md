# Round 113: CI coverage for shared verification

The core workflow previously ignored changes confined to shared verification
fixtures, even though core integration tests import those files. Both push and
pull-request filters now include that directory.

A separate macOS workflow compiles every console target and runs its headless
library tests. Triggers cover library source/manifests, console source/manifests,
shared fixtures, Home data and the ADR files read by the catalog audit. Saved
benchmark archives and logs are excluded to avoid unnecessary jobs. The job
uses read-only repository permissions, a 30-minute timeout and cancellation of
superseded runs. It does not open or inspect a native window.

Both workflow YAML files parse locally. The existing actions/checkout v7 tag
was confirmed against the upstream repository before reuse. The exact console
check command succeeds locally and all 19 headless tests pass after the Home
update. These checks do not prove a successful GitHub runner execution. This
repository still needs a reviewed Git checkpoint and observed remote CI pass.
No production code or performance measurements changed in this round.
