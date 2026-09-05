set shell := ["zsh", "-cu"]

# Show all available commands.
[default]
help:
    @just --list

# Check every library and verification target without running tests or opening a window.
check-build:
    cargo check --locked --all-features --all-targets
    cargo check --locked --manifest-path verification/Cargo.toml --all-targets

# Compile the verification console without opening a window.
verify-check:
    cargo check --locked --manifest-path verification/Cargo.toml --all-targets

# Compile and launch the GPUI verification console.
verify: verify-check
    cargo run --manifest-path verification/Cargo.toml


# Run harness and scenario contract tests without opening a window.
verify-test:
    cargo test --manifest-path verification/Cargo.toml

# List workstreams, scenarios, and implementation availability.
verify-list:
    cargo run --manifest-path verification/Cargo.toml -- --list

# Run the same registered callback used by the native console.
verify-scenario id:
    cargo run --manifest-path verification/Cargo.toml -- --scenario {{id}}
