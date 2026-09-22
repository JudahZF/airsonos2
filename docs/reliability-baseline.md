# Reliability baseline

The implementation baseline is `6affa48e3d0473c0e47852e4833b67d355cd371c`.
The tracked source checkout was clean, with no local edits to preserve. The
postplan refers to `ce59a42` plus reviewed working-tree changes, which are not
fully represented in this checkout. Each finding must be checked against the
actual tracked source before implementation. The original `airsonos2-plan.md`
is historical context, not the implementation record for this effort.

Stage 00 makes the standalone receiver reproducible by tracking its own lockfile
(the receiver is excluded from the root workspace). The lock uses OpenH264 0.9.3,
the last 0.9 release whose `wide` dependency supports the pinned Rust 1.88
toolchain. OpenH264 0.9.5 and later declare Rust 1.85 but depend on `wide` 1.x,
which requires Rust 1.89. Linux development shells now include the native audio
and window-system libraries needed by the receiver's test/example dependencies.

The workspace lock updates h2 to 0.4.19 and rustls to 0.23.45 (including
rustls-webpki 0.103.15), fixing RUSTSEC-2026-0258 and RUSTSEC-2026-0285 found
while establishing this baseline. The dependency policy now checks explicitly
allowed licenses, including the existing vendored receiver's LGPL-3.0-or-later
license. Existing source and license notices must remain in distributions.

## Reproduce

Use the pinned Nix shell from a fresh checkout. All dependency-resolving build
commands retain `--locked`:

```sh
nix develop -c cargo fmt --all -- --check
nix develop -c cargo clippy --workspace --all-features --all-targets --locked -- -D warnings
nix develop -c cargo test --workspace --all-features --locked
nix develop -c cargo test --manifest-path vendor/shairplay/Cargo.toml --locked
nix develop -c cargo test --manifest-path vendor/shairplay/Cargo.toml --features ap2,resample --locked
nix develop -c cargo test --manifest-path vendor/shairplay/Cargo.toml --features video,hls --locked
nix develop -c cargo deny check advisories bans licenses sources
```

CI runs the workspace gates and each standalone feature combination. The same
workflow can be called by release workflows. Synthetic packet, PCM, and fake
Sonos fixtures belong beside the first regression that needs them; stage 00 does
not add a separate test framework.

## Validation record

Validated on x86_64 Linux with the pinned Rust 1.88 Nix shell on 2026-09-22.
A fresh local clone contains only tracked source files; build caches can be
stored outside that clone with `CARGO_TARGET_DIR`.

| Check | Result |
| --- | --- |
| Workspace formatting | Passed |
| Workspace all-target Clippy | Passed |
| Workspace tests | 116 passed |
| Standalone receiver, default features | 73 tests and doctests passed |
| Standalone receiver, AP2/resample | 151 tests and doctests passed |
| Standalone receiver, video/HLS | 158 tests and doctests passed |
| Dependency advisories, bans, licenses, sources | Passed |

These are automated source checks, not physical playback or acoustic evidence.
Container packaging and hardware acceptance are recorded in the release stage.
