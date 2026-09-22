# Receiver allocation measurement

Measured on the same x86_64 Linux machine, Rust 1.88 release profile, on
22 September 2026. Baseline is `3cc8a81`. The workload feeds each stream 10,000
352-frame stereo blocks at 44.1 kHz, resampled to 48 kHz, after 100 warmup blocks.
The counting allocator includes allocations and reallocations during processing.
The supplied example reproduces the workload:

```sh
cargo run --release --manifest-path vendor/shairplay/Cargo.toml --features resample --example resample-benchmark --locked
```

| Streams | Calls | Allocations before | Allocations after | Output samples (both) | Elapsed before (µs) | Elapsed after (µs) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 10,000 | 165,000 | 10,000 | 7,662,586 | 107,340 | 91,514 |
| 2 | 20,000 | 330,000 | 20,000 | 15,325,172 | 221,379 | 182,505 |
| 6 | 60,000 | 990,000 | 60,000 | 45,975,516 | 675,750 | 549,233 |

These are synthetic resampling measurements, not end-to-end room startup or
acoustic results. Single-run elapsed times are illustrative; allocation counts
and identical sample totals justify retaining the change. Each process call now
allocates its returned output once. Input adapters use existing interleaved
storage and Rubato writes into reused output scratch. At most one partial input
chunk is compacted per call.

Buffered delivery also transfers each PCM vector out of the queue instead of
cloning it. A pointer-identity regression test verifies this ownership transfer
and RTP-wrap ordering. The condition variable waits until the nearest source
sample deadline or a state change; it no longer polls at five-millisecond intervals.
Decoded PCM retains the two-second/8-MiB cap and paused TCP backpressure.
