# Bounded audio and performance evidence

Measured on 22 September 2026 on one x86_64 Linux 6.18.50 host, Rust 1.88 release builds. These are synthetic in-process WAV/HTTP workloads, not measurements of Sonos, sender networks, socket delivery, or acoustic alignment. No percentage improvement is claimed from a single run.

## Workload comparison

Baseline: stage 05 commit `f282812`. Current: stage 06 queue/accounting changes. The same `pipeline_benchmark` source ran on both trees; the baseline copy omitted only the new queue-duration/credit fields and the optional scratch-conversion microbenchmark. Each case used 500 frames of 10 ms, 48 kHz stereo, paced every 10 ms on the same machine. There were 20 startup/removal cycles per room before steady playback. The consumer polled each body chunk; all expected PCM sizes and EOF transitions were asserted.

Values below are **before / after**. CPU includes startup and steady playback; allocation counts cover the steady loop. Peak RSS is process resource usage reported by the Unix parent, including process startup. Startup samples are 20, 40 and 120 for one, two and six rooms; idle has no startup samples.

| Rooms | CPU seconds | Peak RSS KiB | Steady allocations | Startup p95 µs | Stop-to-EOF p95 µs |
| --- | --- | --- | --- | --- | --- |
| 0 | 0.002830 / 0.003066 | 13440 / 13504 | 1 / 1 | 0 / 0 | 0 / 0 |
| 1 | 0.005895 / 0.005582 | 13396 / 13336 | 1502 / 1501 | 1073 / 1074 | 1 / 1 |
| 2 | 0.008398 / 0.010939 | 13200 / 13380 | 3003 / 3001 | 1078 / 1071 | 1 / 1 |
| 6 | 0.019343 / 0.019796 | 13508 / 13524 | 9007 / 9001 | 1094 / 1069 | 1 / 0 |

The workload had no audio loss (every expected 1920-byte PCM body chunk was consumed). Queue duration and oldest age were zero at the final drained snapshot. Current counters retained 999280, 1998560 and 5995680 consumed bytes after all streams were removed; baseline counters reset to zero on removal. The small allocation reduction in this WAV workload is not a significant performance claim. Most of this change enforces resource limits and correct accounting.

Reproduce with:

```sh
nix develop -c cargo build -p airsonos2-stream --example pipeline_benchmark --release --locked
python3 scripts/measure-command.py target/release/examples/pipeline_benchmark 6 500
target/release/examples/pipeline_benchmark --conversion
```

## Retained allocation improvements

The MP3 input conversion microbenchmark ran 10000 conversions of 960 samples using the former allocating function and the reusable scratch function on identical input. Allocations fell from **10000 to 1**; elapsed time was 20296 µs versus 17766 µs in this run. The output conversion and sample-level tests are unchanged. Reuse applies to MP3 stdin; WAV output must retain each byte buffer until subscribers finish with it.

The vendor resampler benchmark and its 1/2/6-stream results are in `vendor/shairplay/PERFORMANCE.md`. Ready receiver buffers are moved, and resampler scratch is reused. No pooling or SIMD was added.

## Queue budgets and policy

- Realtime adapter PCM: at most 250 ms; reject an oversized callback before copying, evict the oldest pending PCM under pressure, and expire old arrivals on dequeue. Only one outstanding event notification exists per session queue. FLUSH closes and clears the old queue and creates a new queue for the new epoch.
- Buffered adapter PCM: 250 ms of delivery credits cover both adapter and encoder queues together. Admission fails before copying when credits are occupied; the receiver retries the same decoded packet and eventually applies TCP backpressure. Credits return after encoding or cancellation. Buffered frames are not expired as realtime frames are; their arrival age remains observable while waiting. Large decoded packets admit prefixes without replaying an admitted prefix on retry.
- Encoder PCM: duration budget is the configured startup deadline plus the normalized manual room-offset span plus 250 ms. Default is 2750 ms. The deadline is limited to 60 seconds and the span to 20 seconds. Keeping this budget fixed preserves the chosen starting sample during a delayed WAV release; a regression checks a release beyond the adapter's 250 ms window.
- PCM payload bytes are bounded by sample rate × channels × 4 × duration. Retained frame allocations and active queue entries have a separate limit of twice that payload limit plus 1024 bytes. This count excludes the queue container's bounded spare slots and fixed bookkeeping. Metrics report payload bytes, retained bytes, byte limit, duration, oldest arrival age and discarded frames. Source presentation timestamps do not replace arrival-age measurements.
- Encoded HTTP backlog: 64 chunks of at most 16 KiB, plus at most one 1 MiB shared source allocation. A body reaches EOF if it lags beyond capacity or its next chunk is over two seconds old, so a client reconnects at the live edge. Detailed consumption is counted when the body is polled, not when bytes are broadcast.

Focused regressions cover overflow/latest retention, expiration independent of future source timestamps, buffered credit exhaustion/resumption, epoch replacement, HTTP lag EOF, delayed startup sample retention, and monotonic completed accounting. Real sender pause/soak memory behavior remains a hardware acceptance item.

## Build evidence

Both native amd64 images built successfully on this host. The generic build with an initially cold task-specific Cargo cache took **103.119 s** (including base/runtime downloads); its repeat took **2.455 s**, with Cargo reporting 0.18 s. The subsequent Home Assistant image took **36.703 s**, reusing the identical builder layer and spending time on its runtime image. This is a cache demonstration, not a comparison against a cleaned previous build system. The daemon's unrelated caches were preserved.

Both Dockerfiles use identical builder steps and shared architecture-specific Cargo target cache mounts, plus a registry cache. CI caches Cargo outputs and BuildKit layers. Native multiarchitecture packaging and clean-source acceptance are recorded by the release gate.
