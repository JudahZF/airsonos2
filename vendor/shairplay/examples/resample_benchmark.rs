use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
struct Counting;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;
fn main() {
    let input: Vec<f32> = (0..352).flat_map(|i| [(i as f32 * 0.01).sin(), 0.0]).collect();
    for rooms in [1, 2, 6] {
        let mut streams: Vec<_> = (0..rooms)
            .map(|_| shairplay::codec::resample::StreamResampler::new(44100, 48000, 2).unwrap())
            .collect();
        for stream in &mut streams {
            for _ in 0..100 {
                std::hint::black_box(stream.process(&input));
            }
        }
        ALLOCS.store(0, Ordering::Relaxed);
        let start = std::time::Instant::now();
        let mut samples = 0;
        for _ in 0..10000 {
            for stream in &mut streams {
                samples += std::hint::black_box(stream.process(&input)).len();
            }
        }
        let elapsed = start.elapsed();
        println!(
            "rooms={rooms} calls={} allocations={} samples={samples} elapsed_us={}",
            rooms * 10000,
            ALLOCS.load(Ordering::Relaxed),
            elapsed.as_micros()
        );
    }
}
