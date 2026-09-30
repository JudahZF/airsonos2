//! Bounded delivered PCM, independent of source presentation timestamps.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::PcmFrame;

#[derive(Clone, Debug)]
pub struct PcmQueue {
    inner: Arc<Mutex<State>>,
    ready: Arc<Notify>,
    sample_rate: u32,
    channels: u8,
    max_samples: usize,
    max_bytes: usize,
    max_age: Duration,
}

#[derive(Debug, Default)]
struct State {
    frames: VecDeque<(PcmFrame, Instant)>,
    samples: usize,
    allocated_bytes: usize,
    dropped: u64,
    reported_dropped: u64,
    notified: bool,
    closed: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PcmQueueStats {
    pub bytes: usize,
    pub retained_bytes: usize,
    pub byte_limit: usize,
    pub duration: Duration,
    pub oldest_age: Duration,
    pub dropped_frames: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct PcmAdmission {
    pub accepted: bool,
    /// True only when the consumer needs a new coalesced control notification.
    pub notify: bool,
}

impl PcmQueue {
    pub fn new(sample_rate: u32, channels: u8, duration: Duration) -> Self {
        let max_samples = (u128::from(sample_rate) * u128::from(channels) * duration.as_millis()
            / 1000)
            .min(usize::MAX as u128) as usize;
        Self {
            inner: Arc::new(Mutex::new(State::default())),
            ready: Arc::new(Notify::new()),
            sample_rate,
            channels,
            max_samples,
            max_bytes: max_samples.saturating_mul(8).saturating_add(1024),
            max_age: duration,
        }
    }

    pub fn max_samples(&self) -> usize {
        self.max_samples
    }

    /// Overflow discards oldest delivered audio to retain the live edge. Buffered
    /// network input applies its own backpressure before reaching this queue.
    pub fn push(&self, frame: PcmFrame) -> PcmAdmission {
        self.push_at(frame, Instant::now())
    }

    fn push_at(&self, frame: PcmFrame, now: Instant) -> PcmAdmission {
        let mut state = self.inner.lock().expect("PCM queue lock");
        let samples = frame.samples_f32_interleaved.len();
        let allocated = frame.samples_f32_interleaved.capacity() * std::mem::size_of::<f32>()
            + std::mem::size_of::<(PcmFrame, Instant)>();
        if state.closed {
            return PcmAdmission {
                accepted: false,
                notify: false,
            };
        }
        if samples == 0
            || samples > self.max_samples
            || frame.samples_f32_interleaved.capacity() > self.max_samples
            || frame.sample_rate != self.sample_rate
            || frame.channels != self.channels
            || self.channels == 0
            || samples % usize::from(self.channels) != 0
        {
            state.dropped += 1;
            return PcmAdmission {
                accepted: false,
                notify: false,
            };
        }
        self.expire(&mut state, now);
        while state.samples + samples > self.max_samples
            || state.allocated_bytes + allocated > self.max_bytes
        {
            if state
                .frames
                .front()
                .is_some_and(|(frame, _)| frame.buffered_permit.is_some())
            {
                return PcmAdmission {
                    accepted: false,
                    notify: false,
                };
            }
            Self::discard_front(&mut state);
        }
        state.samples += samples;
        state.allocated_bytes += allocated;
        state.frames.push_back((frame, now));
        let notify = !state.notified;
        state.notified = true;
        drop(state);
        self.ready.notify_one();
        PcmAdmission {
            accepted: true,
            notify,
        }
    }

    fn discard_front(state: &mut State) {
        if let Some((frame, _)) = state.frames.pop_front() {
            state.samples -= frame.samples_f32_interleaved.len();
            state.allocated_bytes -= frame.samples_f32_interleaved.capacity()
                * std::mem::size_of::<f32>()
                + std::mem::size_of::<(PcmFrame, Instant)>();
            state.dropped += 1;
        }
    }

    fn expire(&self, state: &mut State, now: Instant) {
        while state.frames.front().is_some_and(|(frame, at)| {
            frame.buffered_permit.is_none() && now.saturating_duration_since(*at) > self.max_age
        }) {
            Self::discard_front(state);
        }
    }

    /// Snapshot one control-loop batch and rearm notification before consuming it.
    /// New arrivals get another event instead of extending this batch indefinitely.
    pub fn begin_batch(&self) -> usize {
        let mut state = self.inner.lock().expect("PCM queue lock");
        self.expire(&mut state, Instant::now());
        state.notified = false;
        state.frames.len()
    }

    pub fn pop(&self) -> Option<PcmFrame> {
        self.pop_at(Instant::now())
    }

    fn pop_at(&self, now: Instant) -> Option<PcmFrame> {
        let mut state = self.inner.lock().expect("PCM queue lock");
        self.expire(&mut state, now);
        if let Some((frame, _)) = state.frames.pop_front() {
            state.samples -= frame.samples_f32_interleaved.len();
            state.allocated_bytes -= frame.samples_f32_interleaved.capacity()
                * std::mem::size_of::<f32>()
                + std::mem::size_of::<(PcmFrame, Instant)>();
            Some(frame)
        } else {
            state.notified = false;
            None
        }
    }

    pub async fn recv(&self) -> Option<PcmFrame> {
        loop {
            let ready = self.ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if let Some(frame) = self.pop() {
                return Some(frame);
            }
            if self.is_closed() {
                return None;
            }
            ready.await;
        }
    }

    pub fn clear(&self) {
        let mut state = self.inner.lock().expect("PCM queue lock");
        state.dropped += state.frames.len() as u64;
        state.frames.clear();
        state.samples = 0;
        state.allocated_bytes = 0;
        // Preserve notification ownership: an event already queued can drain the new epoch.
    }

    pub fn close(&self) {
        self.inner.lock().expect("PCM queue lock").closed = true;
        self.ready.notify_waiters();
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().expect("PCM queue lock").closed
    }

    /// Transfer new adapter discard counts once per coalesced notification.
    pub fn take_new_drops(&self) -> u64 {
        let mut state = self.inner.lock().expect("PCM queue lock");
        let dropped = state.dropped - state.reported_dropped;
        state.reported_dropped = state.dropped;
        dropped
    }

    pub fn stats(&self) -> PcmQueueStats {
        let now = Instant::now();
        let mut state = self.inner.lock().expect("PCM queue lock");
        self.expire(&mut state, now);
        PcmQueueStats {
            bytes: state.samples * std::mem::size_of::<f32>(),
            retained_bytes: state.allocated_bytes,
            byte_limit: self.max_bytes,
            duration: Duration::from_secs_f64(
                state.samples as f64
                    / (f64::from(self.sample_rate) * f64::from(self.channels)).max(1.0),
            ),
            oldest_age: state
                .frames
                .front()
                .map_or(Duration::ZERO, |(_, at)| now.saturating_duration_since(*at)),
            dropped_frames: state.dropped,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(marker: f32, count: usize) -> PcmFrame {
        PcmFrame {
            buffered_permit: None,
            playback_epoch: 0,
            sample_rate: 1000,
            channels: 1,
            samples_f32_interleaved: vec![marker; count],
            presentation_time: None,
        }
    }

    #[test]
    fn buffered_credits_survive_queue_transfer_and_prevent_expiry() {
        let credits = PcmCredits::new(1000, 1, Duration::from_millis(250));
        let queue = PcmQueue::new(1000, 1, Duration::from_millis(250));
        let mut audio = frame(1.0, 250);
        audio.buffered_permit = Some(credits.try_reserve(250).expect("first credit"));
        let now = Instant::now();
        assert!(queue.push_at(audio, now).accepted);
        let processing = queue
            .pop_at(now + Duration::from_secs(20))
            .expect("buffered frames wait under backpressure");
        assert!(credits.try_reserve(1).is_none());
        assert_eq!(queue.stats().dropped_frames, 0);
        drop(processing);
        assert!(credits.try_reserve(250).is_some());
    }

    #[test]
    fn bounds_duration_retains_current_audio_and_coalesces_notifications() {
        let queue = PcmQueue::new(1000, 1, Duration::from_millis(250));
        assert!(queue.push(frame(1.0, 100)).notify);
        assert!(!queue.push(frame(2.0, 100)).notify);
        assert!(!queue.push(frame(3.0, 100)).notify);
        assert_eq!(queue.stats().bytes, 800);
        assert_eq!(queue.stats().dropped_frames, 1);
        assert_eq!(queue.pop().unwrap().samples_f32_interleaved[0], 2.0);
        assert_eq!(queue.pop().unwrap().samples_f32_interleaved[0], 3.0);
        assert!(queue.pop().is_none());
        assert!(queue.push(frame(4.0, 100)).notify);
        assert!(!queue.push(frame(5.0, 251)).accepted);
    }

    #[test]
    fn arrival_age_expires_independently_of_source_time_and_flush_clears_pcm() {
        let queue = PcmQueue::new(1000, 1, Duration::from_millis(250));
        let now = Instant::now();
        let mut future = frame(1.0, 100);
        future.presentation_time = Some(now + Duration::from_secs(20));
        queue.push_at(future, now);
        assert!(queue.pop_at(now + Duration::from_millis(251)).is_none());
        queue.push(frame(2.0, 100));
        queue.clear();
        let mut next = frame(3.0, 100);
        next.playback_epoch = 1;
        assert!(!queue.push(next).notify);
        assert_eq!(queue.pop().unwrap().playback_epoch, 1);
    }
}

/// Credits follow buffered PCM through both bridge queues until encoding completes.
#[derive(Clone, Debug)]
pub struct PcmCredits {
    used: Arc<Mutex<(usize, usize)>>,
    max_samples: usize,
    max_bytes: usize,
}

#[derive(Debug)]
pub struct PcmPermit {
    credits: PcmCredits,
    samples: usize,
    bytes: usize,
}

impl PcmCredits {
    pub fn new(sample_rate: u32, channels: u8, duration: Duration) -> Self {
        let queue = PcmQueue::new(sample_rate, channels, duration);
        Self {
            used: Arc::new(Mutex::new((0, 0))),
            max_samples: queue.max_samples,
            max_bytes: queue.max_bytes,
        }
    }

    pub fn try_reserve(&self, samples: usize) -> Option<PcmPermit> {
        let bytes = samples
            .checked_mul(4)?
            .checked_add(std::mem::size_of::<(PcmFrame, Instant)>())?;
        let mut used = self.used.lock().expect("PCM credit lock");
        if samples > self.max_samples.saturating_sub(used.0)
            || bytes > self.max_bytes.saturating_sub(used.1)
        {
            return None;
        }
        used.0 += samples;
        used.1 += bytes;
        Some(PcmPermit {
            credits: self.clone(),
            samples,
            bytes,
        })
    }
}

impl Drop for PcmPermit {
    fn drop(&mut self) {
        let mut used = self.credits.used.lock().expect("PCM credit lock");
        used.0 -= self.samples;
        used.1 -= self.bytes;
    }
}
