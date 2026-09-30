//! Sample rate conversion and channel mixdown for AirPlay audio.

use rubato::audioadapter_buffers::direct::InterleavedSlice;
use rubato::{Async, FixedAsync, Resampler, SincInterpolationParameters, SincInterpolationType, WindowFunction};

/// Persistent F32 resampler for streaming audio.
/// Buffers input internally and processes in fixed chunks.
pub struct StreamResampler {
    resampler: Async<f32>,
    channels: usize,
    chunk_size: usize,
    /// Accumulated input samples (interleaved).
    pending: Vec<f32>,
    /// Reused interleaved output workspace; the returned buffer stays caller-owned.
    scratch: Vec<f32>,
}

impl StreamResampler {
    /// Create a new resampler. Returns `None` if rates are equal.
    pub fn new(from_rate: u32, to_rate: u32, channels: usize) -> Option<Self> {
        if from_rate == to_rate {
            return None;
        }
        let params = SincInterpolationParameters {
            sinc_len: 64,
            f_cutoff: 0.95,
            interpolation: SincInterpolationType::Linear,
            oversampling_factor: 128,
            window: WindowFunction::BlackmanHarris2,
        };
        let ratio = to_rate as f64 / from_rate as f64;
        let chunk_size = 128; // small for low latency
        let resampler = Async::<f32>::new_sinc(ratio, 1.0, &params, chunk_size, channels, FixedAsync::Input).ok()?;
        let scratch = vec![0.0; resampler.output_frames_max() * channels];
        Some(Self {
            resampler,
            channels,
            chunk_size,
            pending: Vec::with_capacity(chunk_size * channels * 4),
            scratch,
        })
    }

    /// Resample interleaved F32 audio. Returns resampled interleaved F32.
    pub fn process(&mut self, interleaved: &[f32]) -> Vec<f32> {
        self.pending.extend_from_slice(interleaved);

        let samples_per_chunk = self.chunk_size * self.channels;
        let chunks = self.pending.len() / samples_per_chunk;
        let mut output = Vec::with_capacity(chunks * self.scratch.len());
        let mut consumed = 0;
        while self.pending.len() - consumed >= samples_per_chunk {
            let input = InterleavedSlice::new(
                &self.pending[consumed..consumed + samples_per_chunk],
                self.channels,
                self.chunk_size,
            )
            .expect("resampler input dimensions");
            let frames = self.scratch.len() / self.channels;
            let mut destination = InterleavedSlice::new_mut(&mut self.scratch, self.channels, frames)
                .expect("resampler output dimensions");
            if let Ok((_, written)) = self.resampler.process_into_buffer(&input, &mut destination, None) {
                output.extend_from_slice(&self.scratch[..written * self.channels]);
            }
            consumed += samples_per_chunk;
        }
        // Move at most one partial chunk once per call, rather than front-draining
        // and allocating/deinterleaving each complete chunk.
        self.pending.copy_within(consumed.., 0);
        self.pending.truncate(self.pending.len() - consumed);

        output
    }
}

/// Mix down multi-channel F32 audio to fewer channels.
/// Uses ITU-R BS.775 downmix coefficients for 5.1 and 7.1.
pub fn mixdown(input: &[f32], in_channels: usize, out_channels: usize) -> Vec<f32> {
    if in_channels == out_channels {
        return input.to_vec();
    }
    if out_channels == 1 && in_channels > 0 {
        return input
            .chunks_exact(in_channels)
            .map(|frame| (frame.iter().sum::<f32>() / in_channels as f32).clamp(-1.0, 1.0))
            .collect();
    }
    if out_channels != 2 {
        return input.to_vec();
    }

    let frames = input.len() / in_channels;
    let mut output = Vec::with_capacity(frames * 2);
    let k: f32 = 0.707; // -3dB

    for frame in input.chunks_exact(in_channels) {
        let (l, r) = match in_channels {
            6 => {
                let fl = frame[0];
                let fr = frame[1];
                let fc = frame[2];
                let rl = frame[4];
                let rr = frame[5];
                (fl + k * fc + k * rl, fr + k * fc + k * rr)
            }
            8 => {
                let fl = frame[0];
                let fr = frame[1];
                let fc = frame[2];
                let sl = frame[4];
                let sr = frame[5];
                let rl = frame[6];
                let rr = frame[7];
                (fl + k * fc + k * sl + k * rl, fr + k * fc + k * sr + k * rr)
            }
            _ => (frame[0], frame.get(1).copied().unwrap_or(frame[0])),
        };
        output.push(l.clamp(-1.0, 1.0));
        output.push(r.clamp(-1.0, 1.0));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resample_small_chunks() {
        let mut rs = StreamResampler::new(44100, 96000, 2).unwrap();
        let mut total_out = 0;
        // Feed 10 chunks of 352 frames (typical ALAC)
        for _ in 0..10 {
            let mut input = Vec::new();
            for i in 0..352 {
                let t = i as f32 / 44100.0;
                let s = (2.0 * std::f32::consts::PI * 440.0 * t).sin() * 0.5;
                input.push(s);
                input.push(s);
            }
            let output = rs.process(&input);
            total_out += output.len();
        }
        eprintln!("Total output samples from 10x352 frames: {total_out}");
        assert!(total_out > 0, "no output produced");
    }

    #[test]
    fn resample_passthrough_returns_none() {
        assert!(StreamResampler::new(44100, 44100, 2).is_none());
    }
}

#[cfg(test)]
mod channel_tests {
    #[test]
    fn stereo_downmix_has_one_sample_per_frame() {
        assert_eq!(super::mixdown(&[0.25, 0.75, -0.5, 0.5], 2, 1), vec![0.5, 0.0]);
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    #[test]
    fn scratch_resampling_preserves_channels_across_input_chunk_boundaries() {
        let input: Vec<f32> = (0..4096).flat_map(|frame| [(frame as f32 * 0.01).sin(), 0.0]).collect();
        let expected = StreamResampler::new(44100, 48000, 2).unwrap().process(&input);
        let mut resampler = StreamResampler::new(44100, 48000, 2).unwrap();
        let mut actual = Vec::new();
        for chunk in input.chunks(352 * 2) {
            actual.extend(resampler.process(chunk));
        }
        assert_eq!(actual, expected);
        assert!(actual.chunks_exact(2).all(|frame| frame[1] == 0.0));
        assert!(actual.chunks_exact(2).any(|frame| frame[0].abs() > 0.1));
    }
}
