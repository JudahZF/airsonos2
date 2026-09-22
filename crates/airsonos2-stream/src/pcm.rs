pub fn f32_pcm_to_s16le_bytes(samples: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    write_f32_pcm_to_s16le(samples, &mut bytes);
    bytes
}

pub fn write_f32_pcm_to_s16le(samples: &[f32], bytes: &mut Vec<u8>) {
    bytes.clear();
    bytes.reserve(samples.len() * 2);

    for sample in samples {
        let clamped = if sample.is_finite() {
            sample.clamp(-1.0, 1.0)
        } else {
            0.0
        };
        let value = if clamped == -1.0 {
            i16::MIN
        } else {
            (clamped * f32::from(i16::MAX)).round() as i16
        };

        bytes.extend_from_slice(&value.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_f32_pcm_to_s16le() {
        let bytes = f32_pcm_to_s16le_bytes(&[-1.0, -0.5, 0.0, 0.5, 1.0, f32::NAN]);
        let samples: Vec<i16> = bytes
            .chunks_exact(2)
            .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();

        assert_eq!(samples[0], i16::MIN);
        assert_eq!(samples[2], 0);
        assert_eq!(samples[4], i16::MAX);
        assert_eq!(samples[5], 0);
        assert!(samples[1] < 0);
        assert!(samples[3] > 0);
    }
}
