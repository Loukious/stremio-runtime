use bytes::{Bytes, BytesMut};

#[derive(Debug)]
pub(super) struct Fragment {
    pub init: Bytes,
    pub media: Bytes,
}

type BoxHeader = (usize, usize, [u8; 4]);

fn boxes(bytes: &[u8]) -> Result<Vec<BoxHeader>, &'static str> {
    let mut offset = 0;
    let mut result = Vec::new();
    while offset < bytes.len() {
        if bytes.len() - offset < 8 {
            return Err("Truncated MP4 box");
        }
        let size = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let kind = bytes[offset + 4..offset + 8].try_into().unwrap();
        let length = match size {
            0 => bytes.len() - offset,
            1 => {
                if bytes.len() - offset < 16 {
                    return Err("Truncated extended MP4 box");
                }
                usize::try_from(u64::from_be_bytes(
                    bytes[offset + 8..offset + 16].try_into().unwrap(),
                ))
                .map_err(|_| "MP4 box is too large")?
            }
            _ => size as usize,
        };
        if length < if size == 1 { 16 } else { 8 } || length > bytes.len() - offset {
            return Err("Invalid MP4 box size");
        }
        result.push((offset, length, kind));
        offset += length;
    }
    Ok(result)
}

fn find_timescale(bytes: &[u8]) -> Result<u32, &'static str> {
    for (offset, length, kind) in boxes(bytes)? {
        let body = &bytes[offset + 8..offset + length];
        if kind == *b"mdhd" {
            let pos = if body.first() == Some(&1) { 20 } else { 12 };
            if body.len() < pos + 4 {
                return Err("Truncated media header");
            }
            let timescale = u32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
            return (timescale > 0)
                .then_some(timescale)
                .ok_or("Invalid media timescale");
        }
        if matches!(&kind, b"moov" | b"trak" | b"mdia")
            && let Ok(timescale) = find_timescale(body)
        {
            return Ok(timescale);
        }
    }
    Err("Media timescale is missing")
}

fn find_edit(bytes: &[u8]) -> Result<Option<i64>, &'static str> {
    for (offset, length, kind) in boxes(bytes)? {
        let body = &bytes[offset + 8..offset + length];
        if kind == *b"elst" {
            if body.len() < 8 {
                return Err("Truncated edit list");
            }
            if u32::from_be_bytes(body[4..8].try_into().unwrap()) != 1 {
                return Err("Unsupported edit list");
            }
            let edit = if body[0] == 1 {
                if body.len() < 28 {
                    return Err("Truncated edit list");
                }
                i64::from_be_bytes(body[16..24].try_into().unwrap())
            } else {
                if body.len() < 20 {
                    return Err("Truncated edit list");
                }
                i64::from(i32::from_be_bytes(body[12..16].try_into().unwrap()))
            };
            return Ok(Some(edit));
        }
        if matches!(&kind, b"moov" | b"trak" | b"edts")
            && let Some(edit) = find_edit(body)?
        {
            return Ok(Some(edit));
        }
    }
    Ok(None)
}

pub(super) fn edit_offset(bytes: &[u8]) -> Result<f64, &'static str> {
    Ok(find_edit(bytes)?.unwrap_or(0) as f64 / f64::from(find_timescale(bytes)?))
}

pub(super) fn independent_video_start(bytes: &[u8], codec: &str) -> bool {
    let configuration = if codec == "h264" { b"avcC" } else { b"hvcC" };
    let Some(config) = bytes.windows(4).position(|window| window == configuration) else {
        return false;
    };
    let Some(length_byte) = bytes.get(config + if codec == "h264" { 8 } else { 25 }) else {
        return false;
    };
    let length_size = usize::from((length_byte & 3) + 1);
    let Ok(top) = boxes(bytes) else {
        return false;
    };
    let Some((offset, length, _)) = top.into_iter().find(|(_, _, kind)| kind == b"mdat") else {
        return false;
    };
    let payload = &bytes[offset + 8..offset + length];
    let mut position = 0;
    while position + length_size < payload.len() {
        let length = payload[position..position + length_size]
            .iter()
            .fold(0usize, |length, byte| length * 256 + usize::from(*byte));
        position += length_size;
        if length == 0 || length > payload.len() - position {
            return false;
        }
        let kind = if codec == "h264" {
            payload[position] & 31
        } else {
            (payload[position] >> 1) & 63
        };
        if codec == "h264" && (1..=5).contains(&kind) {
            return kind == 5;
        }
        if codec == "hevc" && kind <= 31 {
            return matches!(kind, 19 | 20);
        }
        position += length;
    }
    false
}

fn relocate_decode_times(
    bytes: &mut [u8],
    target: u64,
    first: &mut Option<u64>,
) -> Result<(), &'static str> {
    for (offset, length, kind) in boxes(bytes)? {
        let body = &mut bytes[offset + 8..offset + length];
        if kind == *b"tfdt" {
            if body.len() < 8 {
                return Err("Truncated decode timestamp");
            }
            let original = if body[0] == 1 {
                if body.len() < 12 {
                    return Err("Truncated decode timestamp");
                }
                u64::from_be_bytes(body[4..12].try_into().unwrap())
            } else {
                u32::from_be_bytes(body[4..8].try_into().unwrap()) as u64
            };
            let base = *first.get_or_insert(original);
            let relocated = target
                .checked_add(original.saturating_sub(base))
                .ok_or("Decode timestamp overflow")?;
            if body[0] == 1 {
                body[4..12].copy_from_slice(&relocated.to_be_bytes());
            } else {
                let relocated =
                    u32::try_from(relocated).map_err(|_| "Decode timestamp overflow")?;
                body[4..8].copy_from_slice(&relocated.to_be_bytes());
            }
        } else if matches!(&kind, b"moof" | b"traf") {
            relocate_decode_times(body, target, first)?;
        }
    }
    Ok(())
}

pub(super) fn split(output: Bytes, start_seconds: f64) -> Result<Fragment, &'static str> {
    let mut init = BytesMut::new();
    let mut media = BytesMut::new();
    let mut has_fragment = false;
    for (offset, length, kind) in boxes(&output)? {
        match &kind {
            b"ftyp" | b"moov" => init.extend_from_slice(&output[offset..offset + length]),
            b"moof" | b"mdat" | b"styp" => {
                media.extend_from_slice(&output[offset..offset + length]);
                has_fragment |= kind == *b"moof";
            }
            _ => {}
        }
    }
    if init.is_empty() || !has_fragment {
        return Err("FFmpeg did not produce fragmented MP4");
    }
    let timescale = find_timescale(&init)?;
    if !start_seconds.is_finite() || start_seconds < 0.0 {
        return Err("Invalid segment timestamp");
    }
    let target = (start_seconds * f64::from(timescale)).round() as u64;
    relocate_decode_times(&mut media, target, &mut None)?;
    Ok(Fragment {
        init: init.freeze(),
        media: media.freeze(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boxed(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
        [(body.len() as u32 + 8).to_be_bytes().as_slice(), kind, body].concat()
    }

    #[test]
    fn relocating_fragments_uses_track_timebase_and_preserves_relative_timing() {
        let mut mdhd = vec![0u8; 20];
        mdhd[12..16].copy_from_slice(&48000u32.to_be_bytes());
        let init = boxed(
            b"moov",
            &boxed(b"trak", &boxed(b"mdia", &boxed(b"mdhd", &mdhd))),
        );
        let fragment = |time: u64| {
            boxed(
                b"moof",
                &boxed(
                    b"traf",
                    &boxed(
                        b"tfdt",
                        &[&[1u8, 0, 0, 0][..], &time.to_be_bytes()].concat(),
                    ),
                ),
            )
        };
        let output = [
            init.as_slice(),
            fragment(1024).as_slice(),
            boxed(b"mdat", &[1, 2]).as_slice(),
            fragment(2048).as_slice(),
        ]
        .concat();
        let result = split(Bytes::from(output), 8.0).unwrap();
        assert_eq!(result.init.as_ref(), init);
        let mut times = Vec::new();
        for offset in 0..=result.media.len().saturating_sub(16) {
            if &result.media[offset..offset + 4] == b"tfdt" {
                times.push(u64::from_be_bytes(
                    result.media[offset + 8..offset + 16].try_into().unwrap(),
                ));
            }
        }
        assert_eq!(times, vec![384000, 385024]);
    }

    #[test]
    fn rejects_incomplete_or_unfragmented_mp4() {
        assert!(split(Bytes::from_static(b"short"), 0.0).is_err());
        assert!(split(Bytes::from(boxed(b"moov", &[])), 0.0).is_err());
    }
}
