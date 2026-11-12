//! Read MP4 timing tables and Matroska cue points using bounded HTTP ranges.
//! Media payloads are skipped; this never downloads the video to find keyframes.
use std::collections::{BTreeMap, BTreeSet};

use futures_util::StreamExt;
use tokio::sync::Semaphore;
use url::Url;

use super::{
    ProbeError,
    hls::{HlsProbe, HlsSample, HlsSamples},
};

const METADATA_LIMIT: usize = 32 * 1024 * 1024;
const TRANSFER_LIMIT: usize = 64 * 1024 * 1024;
const SAMPLE_LIMIT: usize = 1_000_000;
const REQUEST_LIMIT: usize = 256;
static SAMPLE_SLOTS: Semaphore = Semaphore::const_new(2);

pub(crate) async fn probe_samples(
    url: &Url,
    probe: &HlsProbe,
) -> Result<BTreeMap<String, HlsSamples>, ProbeError> {
    let _permit = SAMPLE_SLOTS.try_acquire().map_err(|_| ProbeError::Busy)?;
    let mut source = RangeSource::new(url.clone())?;
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        if probe
            .format
            .name
            .split(',')
            .any(|name| matches!(name, "mp4" | "mov"))
        {
            let moov = source.moov().await?;
            mp4_samples(&moov, probe)
        } else if probe
            .format
            .name
            .split(',')
            .any(|name| matches!(name, "matroska" | "webm"))
        {
            let elements = source.matroska_metadata().await?;
            matroska_samples(&elements, probe)
        } else {
            Ok(BTreeMap::new())
        }
    })
    .await
    .map_err(|_| ProbeError::Timeout)?
}

struct RangeSource {
    url: Url,
    client: reqwest::Client,
    total: Option<u64>,
    transferred: usize,
    requests: usize,
}

impl RangeSource {
    fn new(url: Url) -> Result<Self, ProbeError> {
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(20))
            .redirect(reqwest::redirect::Policy::limited(5))
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .build()
            .map_err(|_| ProbeError::SamplesUnavailable)?;
        Ok(Self {
            url,
            client,
            total: None,
            transferred: 0,
            requests: 0,
        })
    }

    async fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, ProbeError> {
        if length == 0 {
            return Ok(Vec::new());
        }
        if length > METADATA_LIMIT || self.requests >= REQUEST_LIMIT {
            return Err(ProbeError::OutputTooLarge);
        }
        let end = offset
            .checked_add(length as u64 - 1)
            .ok_or(ProbeError::SamplesUnavailable)?;
        self.requests += 1;
        let response = self
            .client
            .get(self.url.clone())
            .header(reqwest::header::RANGE, format!("bytes={offset}-{end}"))
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    ProbeError::Timeout
                } else {
                    ProbeError::SamplesUnavailable
                }
            })?;
        let skip = if response.status() == reqwest::StatusCode::PARTIAL_CONTENT {
            let header = response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .ok_or(ProbeError::SamplesUnavailable)?;
            let (range, total) = header
                .strip_prefix("bytes ")
                .and_then(|value| value.split_once('/'))
                .ok_or(ProbeError::SamplesUnavailable)?;
            let (start, finish) = range
                .split_once('-')
                .ok_or(ProbeError::SamplesUnavailable)?;
            if start.parse::<u64>().ok() != Some(offset)
                || finish
                    .parse::<u64>()
                    .ok()
                    .is_none_or(|finish| finish < offset || finish > end)
            {
                return Err(ProbeError::SamplesUnavailable);
            }
            self.total = total.parse().ok();
            0
        } else if response.status() == reqwest::StatusCode::OK {
            self.total = response.content_length();
            // Origins without Range support are usable only for bounded front metadata.
            usize::try_from(offset)
                .ok()
                .filter(|offset| offset.saturating_add(length) <= METADATA_LIMIT)
                .ok_or(ProbeError::SamplesUnavailable)?
        } else {
            return Err(ProbeError::SamplesUnavailable);
        };
        let wanted = skip.checked_add(length).ok_or(ProbeError::OutputTooLarge)?;
        let mut output = Vec::new();
        let mut body = response.bytes_stream();
        while output.len() < wanted {
            let Some(chunk) = body.next().await else {
                break;
            };
            let chunk = chunk.map_err(|error| {
                if error.is_timeout() {
                    ProbeError::Timeout
                } else {
                    ProbeError::SamplesUnavailable
                }
            })?;
            self.transferred = self
                .transferred
                .checked_add(chunk.len())
                .ok_or(ProbeError::OutputTooLarge)?;
            if self.transferred > TRANSFER_LIMIT {
                return Err(ProbeError::OutputTooLarge);
            }
            let take = (wanted - output.len()).min(chunk.len());
            output.extend_from_slice(&chunk[..take]);
        }
        if output.len() < wanted {
            return Err(ProbeError::SamplesUnavailable);
        }
        Ok(output[skip..].to_vec())
    }

    async fn moov(&mut self) -> Result<Vec<u8>, ProbeError> {
        let mut offset = 0u64;
        for _ in 0..REQUEST_LIMIT {
            let header = self.read(offset, 16).await?;
            let short_size = u32_at(&header, 0)?;
            let header_size = if short_size == 1 { 16 } else { 8 };
            let size = if short_size == 1 {
                u64_at(&header, 8)?
            } else if short_size == 0 {
                self.total
                    .and_then(|total| total.checked_sub(offset))
                    .ok_or(ProbeError::SamplesUnavailable)?
            } else {
                u64::from(short_size)
            };
            let end = offset
                .checked_add(size)
                .ok_or(ProbeError::SamplesUnavailable)?;
            if size < header_size || self.total.is_some_and(|total| end > total) {
                return Err(ProbeError::SamplesUnavailable);
            }
            if &header[4..8] == b"moov" {
                let length =
                    usize::try_from(size - header_size).map_err(|_| ProbeError::OutputTooLarge)?;
                return self.read(offset + header_size, length).await;
            }
            offset = end;
            if self.total.is_some_and(|total| offset >= total) {
                break;
            }
        }
        Err(ProbeError::SamplesUnavailable)
    }

    async fn ebml_header(&mut self, offset: u64) -> Result<(u64, u64, Option<u64>), ProbeError> {
        let length = self
            .total
            .and_then(|total| total.checked_sub(offset))
            .map(|remaining| remaining.min(16) as usize)
            .unwrap_or(16);
        let bytes = self.read(offset, length).await?;
        let (id, id_length, _) = vint(&bytes, 0, true)?;
        let (size, size_length, unknown) = vint(&bytes, id_length, false)?;
        let payload = offset
            .checked_add((id_length + size_length) as u64)
            .ok_or(ProbeError::SamplesUnavailable)?;
        Ok((id, payload, (!unknown).then_some(size)))
    }

    async fn matroska_metadata(&mut self) -> Result<BTreeMap<u64, Vec<u8>>, ProbeError> {
        let (id, payload, size) = self.ebml_header(0).await?;
        if id != 0x1a45dfa3 {
            return Err(ProbeError::SamplesUnavailable);
        }
        let segment_offset = payload
            .checked_add(size.ok_or(ProbeError::SamplesUnavailable)?)
            .ok_or(ProbeError::SamplesUnavailable)?;
        let (id, segment_start, segment_size) = self.ebml_header(segment_offset).await?;
        if id != 0x18538067 {
            return Err(ProbeError::SamplesUnavailable);
        }
        let segment_end = segment_size
            .and_then(|size| segment_start.checked_add(size))
            .or(self.total);
        let mut offset = segment_start;
        let mut found = BTreeMap::new();
        let mut sought = BTreeMap::new();
        while !segment_end.is_some_and(|end| offset >= end) && self.requests < REQUEST_LIMIT {
            let (id, payload, size) = self.ebml_header(offset).await?;
            if [0x114d9b74, 0x1549a966, 0x1654ae6b, 0x1c53bb6b].contains(&id) {
                let length = usize::try_from(size.ok_or(ProbeError::SamplesUnavailable)?)
                    .map_err(|_| ProbeError::OutputTooLarge)?;
                let data = self.read(payload, length).await?;
                if id == 0x114d9b74 {
                    for (_, seek) in elements(&data)?.into_iter().filter(|(id, _)| *id == 0x4dbb) {
                        let seek = elements(seek)?;
                        let id = seek
                            .iter()
                            .find(|(id, _)| *id == 0x53ab)
                            .and_then(|(_, bytes)| uint(bytes).ok());
                        let position = seek
                            .iter()
                            .find(|(id, _)| *id == 0x53ac)
                            .and_then(|(_, bytes)| uint(bytes).ok());
                        if let Some((id, position)) = id.zip(position) {
                            sought.insert(
                                id,
                                segment_start
                                    .checked_add(position)
                                    .ok_or(ProbeError::SamplesUnavailable)?,
                            );
                        }
                    }
                } else {
                    found.insert(id, data);
                }
            }
            // SeekHead points directly to cues near EOF, avoiding reads through clusters.
            for id in [0x1549a966, 0x1654ae6b, 0x1c53bb6b] {
                if found.contains_key(&id) {
                    continue;
                }
                if let Some(&position) = sought.get(&id) {
                    let (actual, payload, size) = self.ebml_header(position).await?;
                    if actual != id {
                        return Err(ProbeError::SamplesUnavailable);
                    }
                    let length = usize::try_from(size.ok_or(ProbeError::SamplesUnavailable)?)
                        .map_err(|_| ProbeError::OutputTooLarge)?;
                    found.insert(id, self.read(payload, length).await?);
                }
            }
            if found.contains_key(&0x1549a966)
                && found.contains_key(&0x1654ae6b)
                && found.contains_key(&0x1c53bb6b)
            {
                break;
            }
            offset = match size.and_then(|size| payload.checked_add(size)) {
                Some(end) if end > offset => end,
                _ => return Err(ProbeError::SamplesUnavailable),
            };
        }
        if !found.contains_key(&0x1549a966) || !found.contains_key(&0x1654ae6b) {
            return Err(ProbeError::SamplesUnavailable);
        }
        if self.requests >= REQUEST_LIMIT {
            return Err(ProbeError::OutputTooLarge);
        }
        Ok(found)
    }
}

fn u32_at(data: &[u8], at: usize) -> Result<u32, ProbeError> {
    Ok(u32::from_be_bytes(
        data.get(at..at + 4)
            .ok_or(ProbeError::SamplesUnavailable)?
            .try_into()
            .map_err(|_| ProbeError::SamplesUnavailable)?,
    ))
}
fn u64_at(data: &[u8], at: usize) -> Result<u64, ProbeError> {
    Ok(u64::from_be_bytes(
        data.get(at..at + 8)
            .ok_or(ProbeError::SamplesUnavailable)?
            .try_into()
            .map_err(|_| ProbeError::SamplesUnavailable)?,
    ))
}

type Mp4Boxes<'a> = Vec<(&'a [u8], &'a [u8])>;

fn boxes(data: &[u8]) -> Result<Mp4Boxes<'_>, ProbeError> {
    let mut result = Vec::new();
    let mut at = 0usize;
    while at < data.len() {
        let size = u32_at(data, at)?;
        let header = if size == 1 { 16 } else { 8 };
        let size = match size {
            0 => data.len() - at,
            1 => usize::try_from(u64_at(data, at + 8)?).map_err(|_| ProbeError::OutputTooLarge)?,
            value => value as usize,
        };
        let end = at
            .checked_add(size)
            .filter(|end| *end <= data.len())
            .ok_or(ProbeError::SamplesUnavailable)?;
        if size < header {
            return Err(ProbeError::SamplesUnavailable);
        }
        result.push((
            data.get(at + 4..at + 8)
                .ok_or(ProbeError::SamplesUnavailable)?,
            &data[at + header..end],
        ));
        at = end;
    }
    Ok(result)
}

fn child<'a>(data: &'a [u8], name: &[u8]) -> Result<&'a [u8], ProbeError> {
    boxes(data)?
        .into_iter()
        .find(|(kind, _)| *kind == name)
        .map(|(_, value)| value)
        .ok_or(ProbeError::SamplesUnavailable)
}

fn mp4_samples(moov: &[u8], probe: &HlsProbe) -> Result<BTreeMap<String, HlsSamples>, ProbeError> {
    let mvhd = child(moov, b"mvhd")?;
    let movie_timescale = u32_at(mvhd, if mvhd.first() == Some(&1) { 20 } else { 12 })?;
    if movie_timescale == 0 {
        return Err(ProbeError::SamplesUnavailable);
    }
    let mut result = BTreeMap::new();
    let mut sample_total = 0usize;
    for (index, (_, trak)) in boxes(moov)?
        .into_iter()
        .filter(|(kind, _)| *kind == b"trak")
        .enumerate()
    {
        let mdia = child(trak, b"mdia")?;
        let hdlr = child(mdia, b"hdlr")?;
        let kind = match hdlr.get(8..12) {
            Some(b"vide") => "video",
            Some(b"soun") => "audio",
            _ => continue,
        };
        if !probe
            .streams
            .iter()
            .any(|stream| stream.index == index as i64 && stream.track == kind)
        {
            continue;
        }
        let mdhd = child(mdia, b"mdhd")?;
        let timescale = u32_at(mdhd, if mdhd.first() == Some(&1) { 20 } else { 12 })?;
        if timescale == 0 {
            return Err(ProbeError::SamplesUnavailable);
        }
        let stbl = child(child(mdia, b"minf")?, b"stbl")?;
        let tables = boxes(stbl)?;
        let stts = tables
            .iter()
            .find(|(kind, _)| *kind == b"stts")
            .map(|(_, value)| *value)
            .ok_or(ProbeError::SamplesUnavailable)?;
        let mut durations = Vec::new();
        let entries = u32_at(stts, 4)? as usize;
        if entries > stts.len().saturating_sub(8) / 8 {
            return Err(ProbeError::SamplesUnavailable);
        }
        for entry in 0..entries {
            let count = u32_at(stts, 8 + entry * 8)? as usize;
            let duration = u32_at(stts, 12 + entry * 8)?;
            sample_total = sample_total
                .checked_add(count)
                .ok_or(ProbeError::OutputTooLarge)?;
            if sample_total > SAMPLE_LIMIT {
                return Err(ProbeError::OutputTooLarge);
            }
            durations.extend(std::iter::repeat_n(duration, count));
        }
        let mut composition = Vec::new();
        if let Some((_, ctts)) = tables.iter().find(|(kind, _)| *kind == b"ctts") {
            let entries = u32_at(ctts, 4)? as usize;
            if entries > ctts.len().saturating_sub(8) / 8 {
                return Err(ProbeError::SamplesUnavailable);
            }
            for entry in 0..entries {
                let count = u32_at(ctts, 8 + entry * 8)? as usize;
                if composition.len().saturating_add(count) > durations.len() {
                    return Err(ProbeError::SamplesUnavailable);
                }
                let raw = u32_at(ctts, 12 + entry * 8)?;
                let offset = if ctts.first() == Some(&1) {
                    i64::from(raw as i32)
                } else {
                    i64::from(raw)
                };
                composition.extend(std::iter::repeat_n(offset, count));
            }
        }
        let sync = if let Some((_, stss)) = tables.iter().find(|(kind, _)| *kind == b"stss") {
            let entries = u32_at(stss, 4)? as usize;
            if entries > stss.len().saturating_sub(8) / 4 {
                return Err(ProbeError::SamplesUnavailable);
            }
            let mut sync = BTreeSet::new();
            for entry in 0..entries {
                sync.insert(u32_at(stss, 8 + entry * 4)? as usize);
            }
            Some(sync)
        } else {
            None
        };
        let mut dts = 0i64;
        if let Some((_, edts)) = boxes(trak)?.into_iter().find(|(kind, _)| *kind == b"edts")
            && let Some((_, elst)) = boxes(edts)?.into_iter().find(|(kind, _)| *kind == b"elst")
        {
            let version = elst
                .first()
                .copied()
                .ok_or(ProbeError::SamplesUnavailable)?;
            let width = if version == 1 { 20 } else { 12 };
            let entries = u32_at(elst, 4)? as usize;
            if entries > elst.len().saturating_sub(8) / width {
                return Err(ProbeError::SamplesUnavailable);
            }
            for entry in 0..entries {
                let at = 8 + entry * width;
                let segment = if version == 1 {
                    u64_at(elst, at)?
                } else {
                    u64::from(u32_at(elst, at)?)
                };
                let media_time = if version == 1 {
                    u64_at(elst, at + 8)? as i64
                } else {
                    i64::from(u32_at(elst, at + 4)? as i32)
                };
                let delta = if media_time >= 0 {
                    -i128::from(media_time)
                } else if media_time == -1 {
                    (i128::from(segment) * i128::from(timescale) + i128::from(movie_timescale) / 2)
                        / i128::from(movie_timescale)
                } else {
                    0
                };
                dts = i64::try_from(i128::from(dts) + delta)
                    .map_err(|_| ProbeError::SamplesUnavailable)?;
            }
        }
        let mut samples = Vec::with_capacity(durations.len());
        let mut sum = 0u64;
        for (number, duration) in durations.into_iter().enumerate() {
            samples.push(HlsSample {
                key: sync
                    .as_ref()
                    .is_none_or(|sync| sync.contains(&(number + 1))),
                pts: dts
                    .checked_add(composition.get(number).copied().unwrap_or(0))
                    .ok_or(ProbeError::SamplesUnavailable)?,
                dts: Some(dts),
                duration: Some(u64::from(duration)),
            });
            dts = dts
                .checked_add(i64::from(duration))
                .ok_or(ProbeError::SamplesUnavailable)?;
            sum = sum
                .checked_add(u64::from(duration))
                .ok_or(ProbeError::SamplesUnavailable)?;
        }
        let sample_duration = (!samples.is_empty()).then(|| sum as f64 / samples.len() as f64);
        result.insert(
            index.to_string(),
            HlsSamples {
                kind: kind.into(),
                timescale: f64::from(timescale),
                sample_duration,
                samples,
            },
        );
    }
    Ok(result)
}

fn vint(data: &[u8], at: usize, retain_marker: bool) -> Result<(u64, usize, bool), ProbeError> {
    let first = *data.get(at).ok_or(ProbeError::SamplesUnavailable)?;
    let width = first.leading_zeros() as usize + 1;
    if width > 8 || (retain_marker && width > 4) {
        return Err(ProbeError::SamplesUnavailable);
    }
    let bytes = data
        .get(at..at + width)
        .ok_or(ProbeError::SamplesUnavailable)?;
    let mut value = u64::from(if retain_marker {
        first
    } else {
        first & ((0xffu16 >> width) as u8)
    });
    for &byte in &bytes[1..] {
        value = (value << 8) | u64::from(byte);
    }
    Ok((
        value,
        width,
        !retain_marker && value == (1u64 << (width * 7)) - 1,
    ))
}

fn uint(data: &[u8]) -> Result<u64, ProbeError> {
    if data.is_empty() || data.len() > 8 {
        return Err(ProbeError::SamplesUnavailable);
    }
    Ok(data
        .iter()
        .fold(0u64, |value, byte| (value << 8) | u64::from(*byte)))
}

fn elements(data: &[u8]) -> Result<Vec<(u64, &[u8])>, ProbeError> {
    let mut at = 0usize;
    let mut result = Vec::new();
    while at < data.len() {
        let (id, id_length, _) = vint(data, at, true)?;
        let (size, size_length, unknown) = vint(data, at + id_length, false)?;
        if unknown {
            return Err(ProbeError::SamplesUnavailable);
        }
        let start = at + id_length + size_length;
        let size = usize::try_from(size).map_err(|_| ProbeError::OutputTooLarge)?;
        let end = start
            .checked_add(size)
            .filter(|end| *end <= data.len())
            .ok_or(ProbeError::SamplesUnavailable)?;
        result.push((id, &data[start..end]));
        at = end;
    }
    Ok(result)
}

fn matroska_samples(
    metadata: &BTreeMap<u64, Vec<u8>>,
    probe: &HlsProbe,
) -> Result<BTreeMap<String, HlsSamples>, ProbeError> {
    let info = elements(
        metadata
            .get(&0x1549a966)
            .ok_or(ProbeError::SamplesUnavailable)?,
    )?;
    let scale = info
        .iter()
        .find(|(id, _)| *id == 0x2ad7b1)
        .map(|(_, bytes)| uint(bytes))
        .transpose()?
        .unwrap_or(1_000_000);
    if scale == 0 {
        return Err(ProbeError::SamplesUnavailable);
    }
    let tracks = elements(
        metadata
            .get(&0x1654ae6b)
            .ok_or(ProbeError::SamplesUnavailable)?,
    )?;
    let cues = metadata
        .get(&0x1c53bb6b)
        .map(|data| elements(data))
        .transpose()?
        .unwrap_or_default();
    let mut cue_points = BTreeMap::<u64, Vec<i64>>::new();
    let mut count = 0usize;
    for (_, point) in cues.into_iter().filter(|(id, _)| *id == 0xbb) {
        let point = elements(point)?;
        let time = point
            .iter()
            .find(|(id, _)| *id == 0xb3)
            .ok_or(ProbeError::SamplesUnavailable)?;
        let time = i64::try_from(uint(time.1)?).map_err(|_| ProbeError::SamplesUnavailable)?;
        for (_, positions) in point.iter().filter(|(id, _)| *id == 0xb7) {
            for (_, track) in elements(positions)?
                .into_iter()
                .filter(|(id, _)| *id == 0xf7)
            {
                count += 1;
                if count > SAMPLE_LIMIT {
                    return Err(ProbeError::OutputTooLarge);
                }
                cue_points.entry(uint(track)?).or_default().push(time);
            }
        }
    }
    let mut result = BTreeMap::new();
    for (index, (_, entry)) in tracks.into_iter().filter(|(id, _)| *id == 0xae).enumerate() {
        let entry = elements(entry)?;
        let track = entry
            .iter()
            .find(|(id, _)| *id == 0xd7)
            .ok_or(ProbeError::SamplesUnavailable)?;
        let number = uint(track.1)?;
        let track_type = entry
            .iter()
            .find(|(id, _)| *id == 0x83)
            .map(|(_, bytes)| uint(bytes))
            .transpose()?
            .unwrap_or(0);
        let kind = match track_type {
            1 => "video",
            2 => "audio",
            _ => continue,
        };
        if !probe
            .streams
            .iter()
            .any(|stream| stream.index == index as i64 && stream.track == kind)
        {
            continue;
        }
        let sample_duration = entry
            .iter()
            .find(|(id, _)| *id == 0x23e383)
            .map(|(_, bytes)| uint(bytes).map(|value| value as f64 / scale as f64))
            .transpose()?;
        let mut points = cue_points.remove(&number).unwrap_or_default();
        points.sort_unstable();
        let samples = points
            .into_iter()
            .map(|pts| HlsSample {
                key: true,
                pts,
                dts: None,
                duration: None,
            })
            .collect();
        result.insert(
            index.to_string(),
            HlsSamples {
                kind: kind.into(),
                timescale: 1_000_000_000.0 / scale as f64,
                sample_duration,
                samples,
            },
        );
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::super::hls::{HlsFormat, HlsStream};
    use super::*;

    fn atom(kind: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut result = ((payload.len() + 8) as u32).to_be_bytes().to_vec();
        result.extend_from_slice(kind);
        result.extend_from_slice(payload);
        result
    }
    fn table(kind: &[u8; 4], version: u8, entries: &[u32], width: usize) -> Vec<u8> {
        let mut payload = vec![version, 0, 0, 0];
        payload.extend_from_slice(&((entries.len() / width) as u32).to_be_bytes());
        for entry in entries {
            payload.extend_from_slice(&entry.to_be_bytes());
        }
        atom(kind, &payload)
    }
    fn probe(kind: &str) -> HlsProbe {
        HlsProbe {
            format: HlsFormat {
                name: "mp4".into(),
                duration: Some(1.0),
            },
            streams: vec![HlsStream {
                id: 0,
                index: 0,
                track: kind.into(),
                codec: "h264".into(),
                stream_bit_rate: Some(0),
                stream_max_bit_rate: Some(0),
                start_time: None,
                start_time_ts: None,
                timescale: 1,
                extra: Default::default(),
            }],
            samples: BTreeMap::new(),
        }
    }

    #[test]
    fn mp4_keyframes_composition_and_edits_preserve_original_timestamps() {
        let mut mvhd = vec![0; 20];
        mvhd[12..16].copy_from_slice(&1000u32.to_be_bytes());
        let mut mdhd = vec![0; 20];
        mdhd[12..16].copy_from_slice(&1000u32.to_be_bytes());
        let mut hdlr = vec![0; 12];
        hdlr[8..12].copy_from_slice(b"vide");
        let stbl = atom(
            b"stbl",
            &[
                table(b"stts", 0, &[3, 40], 2),
                table(b"ctts", 1, &[1, 80, 2, (-40i32) as u32], 2),
                table(b"stss", 0, &[1, 3], 1),
            ]
            .concat(),
        );
        let mdia = atom(
            b"mdia",
            &[
                atom(b"mdhd", &mdhd),
                atom(b"hdlr", &hdlr),
                atom(b"minf", &stbl),
            ]
            .concat(),
        );
        let edts = atom(b"edts", &table(b"elst", 0, &[120, 40, 0x10000], 3));
        let moov = [atom(b"mvhd", &mvhd), atom(b"trak", &[edts, mdia].concat())].concat();
        let result = mp4_samples(&moov, &probe("video")).unwrap();
        let track = &result["0"];
        assert_eq!(track.timescale, 1000.0);
        assert_eq!(track.sample_duration, Some(40.0));
        assert_eq!(
            track
                .samples
                .iter()
                .map(|sample| (sample.key, sample.pts, sample.dts, sample.duration))
                .collect::<Vec<_>>(),
            [
                (true, 40, Some(-40), Some(40)),
                (false, -40, Some(0), Some(40)),
                (true, 0, Some(40), Some(40))
            ]
        );
        assert_eq!(
            mp4_samples(&moov[..moov.len() - 1], &probe("video")).unwrap_err(),
            ProbeError::SamplesUnavailable
        );
    }

    fn ebml(id: &[u8], payload: &[u8]) -> Vec<u8> {
        assert!(payload.len() < 127);
        [id, &[0x80 | payload.len() as u8], payload].concat()
    }

    #[test]
    fn matroska_cues_preserve_track_numbers_and_nanosecond_timescales() {
        let info = ebml(&[0x2a, 0xd7, 0xb1], &100_000u32.to_be_bytes());
        let track = ebml(
            &[0xae],
            &[
                ebml(&[0xd7], &[5]),
                ebml(&[0x83], &[1]),
                ebml(&[0x23, 0xe3, 0x83], &40_000_000u32.to_be_bytes()),
            ]
            .concat(),
        );
        let cues = [300u16, 100u16]
            .into_iter()
            .map(|pts| {
                ebml(
                    &[0xbb],
                    &[
                        ebml(&[0xb3], &pts.to_be_bytes()),
                        ebml(&[0xb7], &ebml(&[0xf7], &[5])),
                    ]
                    .concat(),
                )
            })
            .collect::<Vec<_>>()
            .concat();
        let metadata =
            BTreeMap::from([(0x1549a966, info), (0x1654ae6b, track), (0x1c53bb6b, cues)]);
        let result = matroska_samples(&metadata, &probe("video")).unwrap();
        assert_eq!(result["0"].timescale, 10_000.0);
        assert_eq!(result["0"].sample_duration, Some(400.0));
        assert_eq!(
            result["0"]
                .samples
                .iter()
                .map(|sample| sample.pts)
                .collect::<Vec<_>>(),
            [100, 300]
        );
        let body = serde_json::to_value(&result).unwrap();
        assert!(body["0"]["samples"][0].get("dts").is_none());
        assert_eq!(
            vint(&[0], 0, false).unwrap_err(),
            ProbeError::SamplesUnavailable
        );
        assert_eq!(
            elements(&[0xae, 0xff]).unwrap_err(),
            ProbeError::SamplesUnavailable
        );
    }

    #[tokio::test]
    async fn mp4_metadata_ranges_skip_media_beyond_four_gibibytes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://{}/large.mp4?token=local%2Bencoded",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let ftyp = atom(b"ftyp", &[0; 16]);
        let media_size = 5_000_000_000u64;
        let moov_start = ftyp.len() as u64 + media_size;
        let moov = atom(b"moov", b"metadata");
        let total = moov_start + moov.len() as u64;
        let mdat = [
            1u32.to_be_bytes().as_slice(),
            b"mdat",
            &media_size.to_be_bytes(),
        ]
        .concat();
        let serving = tokio::spawn(async move {
            let mut offsets = Vec::new();
            for _ in 0..4 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut buffer = [0; 1024];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&buffer[..count]);
                }
                let request = String::from_utf8(request).unwrap();
                assert!(request.starts_with("GET /large.mp4?token=local%2Bencoded "));
                let range = request
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("range: bytes=")
                            .map(str::to_string)
                    })
                    .unwrap();
                let (start, end) = range.split_once('-').unwrap();
                let start = start.parse::<u64>().unwrap();
                let end = end.parse::<u64>().unwrap().min(total - 1);
                offsets.push(start);
                let mut body = Vec::new();
                for position in start..=end {
                    let byte = if position < ftyp.len() as u64 {
                        ftyp[position as usize]
                    } else if position < ftyp.len() as u64 + mdat.len() as u64 {
                        mdat[(position - ftyp.len() as u64) as usize]
                    } else if position >= moov_start {
                        moov[(position - moov_start) as usize]
                    } else {
                        0
                    };
                    body.push(byte);
                }
                let headers = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end}/{total}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(headers.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
            offsets
        });
        let mut source = RangeSource::new(url).unwrap();
        assert_eq!(source.moov().await.unwrap(), b"metadata");
        assert_eq!(serving.await.unwrap(), [0, 24, moov_start, moov_start + 8]);
        assert!(source.transferred < 100);
    }
}
