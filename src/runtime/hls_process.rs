use std::{ffi::OsString, process::Stdio, time::Duration};

use bytes::Bytes;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    sync::watch,
};

#[derive(Debug, Clone, Copy)]
pub(super) enum ProcessError {
    Unavailable,
    Failed,
    TooLarge,
    Timeout,
    Cancelled,
}

pub(super) fn executable() -> OsString {
    if let Some(path) = std::env::var_os("STREMIO_FFMPEG_PATH").filter(|p| !p.is_empty()) {
        return path;
    }
    let name = if cfg!(windows) {
        "ffmpeg.exe"
    } else {
        "ffmpeg"
    };
    if let Some(path) = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join(name)))
        .filter(|path| path.is_file())
    {
        return path.into_os_string();
    }
    OsString::from("ffmpeg")
}

struct ChildGuard(Option<Child>);

impl ChildGuard {
    async fn stop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        self.0.take();
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.start_kill();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}

async fn read_bounded(
    mut pipe: impl AsyncRead + Unpin,
    limit: usize,
) -> Result<Vec<u8>, ProcessError> {
    let mut output = Vec::new();
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        let count = pipe
            .read(&mut buffer)
            .await
            .map_err(|_| ProcessError::Failed)?;
        if count == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(count) > limit {
            return Err(ProcessError::TooLarge);
        }
        output.extend_from_slice(&buffer[..count]);
    }
}

/// Owns every pipe and the child until completion. Dropping this future also kills
/// and reaps the process, including when an HTTP request is cancelled.
pub(super) async fn run(
    arguments: &[String],
    output_limit: usize,
    mut cancelled: watch::Receiver<bool>,
) -> Result<Bytes, ProcessError> {
    if *cancelled.borrow() {
        return Err(ProcessError::Cancelled);
    }
    let mut command = Command::new(executable());
    if std::env::var_os("FONTCONFIG_FILE").is_none()
        && let Some(config) = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|directory| directory.join("fonts.conf")))
            .filter(|config| config.is_file())
    {
        command.env("FONTCONFIG_FILE", config);
    }
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(0x0800_0000);
    let child = command.spawn().map_err(|_| ProcessError::Unavailable)?;
    let mut guard = ChildGuard(Some(child));
    let child = guard.0.as_mut().unwrap();
    let stdout = child.stdout.take().ok_or(ProcessError::Failed)?;
    let stderr = child.stderr.take().ok_or(ProcessError::Failed)?;
    let work = async {
        let (output, _diagnostics, status) = tokio::try_join!(
            read_bounded(stdout, output_limit),
            read_bounded(stderr, 256 * 1024),
            async { child.wait().await.map_err(|_| ProcessError::Failed) },
        )?;
        if !status.success() {
            return Err(ProcessError::Failed);
        }
        Ok(Bytes::from(output))
    };
    let result = tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(120), work) =>
            result.unwrap_or(Err(ProcessError::Timeout)),
        _ = cancelled.changed() => Err(ProcessError::Cancelled),
    };
    if result.is_err() {
        guard.stop().await;
    } else {
        guard.0.take();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires FFmpeg; run with --include-ignored using packaged media tools"]
    async fn cancelling_an_active_ffmpeg_process_reaps_it_promptly() {
        let (cancel, receiver) = watch::channel(false);
        let arguments = [
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x90:rate=24",
            "-f",
            "null",
            "-",
        ]
        .map(str::to_owned);
        let work = tokio::spawn(async move { run(&arguments, 1024, receiver).await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !work.is_finished(),
            "The real FFmpeg conversion should still be running"
        );
        cancel.send_replace(true);
        let result = tokio::time::timeout(Duration::from_secs(3), work)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(ProcessError::Cancelled)));
    }

    #[tokio::test]
    #[ignore = "requires FFmpeg; run with --include-ignored using packaged media tools"]
    async fn oversized_ffmpeg_output_is_stopped_before_buffering_the_entire_media() {
        let (_cancel, receiver) = watch::channel(false);
        let arguments = [
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x90:rate=24",
            "-t",
            "10",
            "-f",
            "rawvideo",
            "pipe:1",
        ]
        .map(str::to_owned);
        let result = tokio::time::timeout(Duration::from_secs(3), run(&arguments, 1024, receiver))
            .await
            .unwrap();
        assert!(matches!(result, Err(ProcessError::TooLarge)), "{result:?}");
    }
}
