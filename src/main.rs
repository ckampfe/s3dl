use anyhow::{Context, Result};
use futures_util::StreamExt;
use rusoto_core::Region;
use rusoto_s3::S3;
use std::path::{Path, PathBuf};
use std::{io::BufRead, str::FromStr};
use structopt::StructOpt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};

const DEFAULT_CHANNEL_BUFFER_CAPACITY: &str = "100";

/// Download files from S3 in parallel
#[derive(StructOpt)]
struct Options {
    /// The target S3 bucket.
    #[structopt(long, short)]
    bucket: String,

    /// A path to a newline-separated file of AWS S3 keys to download.
    /// The keys should be relative, like `a/path/to/a/file.jpg`
    #[structopt(long, short)]
    keys_path: PathBuf,

    /// Where the downloaded files should be written.
    #[structopt(long, short = "o")]
    out_path: PathBuf,

    /// The maximum number of inflight tasks.
    /// Defaults to (number of cpus * 10)
    #[structopt(long, short)]
    max_inflight_requests: Option<usize>,

    /// What to do when attempting to download a file that already exists locally
    #[structopt(long, short = "e", possible_values = &OnExistingFile::variants(), default_value = "skip")]
    on_existing_file: OnExistingFile,

    /// The AWS region. Overrides the region found using the provider chain.
    #[structopt(long, short)]
    region: Option<Region>,

    /// The size of the channel that synchronizes writes to stdout.
    /// You generally shouldn't need to worry about this.
    #[structopt(long, default_value = DEFAULT_CHANNEL_BUFFER_CAPACITY)]
    stdout_channel_capacity: usize,

    /// The size of the channel that synchronizes writes to stderr.
    /// You generally shouldn't need to worry about this.
    #[structopt(long, default_value = DEFAULT_CHANNEL_BUFFER_CAPACITY)]
    stderr_channel_capacity: usize,
}

#[derive(Clone, Copy)]
enum OnExistingFile {
    Skip,
    Overwrite,
    Error,
}

impl OnExistingFile {
    fn variants() -> [&'static str; 3] {
        ["skip", "overwrite", "error"]
    }
}

impl FromStr for OnExistingFile {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "skip" => Ok(OnExistingFile::Skip),
            "overwrite" => Ok(OnExistingFile::Overwrite),
            "error" => Ok(OnExistingFile::Error),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "file_write_mode must be one of {skip|overwrite|error}",
            )),
        }
    }
}

macro_rules! ok_or_send_err {
    ($result:expr, $err_fmt_str:expr, $err_chan:ident) => {
        match $result {
            Ok(value) => value,
            Err(e) => {
                return $err_chan
                    .send(format!(concat!($err_fmt_str, "\n"), e))
                    .await
                    .unwrap()
            }
        }
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = Options::from_args();

    let (stdout_s, mut stdout_r): (Sender<String>, Receiver<String>) =
        channel(options.stdout_channel_capacity);
    let (stderr_s, mut stderr_r): (Sender<String>, Receiver<String>) =
        channel(options.stderr_channel_capacity);

    let mut stdout = tokio::io::stdout();
    let mut stderr = tokio::io::stderr();

    let stdout_task = tokio::spawn(async move {
        while let Some(msg) = stdout_r.recv().await {
            stdout.write_all(msg.as_bytes()).await.unwrap();
        }
    });

    let stderr_task = tokio::spawn(async move {
        while let Some(msg) = stderr_r.recv().await {
            stderr.write_all(msg.as_bytes()).await.unwrap();
        }
    });

    let region = if let Some(region) = options.region {
        region
    } else {
        rusoto_core::Region::default()
    };

    let max_inflight_requests = options
        .max_inflight_requests
        .unwrap_or_else(|| num_cpus::get() * 10);

    let client = rusoto_s3::S3Client::new(region);

    let bucket = options.bucket;

    download_keys(
        client,
        bucket,
        options.keys_path,
        options.out_path,
        options.on_existing_file,
        stdout_s,
        stderr_s,
        max_inflight_requests,
    )
    .await?;

    stdout_task.await?;
    stderr_task.await?;

    Ok(())
}

async fn download_keys(
    client: rusoto_s3::S3Client,
    bucket: String,
    keys_path: PathBuf,
    out_path: PathBuf,
    on_existing_file: OnExistingFile,
    stdout_s: Sender<String>,
    stderr_s: Sender<String>,
    max_inflight_requests: usize,
) -> Result<()> {
    let keys_file = std::fs::File::open(&keys_path)?;
    let keys_buf = std::io::BufReader::new(keys_file);
    let keys_lines = futures_util::stream::iter(keys_buf.lines());

    let stream = keys_lines
        .map(|line| -> Result<(String, rusoto_s3::GetObjectRequest)> {
            let key = line?;
            Ok((
                key.clone(),
                rusoto_s3::GetObjectRequest {
                    bucket: bucket.clone(),
                    key,
                    ..Default::default()
                },
            ))
        })
        .map(|res| {
            // these are all just copying pointers
            let stdout_s = stdout_s.clone();
            let stderr_s = stderr_s.clone();
            let mut out = out_path.clone();
            let client = client.clone();

            async move {
                let (key, req) = ok_or_send_err!(res, "{}", stderr_s);

                stdout_s.send(format!("{}: started\n", key)).await.unwrap();

                let filename = Path::new(&key).file_name().unwrap();

                out.push(filename);

                match &on_existing_file {
                    OnExistingFile::Skip => {
                        if Path::new(&out).exists() {
                            return;
                        }
                    }
                    OnExistingFile::Error => {
                        if Path::new(&out).exists() {
                            ok_or_send_err!(
                                Err(anyhow::anyhow!("{:?} already exists", key)),
                                "{}",
                                stderr_s
                            );
                            return;
                        }
                    }
                    OnExistingFile::Overwrite => (),
                }

                let mut file = ok_or_send_err!(
                    tokio::fs::File::create(&out)
                        .await
                        .with_context(|| format!("Could not create local file: {:?}", out)),
                    "{}",
                    stderr_s
                );

                let resp = ok_or_send_err!(client.get_object(req).await, "{}", stderr_s);

                let body =
                    ok_or_send_err!(resp.body.ok_or("response body was empty"), "{}", stderr_s);

                let mut async_body = body.into_async_read();

                ok_or_send_err!(
                    tokio::io::copy(&mut async_body, &mut file).await,
                    "{}",
                    stderr_s
                );

                stdout_s
                    .send(format!("{}: completed\n", key))
                    .await
                    .unwrap();
            }
        });

    let mut buffered = stream.buffer_unordered(max_inflight_requests);

    // we do it this way because Rust does not have async for-loops yet
    while buffered.next().await.is_some() {}

    Ok(())
}
