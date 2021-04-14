use anyhow::{Context, Result};
use futures_util::StreamExt;
use rusoto_core::Region;
use rusoto_s3::S3;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use structopt::*;
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
    #[structopt(long, short)]
    out_path: PathBuf,

    /// The maximum number of inflight tasks.
    /// Defaults to (number of cpus * 10)
    #[structopt(long, short)]
    max_inflight_requests: Option<usize>,

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
                match res {
                    Ok((key, req)) => {
                        stdout_s.send(format!("{}: started\n", key)).await.unwrap();

                        match client.get_object(req).await {
                            Ok(resp) => {
                                let filename = Path::new(&key).file_name().unwrap();

                                out.push(filename);

                                let mut file = tokio::fs::File::create(&out)
                                    .await
                                    .with_context(|| {
                                        format!("Could not create local file: {:?}", out)
                                    })
                                    .unwrap();

                                if let Some(body) = resp.body {
                                    let mut async_body = body.into_async_read();
                                    match tokio::io::copy(&mut async_body, &mut file).await {
                                        Ok(_) => stdout_s
                                            .send(format!("{}: finished\n", key))
                                            .await
                                            .unwrap(),
                                        Err(e) => stderr_s
                                            .send(format!("{}: {:?}", key, e))
                                            .await
                                            .unwrap(),
                                    };
                                } else {
                                    stderr_s
                                        .send(format!("{}: response body was empty\n", key))
                                        .await
                                        .unwrap()
                                }
                            }
                            Err(e) => stderr_s.send(format!("{}: {:?}", key, e)).await.unwrap(),
                        }
                    }
                    Err(e) => stderr_s.send(format!("{}", e)).await.unwrap(),
                }
            }
        });

    let mut buffered = stream.buffer_unordered(max_inflight_requests);

    // we do it this way because Rust does not have async for-loops yet
    while buffered.next().await.is_some() {}

    Ok(())
}
