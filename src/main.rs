use anyhow::{Context, Result};
use futures_util::StreamExt;
use rusoto_core::Region;
use rusoto_s3::S3;
use std::path::{Path, PathBuf};
use std::{io::BufRead, str::FromStr};
use structopt::StructOpt;
use tracing::{error, info, instrument};
use tracing_subscriber::EnvFilter;

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

    /// The maximum number of inflight requests.
    /// Defaults to (number of cpus * 10)
    #[structopt(long, short)]
    parallelism: Option<usize>,

    /// What to do when attempting to download a file that already exists locally
    #[structopt(long, short = "e", possible_values = &OnExistingFile::variants(), default_value = "skip")]
    on_existing_file: OnExistingFile,

    /// The AWS region. Overrides the region found using the provider chain.
    #[structopt(long, short)]
    region: Option<Region>,

    #[structopt(long, short = "l", possible_values = &EventFormat::variants(), default_value = "full")]
    event_format: EventFormat,
}

#[derive(Clone, Copy)]
enum OnExistingFile {
    /// Do not download the file, and do not log anything.
    Skip,
    /// Do not download the file, log an error
    Error,
    /// Download and overwrite the file
    Overwrite,
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
                format!(
                    "file_write_mode must be one of {:?}",
                    OnExistingFile::variants()
                ),
            )),
        }
    }
}

enum EventFormat {
    Full,
    Compact,
    Pretty,
    Json,
}

impl EventFormat {
    fn variants() -> [&'static str; 4] {
        ["full", "compact", "pretty", "json"]
    }
}

impl FromStr for EventFormat {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "full" => Ok(EventFormat::Full),
            "compact" => Ok(EventFormat::Compact),
            "pretty" => Ok(EventFormat::Pretty),
            "json" => Ok(EventFormat::Json),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("event_format must be one of {:?}", EventFormat::variants()),
            )),
        }
    }
}

macro_rules! ok_or_err {
    ($result:expr) => {
        match $result {
            Ok(value) => value,
            Err(e) => {
                error!(error = %e);
                return;
            }
        }
    };
    ($result:expr, $key:expr) => {
        match $result {
            Ok(value) => value,
            Err(e) => {
                error!(key = $key.as_str(), error = %e);
                return;
            }
        }
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = Options::from_args();

    configure_logging(&options);

    let region = if let Some(region) = options.region {
        region
    } else {
        rusoto_core::Region::default()
    };

    let client = rusoto_s3::S3Client::new(region);

    download_keys(
        client,
        options.bucket,
        options.keys_path,
        options.out_path,
        options.on_existing_file,
        options.parallelism.unwrap_or_else(|| num_cpus::get() * 10),
    )
    .await?;

    Ok(())
}

async fn download_keys(
    client: rusoto_s3::S3Client,
    bucket: String,
    keys_path: PathBuf,
    out_path: PathBuf,
    on_existing_file: OnExistingFile,
    parallelism: usize,
) -> Result<()> {
    let keys_file = std::fs::File::open(&keys_path)?;
    let keys_buf = std::io::BufReader::new(keys_file);
    let keys_lines = futures_util::stream::iter(keys_buf.lines());

    let stream = keys_lines.map(|line| {
        let key = line.unwrap();
        download_key(
            client.clone(),
            bucket.clone(),
            key,
            out_path.clone(),
            on_existing_file,
        )
    });

    let mut buffered = stream.buffer_unordered(parallelism);

    // we do it this way because Rust does not have async for-loops yet
    while buffered.next().await.is_some() {}

    Ok(())
}

#[instrument(skip(client, bucket, out_path, on_existing_file))]
async fn download_key(
    client: rusoto_s3::S3Client,
    bucket: String,
    key: String,
    out_path: PathBuf,
    on_existing_file: OnExistingFile,
) {
    let mut out_path = out_path;

    let req = rusoto_s3::GetObjectRequest {
        bucket,
        key: key.clone(),
        ..Default::default()
    };

    info!(key = key.as_str(), status = "started");

    let filename = Path::new(&key).file_name().unwrap();

    out_path.push(filename);

    match &on_existing_file {
        OnExistingFile::Skip => {
            if Path::new(&out_path).exists() {
                return;
            }
        }
        OnExistingFile::Error => {
            if Path::new(&out_path).exists() {
                ok_or_err!(Err(anyhow::anyhow!("{:?} already exists", key)), key);
            }
        }
        OnExistingFile::Overwrite => (),
    }

    let mut file = ok_or_err!(
        tokio::fs::File::create(&out_path)
            .await
            .with_context(|| format!("Could not create local file: {:?}", out_path)),
        key
    );

    let resp = ok_or_err!(client.get_object(req).await, key);

    let body = ok_or_err!(resp.body.ok_or("response body was empty"), key);

    let mut async_body = body.into_async_read();

    ok_or_err!(tokio::io::copy(&mut async_body, &mut file).await, key);

    info!(key = key.as_str(), status = "finished");
}

// this function is stupid-long due to the way tracing does formatting types
// https://github.com/tokio-rs/tracing/issues/575
fn configure_logging(options: &Options) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let tracing_builder = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);

    match options.event_format {
        EventFormat::Full => {
            let subscriber = tracing_builder.finish();

            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");
        }
        EventFormat::Compact => {
            let subscriber = tracing_builder
                .event_format(tracing_subscriber::fmt::format().compact())
                .finish();

            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");
        }

        EventFormat::Pretty => {
            let subscriber = tracing_builder
                .event_format(tracing_subscriber::fmt::format().pretty())
                .finish();

            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");
        }
        EventFormat::Json => {
            let subscriber = tracing_builder
                .event_format(tracing_subscriber::fmt::format().json())
                .finish();

            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");
        }
    };
}
