use anyhow::{Context, Result};
use futures_util::StreamExt;
use rusoto_core::Region;
use rusoto_s3::S3;
use std::path::{Path, PathBuf};
use std::{io::BufRead, str::FromStr};
use structopt::StructOpt;
use tracing::instrument;
use tracing_subscriber::EnvFilter;

/// Download files from S3 in parallel.
#[derive(StructOpt)]
struct Options {
    /// The target S3 bucket.
    #[structopt(long, short = "b")]
    bucket: String,

    /// A path to a newline-separated file of AWS S3 keys to download.
    /// The keys should be relative, like `a/path/to/a/file.jpg`
    #[structopt(long, short = "f")]
    keys_file: PathBuf,

    /// Where the downloaded files should be written.
    #[structopt(long, short = "o")]
    out_path: PathBuf,

    /// The maximum number of inflight requests.
    /// Defaults to (number of cpus * 10)
    #[structopt(long, short = "p")]
    parallelism: Option<usize>,

    /// What to do when attempting to download a file that already exists locally
    #[structopt(long, short = "x", possible_values = &OnExistingFile::variants(), default_value = "skip")]
    on_existing_file: OnExistingFile,

    /// The AWS region. Overrides the region found using the provider chain.
    #[structopt(long, short = "r")]
    region: Option<Region>,

    /// The logging format.
    #[structopt(long, short = "e", possible_values = &EventFormat::variants(), default_value = "full")]
    event_format: EventFormat,

    /// Force keys to download in the order in which they appear in `keys_file`.
    /// By default, keys are downloaded in a nondeterministic order.
    #[structopt(long, short = "d")]
    ordered: bool,
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

    let keys_file = std::fs::File::open(&options.keys_file)?;
    let keys_buf = std::io::BufReader::new(keys_file);
    let keys = futures_util::stream::iter(keys_buf.lines()).map(|line| line.unwrap());

    download_keys(
        client,
        options.bucket,
        keys,
        options.out_path,
        options.on_existing_file,
        options.parallelism.unwrap_or_else(|| num_cpus::get() * 10),
        options.ordered,
    )
    .await?;

    Ok(())
}

async fn download_keys<K: futures_util::stream::Stream<Item = String> + std::marker::Unpin>(
    client: rusoto_s3::S3Client,
    bucket: String,
    keys: K,
    out_path: PathBuf,
    on_existing_file: OnExistingFile,
    parallelism: usize,
    ordered: bool,
) -> Result<()> {
    let stream = keys.map(|key| {
        download_key(
            client.clone(),
            bucket.clone(),
            key,
            out_path.clone(),
            on_existing_file,
        )
    });

    if ordered {
        let mut buffered = stream.buffered(parallelism);
        while buffered.next().await.is_some() {}
    } else {
        let mut buffered = stream.buffer_unordered(parallelism);
        while buffered.next().await.is_some() {}
    };

    Ok(())
}

#[instrument(err, skip(client, bucket, out_path, on_existing_file))]
async fn download_key(
    client: rusoto_s3::S3Client,
    bucket: String,
    key: String,
    out_path: PathBuf,
    on_existing_file: OnExistingFile,
) -> Result<()> {
    let mut out_path = out_path;

    let req = rusoto_s3::GetObjectRequest {
        bucket,
        key: key.clone(),
        ..Default::default()
    };

    let filename = Path::new(&key).file_name().unwrap();

    out_path.push(filename);

    match &on_existing_file {
        OnExistingFile::Skip => {
            if Path::new(&out_path).exists() {
                return Ok(());
            }
        }
        OnExistingFile::Error => {
            if Path::new(&out_path).exists() {
                return Err(anyhow::anyhow!("{:?} already exists", key));
            }
        }
        OnExistingFile::Overwrite => (),
    }

    let mut file = tokio::fs::File::create(&out_path)
        .await
        .with_context(|| format!("Could not create local file: {:?}", out_path))?;

    let resp = client.get_object(req).await?;

    let body = resp
        .body
        .ok_or_else(|| anyhow::anyhow!("response body was empty"))?;

    let mut async_body = body.into_async_read();

    tokio::io::copy(&mut async_body, &mut file).await?;

    Ok(())
}

// this function is stupid-long due to the way tracing does formatting types
// https://github.com/tokio-rs/tracing/issues/575
fn configure_logging(options: &Options) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let tracing_builder = tracing_subscriber::FmtSubscriber::builder()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
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
            // TODO: this does not currently output keys
            // https://github.com/tokio-rs/tracing/issues/1032
            // https://github.com/tokio-rs/tracing/pull/1334
            let subscriber = tracing_builder
                .event_format(tracing_subscriber::fmt::format().json())
                .finish();

            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");
        }
    };
}
