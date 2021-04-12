# s3dl

## install

```
$ cargo install --git https://github.com/ckampfe/s3dl --branch main
```

## example

```
$ cat my_s3_files.txt
a.stl
b.stl
c.png
d.png
e.rs
$ mkdir out
$ s3dl --bucket mybucket --keys-path my_s3_files.txt -o out
```

## use/options

```
clark@doomguy:~/code/personal/s3dl$ ./target/release/s3dl -h
s3dl 0.1.0
Download files from S3 in parallel

USAGE:
    s3dl [OPTIONS] --bucket <bucket> --keys-path <keys-path> --out-path <out-path>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -b, --bucket <bucket>                                      The target S3 bucket
    -k, --keys-path <keys-path>
            A path to a newline-separated file of AWS S3 keys to download. The keys should be relative, like
            `a/path/to/a/file.jpg`
    -m, --max-inflight-requests <max-inflight-requests>
            The maximum number of inflight tasks. Defaults to (number of cpus * 10)

    -o, --out-path <out-path>                                  Where the downloaded files should be written
    -r, --region <region>
            The AWS region. Overrides the region found using the provider chain

        --stderr-channel-capacity <stderr-channel-capacity>
            The size of the channel that synchronizes writes to stderr. You generally shouldn't need to worry about this
            [default: 100]
        --stdout-channel-capacity <stdout-channel-capacity>
            The size of the channel that synchronizes writes to stdout. You generally shouldn't need to worry about this
            [default: 100]
```

## todo

- [ ] options for what to do when a file already exists? replace/ignore/error
- [ ] quiet mode?
- [ ] logging instead of echo to stdout?
