# s3dl

Download files from S3 in parallel

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
s3dl 0.1.0
Download files from S3 in parallel

USAGE:
    s3dl [OPTIONS] --bucket <bucket> --keys-path <keys-path> --out-path <out-path>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -b, --bucket <bucket>                        The target S3 bucket
    -l, --event-format <event-format>             [default: full]  [possible values: full, compact, pretty, json]
    -k, --keys-path <keys-path>
            A path to a newline-separated file of AWS S3 keys to download. The keys should be relative, like
            `a/path/to/a/file.jpg`
    -e, --on-existing-file <on-existing-file>
            What to do when attempting to download a file that already exists locally [default: skip]  [possible values:
            skip, overwrite, error]
    -o, --out-path <out-path>                    Where the downloaded files should be written
    -p, --parallelism <parallelism>
            The maximum number of inflight requests. Defaults to (number of cpus * 10)

    -r, --region <region>                        The AWS region. Overrides the region found using the provider chain
```

## todo

- [ ] options for what to do when a file already exists? replace/ignore/error
- [ ] quiet mode?
- [ ] logging instead of echo to stdout?
