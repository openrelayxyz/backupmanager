# Backup Manager

Backup Manager is used for backing up single, large files.

When backing up to Amazon S3, there are two main levers to pull to manage
upload and download speed: Concurrency and Compression. When you have a large
file you want to backup (or restore from backup) quickly, you can break the
file up into chunks to take advantage of multiple upload / download streams for
higher total bandwidth, or you can compress the file to lower your total data
transfer requirements.

Unfortunately, most S3 management tools only let you leverage one of these
options at a time. If you compress the file as a whole, it must be compressed
and decompressed serially, and you cannot get much advantage from concurrency.

For example, you could do:

```
gzip myfile -C | aws s3 cp - s3://bucket/myfile.gz
```

To upload a compressed copy of the file, but simply running:

```
aws s3 cp myfile s3://bucket/myfile.gz
```

Will upload the file in parallel, potentially giving comparable upload times to
the compressed file due to higher bandwidth.

But while compression and parallelism each give substantial backup / restore
time improvements, using both enhancements together would be ideal. This is
where Backup Manager comes in.

Running:

```
backupmanager upload myfile s3://bucket/myfile
```

Will break `myfile` into 128 MB chunks, compress each chunk, then upload it
into an item in a folder at `s3://bucket/myfile/`, taking advantage of both
parallel uploads *and* compression. In turn, running:

```
backupmanager download s3://bucket/myfile myfile
```

Will download the chunks in parallel, decompress them, and write them out to
the indicated file.

## Options

### Uploader

* `--compressor` (s2): Indicate the compression algorithm to use for the upload. Options are:
  * `s2` (default): an extended Snappy compression that offers improved
  decompression speeds
  * `snappy`: the standard Snappy compression algorithm, decompressible with
  standard snappy tools
  * `zstd[:<level>]`: uses zstandard compression. If no compression level is
  specified, zstd level 3 will be used.
  * `zlib[:<level>]`: uses zlib compression. If no compression level is
  specified, zlib level 6 will be used.
* `--chunk-size` (134217728): The number of bytes (pre-compression) of each
  uploaded chunk. Increasing this will increase memory requirements of both the
  uploader and downloader, but may offer slightly better compression
  performance.


## Downloader

The downloader utility will derive the chunk size and compression algorithm
from the uploaded data, so those attributes need not be specified in the
downloader.

* `--concurrency`: The number of concurrent threads to be used for downloading
  and  decompressing the file during restoration.
