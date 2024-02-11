package main

import (
  "os"
  "flag"
  "log"
  "strings"

  "net/http"
  _ "net/http/pprof"
  "time"
)

func parseTarget(target string) (string, string) {
  trimmed := strings.TrimSuffix(strings.TrimPrefix(target, "s3://"), "/")
  return strings.Split(trimmed, "/")[0], strings.Join(strings.Split(trimmed, "/")[1:], "/")
}

func main() {
  p := &http.Server{
    Addr: ":6969",
    Handler: http.DefaultServeMux,
    ReadHeaderTimeout: 5 * time.Second,
    MaxHeaderBytes: 1 << 20,
  }
  go p.ListenAndServe()
  switch subcommand := os.Args[1]; subcommand {
  case "upload":
    compressor := flag.String("compressor", "s2", "The compression algorithm and level (where applicable) to use")
    chunkSize := flag.Uint("chunk-size", 134217728, "The number of bytes (pre-compressed) of each uploaded file chunk")
    concurrency := flag.Int("concurrency", 10, "The number of concurrent threads for uploading / compressing data")
    flag.CommandLine.Parse(os.Args[2:])
    args := flag.CommandLine.Args()
    file := args[0]
    bucket, prefix := parseTarget(args[1])
    if err := uploadFile(bucket, prefix, file, *compressor, *chunkSize, *concurrency); err != nil {
      log.Fatalf(err.Error())
    }
  case "download":
    concurrency := flag.Int("concurrency", 10, "The number of concurrent threads for downloading / decompressing data")
    pattern := flag.String("pattern", ".*", "A regular expression indicating which files to download")
    flag.CommandLine.Parse(os.Args[2:])
    args := flag.CommandLine.Args()
    bucket, prefix := parseTarget(args[0])
    file := args[1]
    if err := downloadFile(bucket, prefix, file, *pattern, *concurrency); err != nil {
      log.Fatalf(err.Error())
    }
  case "unpack":
    concurrency := flag.Int("concurrency", 10, "The number of concurrent threads for downloading / decompressing data")
    pattern := flag.String("pattern", ".*", "A regular expression indicating which files to download")
    flag.CommandLine.Parse(os.Args[2:])
    args := flag.CommandLine.Args()
    path := args[0]
    file := args[1]
    if err := unpack(path, file, *pattern, *concurrency); err != nil {
      log.Fatalf(err.Error())
    }
  default:
    log.Fatalf("Unknown subcommand\n")
  }
  log.Printf("Done")
}
