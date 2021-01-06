package main

import (
  "os"
  "flag"
  "log"

  "net/http"
  _ "net/http/pprof"
  "time"
)

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
    chunkSize := flag.Int64("chunk-size", 134217728, "The number of bytes (pre-compressed) of each uploaded file chunk")
    flag.CommandLine.Parse(os.Args[2:])
    args := flag.CommandLine.Args()
    bucket := args[0]
    prefix := args[1]
    file := args[2]
    if err := uploadFile(bucket, prefix, file, *compressor, *chunkSize); err != nil {
      log.Fatalf(err.Error())
    }
  case "download":
    concurrency := flag.Int("concurrency", 10, "The number of concurrent threads for downloading / decompressing data")
    flag.CommandLine.Parse(os.Args[2:])
    args := flag.CommandLine.Args()
    bucket := args[0]
    prefix := args[1]
    file := args[2]
    if err := downloadFile(bucket, prefix, file, *concurrency); err != nil {
      log.Fatalf(err.Error())
    }
  default:
    log.Fatalf("Unknown subcommand\n")
  }
  log.Printf("Done")
}
