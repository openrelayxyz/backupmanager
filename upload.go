package main

import (
  "bytes"
  "fmt"
  "io"
  "io/fs"
  "path"
  "path/filepath"
  "encoding/json"
  "log"
  "os"
  "sync"
  "strings"
  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/session"
  s3 "github.com/aws/aws-sdk-go/service/s3"
)

func uploadFile(bucket, prefix, source, compressor string, chunkSize uint, concurrency int) error {
  session.Must(session.NewSession())
  svc := s3.New(session.Must(session.NewSession()))
  _, err := svc.GetObject(&s3.GetObjectInput{
    Bucket: aws.String(bucket),
    Key: aws.String(fmt.Sprintf("%v/manifest.0", prefix)),
  })
  if err == nil {
    log.Printf("Manifest already exists at s3://%v/%v/manifest.0 - stopping", bucket, prefix)
    return err
  }
  finfo, err := os.Stat(source)
  if err != nil { return err }
  mf := &manifest{
    Compression: compressor,
    Files: make(map[string]fileRecord),
  }
  start := uint(0)
  var singleFile bool
  switch mode := finfo.Mode(); {
  case mode.IsDir():
    singleFile = false
    filepath.Walk(source, func(path string, finfo fs.FileInfo, err error) error {
      if err != nil { return err }
      if finfo.IsDir() { return nil }
      mf.Files[strings.TrimPrefix(path, source)] = fileRecord{
        Start: start,
        Size: uint(finfo.Size()),
        ChunkSize: chunkSize,
      }
      start += uint(finfo.Size())
      return nil
    })
  case mode.IsRegular():
    singleFile = true
    mf.Files[source] = fileRecord{
      Start: 0,
      Size: uint(finfo.Size()),
      ChunkSize: chunkSize,
    }
  default:
    return fmt.Errorf("unknown file mode")
  }
  for file, fr := range mf.Files {
    fpath := path.Join(source, file)
    if singleFile {
      fpath = file
    }
    fd, err := os.Open(fpath)
    if err != nil { return err }
    defer fd.Close()
    partCh := make(chan part, concurrency)
    errCh := make(chan error)
    go func(file string, fr fileRecord){
      counter := 0
      for offset := uint(0); offset < fr.Size; offset += chunkSize {
        data := make([]byte, chunkSize)
        n, err := fd.Read(data[:])
        if err != nil && err != io.EOF { errCh <- err }
        if n > 0 {
          partCh <- part{
            data: data[:n],
            offset: offset + fr.Start,
          }
        } else {
          break
        }
        counter++
        if counter % 100 == 0 { log.Printf("Progress (%v): %v / %v chunks", file, counter, fr.Size / chunkSize)}
      }
      close(partCh)
    }(file, fr)
    var wg sync.WaitGroup
    for i := 0; i < concurrency; i++ {
      wg.Add(1)
      go func(wg *sync.WaitGroup) {
        compress := getCompressor(compressor)
        for part := range partCh {
          key := fmt.Sprintf("%v/%0.12x", prefix, part.offset)
          _, err := svc.PutObject(&s3.PutObjectInput{
            Bucket: aws.String(bucket),
            Key: aws.String(key),
            Body: bytes.NewReader(compress(part.data)),
          })
          if err != nil { errCh <- err }
        }
        wg.Done()
      }(&wg)
    }
    go func(wg *sync.WaitGroup) {
      wg.Wait()
      errCh <- nil
    }(&wg)
    if err := <-errCh; err != nil {
      return err
    }
  }
  data, err := json.Marshal(mf)
  if err != nil { return err }
  _, err = svc.PutObject(&s3.PutObjectInput{
    Bucket: aws.String(bucket),
    Key: aws.String(fmt.Sprintf("%v/manifest.1", prefix)),
    Body: bytes.NewReader(data),
  })
  return err
}
