package main

import (
  "bytes"
  "fmt"
  "io"
  "log"
  "os"
  "sync"
  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/session"
  s3 "github.com/aws/aws-sdk-go/service/s3"
)

func uploadFile(bucket, prefix, file, compressor string, chunkSize int64, concurrency int) error {
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
  fd, err := os.Open(file)
  if err != nil { return err }
  defer fd.Close()
  info, err := fd.Stat()
  if err != nil { return err }
  size := info.Size()
  partCh := make(chan part, concurrency)
  errCh := make(chan error)
  go func(){
    counter := 0
    for offset := int64(0); offset < size; offset += chunkSize {
      data := make([]byte, chunkSize)
      n, err := fd.Read(data[:])
      if err != nil && err != io.EOF { errCh <- err }
      if n > 0 {
        partCh <- part{
          data: data[:n],
          offset: offset,
        }
      } else {
        break
      }
      counter++
      if counter % 100 == 0 { log.Printf("Progress: %v / %v chunks", counter, size / chunkSize)}
    }
    close(partCh)
  }()
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
  _, err = svc.PutObject(&s3.PutObjectInput{
    Bucket: aws.String(bucket),
    Key: aws.String(fmt.Sprintf("%v/manifest.0", prefix)),
    Body: bytes.NewReader([]byte(compressor)),
  })
  return err
}
