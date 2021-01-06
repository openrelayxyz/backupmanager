package main

import (
  "fmt"
  "log"
  "io/ioutil"
  "os"
  "strconv"
  "strings"
  "sync"
  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/session"
  s3 "github.com/aws/aws-sdk-go/service/s3"
)


func downloadFile(bucket, prefix, file string, concurrency int) error {
  session.Must(session.NewSession())
  svc := s3.New(session.Must(session.NewSession()))
  // file := os.Args[3]
  fd, err := os.Create(file)
  if err != nil { log.Fatalf(err.Error()) }

  // TODO: Get manifest, list objects, download
  obj, err := svc.GetObject(&s3.GetObjectInput{
    Bucket: aws.String(bucket),
    Key: aws.String(fmt.Sprintf("%v/manifest.0", prefix)),
  })
  if err != nil {
    log.Printf("Error getting object: s3://%v/%v/manifest.0", bucket, prefix)
    return err
  }
  manifestBody, err := ioutil.ReadAll(obj.Body)
  if err != nil { return err }
  compressionCodec := string(manifestBody)
  keyCh := make(chan string, concurrency)
  errCh := make(chan error)
  knownKeys := 0

  go func() {
    objList, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
      Bucket: aws.String(bucket),
      Prefix: aws.String(prefix + "/"),
      Delimiter: aws.String("/"),
    })
    if err != nil {
      log.Printf("Error getting object listing: s3://%v/%v", bucket, prefix)
      errCh <- err
      return
    }
    knownKeys += len(objList.Contents)
    for _, obj := range objList.Contents {
      if !strings.HasSuffix(*obj.Key, "manifest.0") {
        keyCh <- *obj.Key
      }
    }
    for *objList.IsTruncated {
      objList, err = svc.ListObjectsV2(&s3.ListObjectsV2Input{
        Bucket: aws.String(bucket),
        Prefix: aws.String(prefix + "/"),
        Delimiter: aws.String("/"),
        ContinuationToken: objList.NextContinuationToken,
      })
      if err != nil {
        log.Printf("Error getting object listing: s3://%v/%v", bucket, prefix)
        errCh <- err
        return
      }
      knownKeys += len(objList.Contents)
      for _, obj := range objList.Contents {
        if !strings.HasSuffix(*obj.Key, "manifest.0") {
          keyCh <- *obj.Key
        }
      }
    }
    close(keyCh)
  }()
  partCh := make(chan part, concurrency)
  var wg sync.WaitGroup
  for i := 0; i < concurrency; i++ {
    wg.Add(1)
    go func(wg *sync.WaitGroup){
      decompress := getDecompressor(compressionCodec)
      for key := range keyCh {
        obj, err := svc.GetObject(&s3.GetObjectInput{
          Bucket: aws.String(bucket),
          Key: aws.String(key),
        })
        if err != nil {
          log.Printf("Error getting object: s3://%v/%v", bucket, key)
          errCh <- err
          return
        }
        hexOffset := strings.TrimPrefix(key, prefix + "/")
        offset, err := strconv.ParseInt(hexOffset, 16, 64)
        if err != nil {
          log.Printf("Error parsing int from key s3://%v/%v (%v)", bucket, key, strings.HasSuffix(key, "manifest.0"))
          errCh <- err
          return
        }
        data, err := decompress(obj.Body)
        if err != nil {
          errCh <- err
          return
        }
        partCh <- part{
          data: data,
          offset: offset,
        }
      }
      wg.Done()
    }(&wg)
  }
  go func(wg *sync.WaitGroup) {
    wg.Wait()
    close(partCh)
  }(&wg)
  counter := 1
  for part := range partCh {
    if _, err := fd.Seek(part.offset, 0); err != nil { log.Fatalf("Failed to seek: %v", err.Error()) }
    if _, err := fd.Write(part.data); err != nil { log.Fatalf("Failed to write: %v", err.Error()) }
    log.Printf("%v of at least %v", counter, knownKeys)
    counter++
    select {
    case err := <-errCh:
      if err != nil { return err }
    default:
    }
  }
  select {
  case err := <-errCh:
    return err
  default:
    return nil
  }
}
