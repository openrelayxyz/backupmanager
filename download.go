package main

import (
  "fmt"
  "log"
  "io/ioutil"
  "os"
  "path"
  "encoding/json"
  "sync"
  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/session"
  s3 "github.com/aws/aws-sdk-go/service/s3"
)

func recursiveMakeDir(p string) {
  parent := path.Dir(p)
  if len(parent) > 1 {
    recursiveMakeDir(parent)
    os.Mkdir(parent, 0755)
  }
}

func downloadFile(bucket, prefix, destination string, concurrency int) error {
  session.Must(session.NewSession())
  svc := s3.New(session.Must(session.NewSession()))

  obj, err := svc.GetObject(&s3.GetObjectInput{
    Bucket: aws.String(bucket),
    Key: aws.String(fmt.Sprintf("%v/manifest.1", prefix)),
  })
  if err != nil {
    log.Printf("Error getting object: s3://%v/%v/manifest.1", bucket, prefix)
    return err
  }
  manifestBody, err := ioutil.ReadAll(obj.Body)

  var mf manifest
  if err := json.Unmarshal(manifestBody, &mf); err != nil { return err }


  if err != nil { return err }
  keyCh := make(chan dpart, concurrency)
  errCh := make(chan error)
  progressCh := make(chan struct{})
  knownKeys := uint(0)
  filech := make(map[string]chan dpart)

  var kwg sync.WaitGroup
  var wwg sync.WaitGroup
  go func() {
    i := 0
    for _ = range progressCh {
      if i % 100 == 0 && i > 0 {
        log.Printf("Downloaded parts %v / %v", i, knownKeys)
      }
      i += 1
    }
    log.Printf("Complete!")
  }()
  go func() {
    kwg.Wait()
    close(keyCh)
    wwg.Wait()
    close(progressCh)
  }()
  for fname, frecord := range mf.Files {
    knownKeys += frecord.Size / frecord.ChunkSize
    filech[fname] = make(chan dpart)
    kwg.Add(1)
    wwg.Add(1)
    go func(fname string, frecord fileRecord, wg *sync.WaitGroup, ch chan dpart) {
      for i := uint(0); i < frecord.Size; i += frecord.ChunkSize {
        keyCh <- dpart{
          key: fmt.Sprintf("%v/%0.12x", prefix, (i + frecord.Start)),
          offset: i,
          ch: ch,
        }
      }
      wg.Done()
    }(fname, frecord, &kwg, filech[fname])
    go func(fname string, wg *sync.WaitGroup) {
      var fpath string
      if len(mf.Files) != 1 {
        fpath = path.Join(destination, fname)
        recursiveMakeDir(fpath)
      } else {
        fpath = destination
      }
      fd, err := os.Create(fpath)
      if err != nil { log.Fatalf(err.Error()) }
      for p := range filech[fname] {
        if _, err := fd.Seek(int64(p.offset), 0); err != nil { log.Fatalf("Failed to seek: %v", err.Error()) }
        if _, err := fd.Write(p.data); err != nil { log.Fatalf("Failed to write: %v", err.Error()) }
        progressCh <- struct{}{}
      }
      wg.Done()
    }(fname, &wwg)
  }

  var wg sync.WaitGroup
  for i := 0; i < concurrency; i++ {
    wg.Add(1)
    go func(wg *sync.WaitGroup){
      decompress := getDecompressor(mf.Compression)
      for p := range keyCh {
        obj, err := svc.GetObject(&s3.GetObjectInput{
          Bucket: aws.String(bucket),
          Key: aws.String(p.key),
        })
        if err != nil {
          log.Printf("Error getting object: s3://%v/%v", bucket, p.key)
          errCh <- err
          return
        }
        p.data, err = decompress(obj.Body)
        if err != nil {
          errCh <- fmt.Errorf("Error decompressing %v: %v", p.key, err.Error())
          return
        }
        p.ch <- p
      }
      wg.Done()
    }(&wg)
  }
  go func(wg *sync.WaitGroup) {
    wg.Wait()
    for _, ch := range filech {
      close(ch)
    }
    wwg.Wait()
    errCh <- nil
  }(&wg)
  select {
  case err := <-errCh:
    return err
  }
}
