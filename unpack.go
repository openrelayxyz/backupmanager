package main

import (
  "fmt"
  "log"
  "io/ioutil"
  "os"
  "path"
  "encoding/json"
  "regexp"
  "sync"
  "path/filepath"
)

func unpack(source, destination, pattern string, concurrency int) error {
  pattern_re, err := regexp.Compile(pattern)
  if err != nil {
    return err
  }

  obj, err := os.Open(filepath.Join(source, "manifest.1"))
  if err != nil {
    log.Printf("Error getting object: %v/manifest.1", source)
    return err
  }
  manifestBody, err := ioutil.ReadAll(obj)
  obj.Close()

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
        log.Printf("Unpacked parts %v / %v", i, knownKeys)
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
    if !pattern_re.MatchString(fname) {
      continue
    }
    knownKeys += frecord.Size / frecord.ChunkSize
    filech[fname] = make(chan dpart)
    kwg.Add(1)
    wwg.Add(1)
    go func(fname string, frecord fileRecord, wg *sync.WaitGroup, ch chan dpart) {
      for i := uint(0); i < frecord.Size; i += frecord.ChunkSize {
        keyCh <- dpart{
          key: fmt.Sprintf("%v/%0.12x", source, (i + frecord.Start)),
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
        obj, err := os.Open(filepath.Join(p.key))
        if err != nil {
          log.Printf("Error getting object: %v/%v", source, p.key)
          errCh <-err
          return
        }

        p.data, err = decompress(obj)
        obj.Close()
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
