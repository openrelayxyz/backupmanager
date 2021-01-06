package main

import (
  "bytes"
  "io"
  "io/ioutil"
  "os"
  "strconv"
  "strings"
  "github.com/klauspost/compress/zlib"
  "github.com/klauspost/compress/s2"
  "github.com/klauspost/compress/snappy"
  "github.com/klauspost/compress/zstd"
)

type encoder interface{
  Close() error
  Flush() error
  Reset(w io.Writer)
  Write(p []byte) (n int, err error)
}

func getCompressor(sig string) func([]byte) []byte {
  var enc encoder
  var compressionBuffer = bytes.NewBuffer(make([]byte, 25*1024*1024))
  parts := strings.Split(sig, ":")
  level := -1
  if len(parts) > 1 {
    var err error
    level, err = strconv.Atoi(parts[1])
    if err != nil { return nil }
  }
  switch alg := parts[0]; alg {
  case "zstd":
    if level == -1 { level = 3 }
    opts := []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level))}
    if len(parts) > 2 {
      fd, err := os.Open(parts[2])
      if err == nil {
        dict, err := ioutil.ReadAll(fd)
        if err == nil {
          opts = append(opts, zstd.WithEncoderDict(dict))
        }
        fd.Close()
      }
    }
    enc, _ = zstd.NewWriter(compressionBuffer, opts...)
  case "zlib":
    if level == -1 { level = 6 }
    enc, _ = zlib.NewWriterLevel(compressionBuffer, level)
  case "s2":
    enc = s2.NewWriter(compressionBuffer)
  case "snappy":
    enc = snappy.NewBufferedWriter(compressionBuffer)
  }
  return func(in []byte) []byte {
    compressionBuffer.Reset()
    enc.Reset(compressionBuffer)
    enc.Write(in)
    enc.Close()
    return compressionBuffer.Bytes()
  }
}


//   if len(data) == 0 { return data, nil }
//   r, err := zlib.NewReader(bytes.NewBuffer(data))
//   if err != nil { return []byte{}, err }
//   raw, err := ioutil.ReadAll(r)
//   if err == io.EOF || err == io.ErrUnexpectedEOF {
//     return raw, nil
//   }
//   return raw, err
// }

type resetError interface {
  Reset(io.Reader) error
}
type resetNoError interface {
  Reset(io.Reader)
}

// TODO: Keep the same reader around and reset each call.
func getDecompressor(sig string) func(in io.Reader) ([]byte, error) {
  parts := strings.Split(sig, ":")
  var dec io.Reader
  switch alg := parts[0]; alg {
  case "zstd":
    opts := []zstd.DOption{}
    if len(parts) > 2 {
      fd, err := os.Open(parts[2])
      if err == nil {
        dict, err := ioutil.ReadAll(fd)
        if err == nil {
          opts = append(opts, zstd.WithDecoderDicts(dict))
        }
        fd.Close()
      }
    }
    dec, _ = zstd.NewReader(nil, opts...)
  case "zlib":
    dec, _ = zlib.NewReader(nil)
  case "s2":
    dec = s2.NewReader(nil)
  case "snappy":
    dec = snappy.NewReader(nil)
  }
  return func(in io.Reader) ([]byte, error) {
    switch resetter := dec.(type) {
    case resetError:
      if err := resetter.Reset(in); err != nil { return nil, err }
    case resetNoError:
      resetter.Reset(in)
    }
    raw, err := ioutil.ReadAll(dec)
    if err == io.EOF {
      return raw, nil
    }
    return raw, err
  }
}
