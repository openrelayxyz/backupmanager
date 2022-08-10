package main

type part struct {
  data []byte
  offset uint
}

type fileRecord struct {
  Start uint `json:"s"`
  Size uint `json:"e"`
  ChunkSize uint `json:"c"`
}

type manifest struct {
  Compression string `json:"c"`
  Files map[string]fileRecord `json:"f"`
}

type dpart struct {
  key string
  data []byte
  offset uint
  ch chan dpart
}
