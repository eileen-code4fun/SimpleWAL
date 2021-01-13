package wal

import (
  "encoding/binary"
  "fmt"
  "hash/crc32"
  "os"
)

const (
  dataLenSize = 4
  crcSize = 4
)

type WAL struct {
  f *os.File
  buffer []byte
  bi int
  fsync bool
  bufferSize, maxRecordSize int
}

func NewWAL(filename string, fsync bool, bufferSize, maxRecordSize int) (*WAL, error) {
  if maxRecordSize + dataLenSize + crcSize > bufferSize {
    return nil, fmt.Errorf("error in size configuration")
  }
  file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0660)
  if err != nil {
    return nil, err
  }
  return &WAL{
    f: file,
    buffer: make([]byte, bufferSize),
    bi: 0,
    fsync: fsync,
    bufferSize: bufferSize,
    maxRecordSize: maxRecordSize + dataLenSize + crcSize,
  }, nil
}

func (w *WAL) AddRecord(data []byte) error {
  if dataLenSize + len(data) > w.maxRecordSize {
    return fmt.Errorf("record size %d exceeds limit %d", len(data), w.maxRecordSize - dataLenSize - crcSize)
  }
  if w.bi + w.maxRecordSize > w.bufferSize {
    if err := w.Flush(); err != nil {
      return err
    }
  }
  // Write data len.
  binary.LittleEndian.PutUint32(w.buffer[w.bi:], uint32(len(data)))
  w.bi += dataLenSize
  copy(w.buffer[w.bi:], data)
  w.bi += len(data)
  crc := crc32.ChecksumIEEE(w.buffer[w.bi-len(data)-dataLenSize:w.bi])
  binary.LittleEndian.PutUint32(w.buffer[w.bi:], crc)
  w.bi += crcSize
  return nil
}

func (w *WAL) Flush() error {
  for ; w.bi < w.bufferSize; w.bi ++ {
    // Pad the remaining space with 0.
    w.buffer[w.bi] = 0
  }
  for i := 0; i < w.bi; {
    n, err := w.f.Write(w.buffer[i:])
    if err != nil {
      return err
    }
    i += n
  }
  w.bi = 0
  if w.fsync {
    return w.f.Sync()
  }
  return nil
}

func (w *WAL) Close() error {
  if err := w.Flush(); err != nil {
    return err
  }
  return w.f.Close()
}

type LogIterator struct {
  f *os.File
  buffer []byte
  bi int
  bufferSize, maxRecordSize int
}

func NewLogIterator(filename string, bufferSize, maxRecordSize int) (*LogIterator, error) {
  if maxRecordSize + dataLenSize + crcSize > bufferSize {
    return nil, fmt.Errorf("error in size configuration")
  }
  file, err := os.Open(filename)
  if err != nil {
    return nil, err
  }
  itr := &LogIterator{
    f: file,
    buffer: make([]byte, bufferSize),
    bi: 0,
    bufferSize: bufferSize,
    maxRecordSize: maxRecordSize + dataLenSize + crcSize,
  }
  return itr, itr.read()
}

func (itr *LogIterator) read() error {
  for i := 0; i < itr.bufferSize; {
    n, err := itr.f.Read(itr.buffer[i:])
    if err != nil {
      return err
    }
    i += n
  }
  return nil
}

func (itr *LogIterator) Next() ([]byte, error) {
  if itr.bi + itr.maxRecordSize > itr.bufferSize {
    if err := itr.read(); err != nil {
      return nil, err
    }
    itr.bi = 0
  }
  len := int(binary.LittleEndian.Uint32(itr.buffer[itr.bi:]))
  itr.bi += dataLenSize
  data := make([]byte, len)
  copy(data, itr.buffer[itr.bi:])
  itr.bi += len
  crc := binary.LittleEndian.Uint32(itr.buffer[itr.bi:])
  expectedCRC := crc32.ChecksumIEEE(itr.buffer[itr.bi-len-dataLenSize:itr.bi])
  itr.bi += crcSize
  if crc != expectedCRC {
    return nil, fmt.Errorf("crc mismatch; want %d; stored %d", expectedCRC, crc)
  }
  return data, nil
}

func (itr *LogIterator) Close() error {
  return itr.f.Close()
}
