package wal

// go test -v -run TestWAL

import (
  "fmt"
  "io"
  "os"
  "reflect"
  "testing"
)

func write(w *WAL, data string, t *testing.T) {
  if err := w.AddRecord([]byte(data)); err != nil {
    t.Fatalf("failed to write data %s; %v", data, err)
  }
}

func read(itr *LogIterator, t *testing.T) (string, bool) {
  data, err := itr.Next()
  if err != nil && err != io.EOF {
    t.Fatalf("failed to read log; %v", err)
  }
  return string(data), err == io.EOF
}

func TestWAL(t *testing.T) {
  defer os.Remove("test.log")
  bufferSize := 35
  maxRecordSize := 15
  w, err := NewWAL("test.log", true, bufferSize, maxRecordSize)
  if err != nil {
    t.Fatalf("failed to create WAL; %v", err)
  }
  data := []string{"hello world", "hello again", "hi world", "hi again"}
  for i, d := range data {
    t.Logf("write log entry: %d; content: %s", i, d)
    write(w, d, t)
  }
  if err := w.Close(); err != nil {
    t.Fatalf("failed to close; %v", err)
  }
  itr, err := NewLogIterator("test.log", bufferSize, maxRecordSize)
  if err != nil {
    t.Fatalf("failed to create iterator; %v", err)
  }
  var log []string
  for i := 0;; i ++{
    t.Logf("reading log entry: %d", i)
    l, done := read(itr, t)
    if done {
      break
    }
    log = append(log, l)
  }
  if err := itr.Close(); err != nil {
    t.Fatalf("failed to close; %v", err)
  }
  if !reflect.DeepEqual(data, log) {
    t.Errorf("want log %v; got %v", data, log)
  }
}

// go test -bench=.

func benchmark(fsync bool, b *testing.B) {
  filename := fmt.Sprintf("btest_%t.log", fsync)
  defer os.Remove(filename)
  bufferSize := 35
  maxRecordSize := 15
  w, err := NewWAL(filename, fsync, bufferSize, maxRecordSize)
  if err != nil {
    b.Fatalf("failed to create WAL; %v", err)
  }
  data := make([]byte, 12)
  for i := 0; i < 100; i ++ {
    if err := w.AddRecord(data); err != nil {
      b.Fatalf("failed to write data; %v", err)
    }
  }
  if err := w.Close(); err != nil {
    b.Fatalf("failed to close; %v", err)
  }
}

func BenchmarkSyncWAL(b *testing.B) {
  benchmark(true, b)
}

func BenchmarkAsyncWAL(b *testing.B) {
  benchmark(false, b)
}