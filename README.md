# SimpleWAL
A demo implementation of write ahead log.

A more elaborate explanable in https://eileen-code4fun.medium.com/building-an-append-only-log-from-scratch-e8712b49c924.

Benchmark test for sync vs async:

| Test                | Iterations | Cost           |
| :------------------ | :--------: | -------------: |
| BenchmarkSyncWAL-8  | 1000000000 |	0.363 ns/op |
| BenchmarkAsyncWAL-8 | 1000000000 | 0.000796 ns/op |
