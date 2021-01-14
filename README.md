# SimpleWAL
A demo implementation of write ahead log.

Benchmark test for sync vs async:

| Test                | Iterations | Cost           |
| :------------------ | :--------: | -------------: |
| BenchmarkSyncWAL-8  | 1000000000 |	0.363 ns/op |
| BenchmarkAsyncWAL-8 | 1000000000 | 0.000796 ns/op |
