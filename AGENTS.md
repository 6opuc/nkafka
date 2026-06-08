# Project Instructions

## Git Rules
- **NEVER commit changes unless explicitly asked by the user.**
- **NEVER push changes.**
- Do not stage files with `git add` unless the user explicitly requests it.
- Always confirm before making any git operation beyond status/diff/log.

## Code Review Lessons Learned

### API Consistency
- **Serialization**: Use `ref BufferWriter writer`
- **Deserialization**: Use `ref BufferReader reader`
- Symmetric API design, zero-allocation, no manual position tracking
- Parameter names: `writer` (not `tw`), `reader` (not `input`)

### Code Organization
- **One class per file** - avoid multi-type files
- **Remove unnecessary abstractions** - e.g., PooledBuffer was redundant
- Return `BufferWriter` directly from `ISerializationContext.CreateWriter()`

### Naming Conventions
- **Avoid abbreviations**: `tw` → `writer`, `input` → `reader`
- Use descriptive names that clarify intent
- Consistent naming across generated and manual code

### Safety & Validation
- **Protocol violations should fail fast** - throw exceptions, don't just log
- Validate response length: `reader.Remaining == 0` after deserialization
- Create exception hierarchy: `KafkaException` base with typed derivatives

### Code Cleanup
- **Remove unused methods** - trace all usages before keeping
- Consolidate duplicate methods (e.g., `VarIntSize` to `BufferWriter`)
- Remove redundant conditions (e.g., CRC check position validation)

### Formatting
- **Always format touched code after making changes**: `dotnet format src/nKafka.sln`
- The `.editorconfig` at the solution root defines all formatting rules (4-space indent, LF line endings, file-scoped namespaces, etc.)
- Respect `.editorconfig` rules when writing new code — do not manually fix formatting, rely on `dotnet format`
- Commit formatting changes separately if needed

### Performance
- Change `Message.Key/Value` from `byte[]?` to `Memory<byte>?`
- Eliminate `.ToArray()` allocations in deserializers
- Use span-based APIs throughout
- Prefer `BinaryPrimitives.WriteInt32BigEndian/WriteInt64BigEndian` over manual byte shifting in BufferWriter — reduces bounds checks from N to 1

### Documentation
- Add XML comments for non-obvious patterns (e.g., temporary writer patterns)
- Explain why certain approaches are used (e.g., ref struct position handling)

### Testing
- Comprehensive test coverage for buffer operations
- Test bounds checking, error cases, and edge conditions
- Roundtrip tests for all data types
- Test naming pattern: `MethodUnderTest_Condition_Expectation` (e.g., `WriteByte_Zero_CanBeReadByBufferReader`)

### Concurrency & Task Management
- **Shared CancellationTokenSource:** When cancelling a shared `_stop` CTS, do NOT replace it with a new one — bound tasks (fetch loops, heartbeat loop) hold references to the old token and won't see the new one
- **Self-deadlock:** When calling an async method from within a task (e.g., heartbeat task → `JoinGroupAsync()` → `StartSendingHeartbeats()`), do NOT `await _heartbeatsBackgroundTask` — it IS the current task
- **Heartbeat task cleanup:** Cancel `_stop` to signal the old heartbeat task to exit via `OperationCanceledException`; don't await when called from within the same task (it exits naturally after the calling method returns)
- **Fetch task cancellation:** `StopFetchingAsync()` calls `_stop.CancelAsync()` then `Task.WhenAll(_fetchTasks)` — wrap `WhenAll` in try-catch for `OperationCanceledException` since cancelled tasks throw

### Integration Tests for Rebalance
- **Timing:** Don't use `Task.Delay` before starting second consumer — start immediately so rebalance happens while first consumer is actively fetching (consuming all 4MB is fast, <1s)
- **Exception handling:** `ConsumeBatchAsync()` can throw `ChannelClosedException` (channel closed during rebalance) and `OperationCanceledException` (test cleanup) — catch both in consume loops
- **Test pattern:** Start consumer A, start consumer B immediately, wait for both to finish, verify both received messages

### Exception Hierarchy
- **KafkaException** (base)
  - **SerializationException** - BufferWriter errors, serialization failures
  - **DeserializationException** - BufferReader errors, deserialization failures
  - **ProtocolException** - Protocol violations, invalid message structure
  - **ChecksumValidationException** - CRC/checksum validation failures
  - **ConnectionException** - Network/connection failures
  - **IncompleteMessageException** - Partial/incomplete messages
- **Remove:** `InvalidMessageException` (unused), `CorruptRecordException` (rename to ChecksumValidationException)
- **Keep as `InvalidOperationException`:** Programming errors (uninitialized properties, null key values)

### Buffer Management
- **BufferWriter:** Use pool-based constructor only: `new BufferWriter(arrayPool, size)`
- **Buffer size:** Use `Math.Max(RequestBufferSize, ResponseBufferSize)` for consistency
- **Temporary writers:** Use `writerTemp` naming convention for temporary serializers
- **CreateWriter():** No parameters, uses configured buffer size

### Dead Code Removal
- **Crc32c.cs:** Remove all MemoryStream-based methods (`CalculateStream`, `_calculateStream` field)
- Keep only `ReadOnlySpan<byte>` overloads
- **Remove unused methods** before refactoring

### Benchmarks
- Project: `src/nKafka.Client.Benchmarks` (BenchmarkDotNet 0.15.8, compares nKafka vs Confluent.Kafka)
- **Prerequisites:** Kafka cluster running via docker-compose (`infra/docker-compose.yaml`), benchmark topics created
- **Create topics:** `bash infra/init-benchmark-topics.sh`
- **Run all benchmarks:** `dotnet run --project src/nKafka.Client.Benchmarks -c Release`
- **Run subset:** `dotnet run --project src/nKafka.Client.Benchmarks -c Release -- --filter 'nKafka.Client.Benchmarks.FetchBenchmarks.NKafkaFetchBytes*'`
- **Filter format:** `--filter 'namespace.TypeName.MethodName'` (full names from `--list flat`)
- **Outputs:** CSV, HTML, GitHub-flavored markdown in `BenchmarkDotNet.Artifacts/results/`
- **Always run benchmarks before proposing a PR** for performance-sensitive changes
- Compare against baseline: `--filter` only the changed method, run on same machine, same cluster state

