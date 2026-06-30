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
- All tests must be green (`dotnet test src/nKafka.sln -c Release`) before committing or pushing
- Comprehensive test coverage for buffer operations
- Test bounds checking, error cases, and edge conditions
- Roundtrip tests for all data types
- Test naming pattern: `MethodUnderTest_Condition_Expectation` (e.g., `WriteByte_Zero_CanBeReadByBufferReader`)

### Exception Hierarchy
- **KafkaException** (base)
  - **SerializationException** - BufferWriter errors, serialization failures
  - **DeserializationException** - BufferReader errors, deserialization failures
  - **ProtocolException** - Protocol violations, invalid message structure
  - **ChecksumValidationException** - CRC/checksum validation failures
  - **ConnectionException** - Network/connection failures
  - **IncompleteMessageException** - Partial/incomplete messages
- **Keep as `InvalidOperationException`:** Programming errors (uninitialized properties, null key values)

### Buffer Management
- **BufferWriter:** Use pool-based constructor only: `new BufferWriter(arrayPool, size)`

### Dead Code Removal
- **Remove unused methods** before and after refactoring

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

