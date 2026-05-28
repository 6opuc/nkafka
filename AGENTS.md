# Project Instructions

## Git Rules
- **NEVER commit changes unless explicitly asked by the user.**
- **NEVER push changes.**
- Do not stage files with `git add` unless the user explicitly requests it.
- Always confirm before making any git operation beyond status/diff/log.

## Code Review Lessons Learned (PR #2)

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
- **Do NOT fix formatting individually** during implementation
- Final step: `dotnet format src/nKafka.sln`
- Commit formatting changes separately if needed

### Performance
- Change `Message.Key/Value` from `byte[]?` to `Memory<byte>?`
- Eliminate `.ToArray()` allocations in deserializers
- Use span-based APIs throughout

### Documentation
- Add XML comments for non-obvious patterns (e.g., temporary writer patterns)
- Explain why certain approaches are used (e.g., ref struct position handling)

### Testing
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

### Implementation Plan
- All 18 PR #2 review comments analyzed and organized into 7 phases
- Implementation plan: `~/projects/pr2-implementation-plan.md` (NOT in repository)
- Wait for approval before implementing
