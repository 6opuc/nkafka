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
