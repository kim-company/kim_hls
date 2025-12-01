# HLS Playlist Unmarshaling Benchmarks

## Overview

Benchmarks for measuring and optimizing HLS playlist unmarshaling performance.

## Running Benchmarks

```bash
mix run bench/playlist_unmarshal_bench.exs
```

## Optimization Results

### Performance Improvements (vs baseline)

| Metric | 100 segments | 1K segments | 10K segments |
|--------|-------------|-------------|--------------|
| **Speed** | 38.8% faster | 38.6% faster | 32.8% faster |
| **Memory** | 43.3% less | 43.8% less | 43.1% less |

### Detailed Metrics

#### Before Optimizations
- 100 segments: 1257 μs, 1.87 MB
- 1K segments: 12.4 ms, 18.47 MB
- 10K segments: 123.8 ms, 186.72 MB

#### After Optimizations
- 100 segments: 769 μs, 1.06 MB
- 1K segments: 7.6 ms, 10.38 MB
- 10K segments: 83.2 ms, 106.2 MB

## Optimizations Applied

### 1. Line Splitting (lib/hls/playlist.ex)
**Before:** Used regex `~r/\R/` for splitting lines
**After:** Use `String.split(["\r\n", "\r", "\n"], trim: true)`
**Impact:** Eliminates regex compilation and matching overhead

### 2. Attribute List Parsing (lib/hls/playlist/tag.ex)
**Before:** Converted string to codepoints list using `String.codepoints()`, then reduced with `Enum.reduce`
**After:** Direct binary pattern matching with recursive state machine
**Impact:** Avoids list allocation and codepoint conversion overhead

```elixir
# Old approach
data
|> String.codepoints()
|> Enum.reduce(state, fn cp, acc -> ... end)

# New approach
defp parse_attribute_list_binary(<<char::utf8, rest::binary>>, state, ...) do
  # Process character and recurse
end
```

### 3. Segment URI Fast Path (lib/hls/playlist.ex)
**Before:** Linear search through all unmarshalers for every line
**After:** Separate URI handler, check non-tag lines first before searching
**Impact:** Segment URIs (most common in media playlists) matched in O(1) instead of O(n)

```elixir
# Extract URI handler separately
{uri_handler, tag_handlers} = Enum.split_with(unmarshalers, fn u -> u.id() == :uri end)

# Fast path for segment URIs
matching_unmarshaler =
  if uri_handler && !String.starts_with?(line, "#") do
    uri_handler  # O(1) - no search needed
  else
    Enum.find(tag_handlers, fn unmarshaler -> unmarshaler.match?(line) end)
  end
```

### 4. Media Playlist Load Optimization (lib/hls/playlist/media.ex)
**Before:** Multiple passes with filter, map, sort, and multiple stream transforms
**After:** Combined filter+map in single reduce, fewer passes over data
**Impact:** Reduced intermediate allocations and iterations

## Test Coverage

All existing tests pass without modifications, ensuring backward compatibility.

```bash
mix test
# 3 doctests, 83 tests, 0 failures
```

## Future Optimization Opportunities

If further optimization is needed:

1. **Full unmarshaler map**: Build a prefix-based map for O(1) tag lookup (requires careful handling of edge cases)
2. **Streaming parser**: For very large playlists, implement streaming to avoid loading entire file in memory
3. **Parallel parsing**: For master playlists with many variants, parse segments in parallel
4. **Tag pooling**: Reuse tag structs to reduce allocation overhead

## Notes

- Optimizations maintain full RFC 8216 compliance
- All optimizations are backward compatible
- Memory improvements come from reduced intermediate allocations
- Speed improvements are most significant for media playlists with many segments
