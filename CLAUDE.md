# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing
- `mix test` - Run all tests
- `mix test test/path/to/specific_test.exs` - Run a specific test file
- `mix test --only tag_name` - Run tests with specific tags

### Code Quality
- `mix format` - Format code according to `.formatter.exs` configuration
- `mix deps.get` - Install dependencies
- `mix compile` - Compile the project

### Documentation
- `mix docs` - Generate documentation (outputs to `doc/` directory)

## Project Architecture

This is an Elixir library for HTTP Live Streaming (HLS) that implements RFC 8216 specifications. The library is structured around three main components:

### Core Components

1. **HLS.Packager** (`lib/hls/packager.ex`) - Pure functional packager that manages playlist state
   - Returns actions the caller executes (upload segments, write playlists, delete files)
   - Handles track addition, segment uploads, synchronization, and flush
   - Manages both variant streams and alternative renditions
   - Supports resuming from existing playlists

2. **HLS.Tracker** (`lib/hls/tracker.ex`) - Monitors HLS renditions and notifies about new segments
   - GenServer that periodically polls media playlists
   - Sends messages to parent process when new segments are available
   - Handles live streaming scenarios with proper buffering

3. **HLS.Storage** (`lib/hls/storage.ex`) - Protocol for storage backends
   - Defines get/put/delete operations for playlist and segment storage
   - Implementations available for File system (`storage/file.ex`) and HTTP/S3 (`storage/req.ex`)

### Supporting Modules

- **HLS.Playlist** - Handles playlist marshaling/unmarshaling with separate modules for Master and Media playlists
- **HLS.Playlist.Tag** - Individual HLS tag implementations (EXT-X-* tags)
- **HLS.Segment** - Represents HLS segments with duration, URI, and optional init sections
- **HLS.VariantStream** & **HLS.AlternativeRendition** - Stream representation structures

### Key Design Patterns

- Uses GenServer for stateful components (Tracker)
- Packager is a pure state machine that returns explicit actions
- Protocol-based storage abstraction allows different backends
- Async task-based segment uploads with proper error handling
- Telemetry integration for monitoring (optional dependency)
- Separation of master playlist (tracks/streams) and media playlists (segments)

### Test Structure

Tests are organized in `test/hls/` with fixtures in `test/fixtures/` including real HLS streams for testing. The test suite includes both unit tests for individual modules and integration tests with actual HLS content.

### Key Features

- **Sliding Window Support** - Configurable segment retention with automatic cleanup of old segments from both playlists and storage
- **Smart Flush Behavior** - When `max_segments` is configured, `flush/1` performs complete storage cleanup instead of creating VOD playlists
- **Storage Abstraction** - Protocol-based design supports multiple storage backends
- **Live Streaming** - Real-time segment tracking and playlist updates
- **Telemetry Integration** - Optional monitoring and metrics collection

### Configuration Options

Key configuration options for `HLS.Packager.new/1` and `HLS.Packager.resume/1`:

- `manifest_uri: URI.t()` - Required for `new/1`, identifies the master playlist URI.
- `max_segments: nil | pos_integer()` - Maximum segments to keep in media playlists. When exceeded, old segments are removed from playlists and storage. Also changes `flush/1` behavior to perform complete cleanup instead of creating VOD playlists. Default: `nil` (unlimited)
- `master_playlist: HLS.Playlist.Master.t()` - Required for `resume/1`, loaded by the caller.
- `media_playlists: [HLS.Playlist.Media.t()]` - Required for `resume/1`, loaded by the caller.

### Important Behavioral Changes

When `max_segments` is configured:
- **Sync Operations**: Old segments are automatically removed from playlists and storage when the limit is exceeded
- **Flush Operations**: `HLS.Packager.flush/1` performs complete storage cleanup (deletes all segments and playlists) instead of creating VOD playlists
- **Use Case**: Designed for live streaming scenarios where storage efficiency is prioritized over content preservation

### Dependencies

- `req` (optional) - HTTP client for S3/HTTP storage backend
- `telemetry` (optional) - Metrics and monitoring
- `ex_doc` (dev only) - Documentation generation

The library is designed to be lightweight with minimal required dependencies, making optional features truly optional.
