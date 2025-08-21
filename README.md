# HLS

![Hex.pm Version](https://img.shields.io/hexpm/v/kim_hls)

HTTP Live Streaming (HLS) library implementing RFC 8216 specifications.

## Installation

Add `kim_hls` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:kim_hls, "~> 2.5"}
  ]
end
```

## Architecture

The library is structured around three main components:

### Core Components

- **HLS.Packager** - Central GenServer managing HLS playlist generation and segment handling. Manages master and media playlists, track addition, segment uploads, and synchronization.

- **HLS.Tracker** - GenServer monitoring HLS renditions, polling media playlists and notifying about new segments for live streaming scenarios.

- **HLS.Storage** - Protocol defining storage backend operations (get/put/delete). Includes file system and HTTP/S3 implementations.

### Supporting Modules

- **HLS.Playlist** - Playlist marshaling/unmarshaling with Master and Media playlist modules
- **HLS.Playlist.Tag** - Individual HLS tag implementations (EXT-X-* tags)
- **HLS.Segment** - Represents HLS segments with duration, URI, and optional init sections
- **HLS.VariantStream** & **HLS.AlternativeRendition** - Stream representation structures

## Usage

### Basic Packager Usage

```elixir
# Start a packager
{:ok, pid} = HLS.Packager.start_link(storage: %HLS.Storage.File{base_path: "./output"})

# Add a variant stream
variant = %HLS.VariantStream{uri: "stream_480p.m3u8", bandwidth: 800_000}
:ok = HLS.Packager.add_variant_stream(pid, variant)

# Upload segments
segment = %HLS.Segment{duration: 6.0, uri: "segment_001.ts"}
:ok = HLS.Packager.upload_segment(pid, "stream_480p.m3u8", segment, segment_data)

# Flush to create VOD playlist
:ok = HLS.Packager.flush(pid)
```

### Live Stream Tracking

```elixir
# Start tracking a live stream
{:ok, pid} = HLS.Tracker.start_link(
  playlist_uri: "https://example.com/live.m3u8",
  storage: storage_backend,
  parent: self()
)

# Receive notifications
receive do
  {:hls_tracker, :new_segment, segment} -> 
    IO.puts("New segment: #{segment.uri}")
end
```

### Configuration Options

Key options for `HLS.Packager.start_link/1`:

- `max_segments` - Maximum segments to retain (enables sliding window)
- `resume_finished_tracks` - Resume finished playlists on startup
- `restore_pending_segments` - Restore pending segments on startup

## Copyright and License

Copyright 2024, [KIM Keep In Mind GmbH](https://www.keepinmind.info/)

Licensed under the [Apache License, Version 2.0](LICENSE)