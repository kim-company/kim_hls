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

- **HLS.Packager** - Pure functional module for HLS playlist generation and segment handling. Returns actions for the caller to execute, providing explicit control over I/O operations.

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
# Initialize packager state
{:ok, state} = HLS.Packager.new(
  manifest_uri: URI.new!("stream.m3u8"),
  max_segments: 10  # Optional: enables sliding window
)

# Add a variant stream
{state, []} = HLS.Packager.add_track(state, "video_480p",
  stream: %HLS.VariantStream{
    bandwidth: 800_000,
    resolution: {854, 480},
    codecs: ["avc1.64001e"]
  },
  segment_extension: ".ts",
  target_segment_duration: 6.0
)

# Add segment (returns upload action)
{state, [action]} = HLS.Packager.put_segment(state, "video_480p", duration: 6.0)

# Caller uploads the segment
storage = HLS.Storage.File.new(base_path: "./output")
:ok = HLS.Storage.put(storage, action.uri, segment_data)

# Confirm upload (may return playlist write actions)
{state, actions} = HLS.Packager.confirm_upload(state, action.id)

# Execute write actions
Enum.each(actions, fn
  %HLS.Packager.Action.WritePlaylist{uri: uri, content: content} ->
    HLS.Storage.put(storage, uri, content)
end)

# Sync and flush to create VOD playlist
{state, actions} = HLS.Packager.flush(state)
Enum.each(actions, &execute_action(&1, storage))
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

Key options for `HLS.Packager.new/1`:

- `manifest_uri` (required) - URI of the master playlist
- `max_segments` - Maximum segments to retain (enables sliding window mode with automatic cleanup)

For resuming from existing playlists, use `HLS.Packager.resume/1` with loaded playlist data.

## Copyright and License

Copyright 2024, [KIM Keep In Mind GmbH](https://www.keepinmind.info/)

Licensed under the [Apache License, Version 2.0](LICENSE)