# HLS
![Hex.pm Version](https://img.shields.io/hexpm/v/kim_hls)

HTTP Live Streaming (HLS) library. Modules, variables and functionality is
bound to RFC 8216.

This library provides an `HLS.Tracker` that follows an HLS
rendition sending a message to its parent process each time a new segment is
available.

The `HLS.Packager` module is responsible for managing and generating media and master playlists
for HTTP Live Streaming (HLS). It handles various tasks such as loading and saving playlists,
inserting new streams, uploading segments and maintaining synchronization points for different streams.

This library is the base of our [HLS plugin for the Membrane
Framework](https://github.com/kim-company/membrane_hls_plugin) which is being
battle-tested with production workloads. Checkout its code and the tests for
some usage examples.

## Installation
```elixir
def deps do
  [
    {:kim_hls, "~> 1.0"}
  ]
end
```

## Copyright and License
Copyright 2024, [KIM Keep In Mind GmbH](https://www.keepinmind.info/)
Licensed under the [Apache License, Version 2.0](LICENSE)
