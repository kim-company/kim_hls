# HLS
HTTP Live Streaming (HLS) library. Modules, variables and functionality is
bound to RFC 8216. This library provides an HLS.Tracker that follows an HLS
rendition sending a message to its parent process each time a new segment is
available.

This library is the base of our [HLS plugin for the Membrane
Framework](https://github.com/kim-company/membrane_hls_plugin) which is being
battle-tested with production workloads. Checkout its code and the tests for
some usage examples.

## Installation
```elixir
def deps do
  [
    {:kim_hls, github: "kim-company/kim_hls"}
  ]
end
```

Note that this library uses Tesla, you are responsible for choosing and
configuring its adapter.

## Gotchas
### On LFS (if tests are failing
Beware that fixtures are stored using the git LFS protocol. On debian, set it up
with
```
% sudo apt install git-lfs
# Within the repo
% git lfs install
% git lfs pull
```

If you add more fixture files, track them on LFS with `git lfs track <the
files>`.

## Copyright and License
Copyright 2022, [KIM Keep In Mind GmbH](https://www.keepinmind.info/)
Licensed under the [Apache License, Version 2.0](LICENSE)
