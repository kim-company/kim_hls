# RFC 8216 Compliance Test Plan

This document lists RFC 8216 compliance requirements and how we plan to
verify them in the test suite. Each item references the relevant section
and notes whether it is covered today or needs a new test.

## Playlist File Rules (RFC 8216 §4.2, §4.3.1, §4.3)

- **EXTM3U first line (MUST)**: assert first line is `#EXTM3U` for master
  and media playlists. Covered in `test/hls/packager_test.exs`.
- **No mixed playlist types (MUST)**: master playlists must not contain
  media segment tags; media playlists must not contain master tags.
  Add a test scanning output for forbidden tags.
- **Tag case sensitivity (MUST)**: tags must be uppercase.
  Add a test ensuring tags begin with uppercase `#EXT-`.
- **Attribute list formatting (MUST NOT contain whitespace)**:
  add a test that marshaled `EXT-X-STREAM-INF` and `EXT-X-MEDIA` lines
  have no spaces around commas or `=`.

## Media Playlist Tags (RFC 8216 §4.3.3)

- **EXT-X-TARGETDURATION present (REQUIRED)**:
  already covered in `test/hls/packager_test.exs`.
- **EXTINF per segment (REQUIRED)**:
  add a test that every segment has a preceding `#EXTINF`.
- **EXT-X-MEDIA-SEQUENCE before first segment (MUST)**:
  add a test verifying ordering.
- **EXT-X-DISCONTINUITY-SEQUENCE before first segment and before any
  EXT-X-DISCONTINUITY (MUST)**:
  add a test with discontinuities and assert ordering.
- **EXT-X-ENDLIST for VOD (MUST to signal end)**:
  add a test that `flush/1` in VOD mode includes `#EXT-X-ENDLIST`.
- **EXT-X-PLAYLIST-TYPE consistency across variants (MUST)**:
  add a test that all variants share the same type when set.

## Media Segment Tags (RFC 8216 §4.3.2)

- **EXTINF duration must be <= target duration (MUST)**:
  already covered in `test/hls/packager_test.exs`.
- **EXT-X-MAP in fMP4 playlists (MUST in fMP4 contexts)**:
  add a test that when an init section is used, the media playlist includes
  `#EXT-X-MAP` prior to the first segment (and after discontinuities).
- **EXT-X-PROGRAM-DATE-TIME format (SHOULD include TZ and ms)**:
  add a test validating ISO-8601 with timezone and millisecond precision.
- **No media segment tags in master playlists (MUST)**:
  covered by the mixed playlist type test above.

## Master Playlist Tags (RFC 8216 §4.3.4)

- **EXT-X-STREAM-INF followed by URI (MUST)**:
  already covered in `test/hls/packager_test.exs`.
- **EXT-X-MEDIA required attributes (MUST)**:
  already covered in `test/hls/packager_test.exs`.
- **CODECS attribute includes all codecs across renditions (MUST)**:
  add a test using video + audio alternative renditions that validates
  merged codecs.
- **No master tags in media playlists (MUST)**:
  covered by the mixed playlist type test above.

## Variant Stream Constraints (RFC 8216 §6.2.4)

- **Matching content has matching timestamps (MUST)**:
  add a test that `sync/2` returns warnings and no write actions when
  timestamps diverge beyond tolerance.
- **Matching discontinuity sequence numbers (MUST)**:
  add a test that discontinuities are aligned across all tracks and the
  sequence numbers stay aligned after sliding window eviction.
- **Same target duration across variants (MUST, with exceptions)**:
  add a test that variant playlists share target duration.
- **Consistent EXT-X-PLAYLIST-TYPE across variants (MUST)**:
  add a test that type matches for all variant playlists.
- **Consistent EXT-X-PROGRAM-DATE-TIME across variants (MUST)**:
  add a test that PDT tags exist for all and align via DTS/PTS.

## Out of Scope (Payload-Level)

The packager does not inspect segment payloads, so these RFC requirements
are out of scope unless we add fixture-based payload validation:

- MPEG-TS PAT/PMT continuity and IDR presence.
- fMP4 box constraints (ftyp/moov/trak/mvex, tfdt).
- ID3 PRIV timestamping for packed audio.
- WebVTT cue timing and X-TIMESTAMP-MAP constraints.
