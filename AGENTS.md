# Repository Guidelines

## Project Structure & Module Organization
- `lib/` contains the HLS library code, organized under `lib/hls/` (core modules like `packager.ex`, `tracker.ex`, and playlist/storage implementations).
- `test/` holds ExUnit tests, with fixtures in `test/fixtures/` and shared helpers in `test/support/`.
- `doc/` is the generated documentation output (`mix docs`).
- `bench/` contains benchmark scripts (dev-only).

## Build, Test, and Development Commands
- `mix deps.get` installs dependencies.
- `mix compile` compiles the library.
- `mix test` runs the full test suite.
- `mix test test/hls/some_test.exs` runs a specific test file.
- `mix test --only tag_name` runs tagged tests only.
- `mix format` formats code using `.formatter.exs`.
- `mix docs` generates API docs into `doc/`.

## Coding Style & Naming Conventions
- Use standard Elixir formatting via `mix format`; don’t hand-format.
- Indentation is 2 spaces (Elixir standard).
- Files follow module naming (e.g., `HLS.Packager` in `lib/hls/packager.ex`).
- Tests use `*_test.exs` and mirror module names where practical.

## Testing Guidelines
- Framework: ExUnit.
- Tests live under `test/hls/` with fixtures under `test/fixtures/`.
- Prefer unit tests for module behavior and integration tests for real HLS playlists.
- Tag slow or external tests and run with `mix test --only tag_name`.

## Commit & Pull Request Guidelines
- Recent commits use short, imperative, sentence-case subjects (e.g., “Fix local storage”, “Add warning…”).
- Keep commits focused and scoped to one change.
- PRs should include: a clear description, linked issues (if any), and test notes (`mix test` output or rationale for skipping).

## Agent-Specific Notes
- `CLAUDE.md` documents repository-specific development workflows and architecture references for automation tools.
