defmodule HLS.Packager.PlaylistFinishedError do
  defexception [:message]
end

defmodule HLS.Packager.ResumeError do
  defexception [:message]
end

defmodule HLS.Packager.PlaylistNotFoundError do
  defexception [:message]
end

defmodule HLS.Packager.UpsertError do
  defexception [:message]
end
