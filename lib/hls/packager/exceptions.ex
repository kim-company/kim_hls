defmodule HLS.Packager.PlaylistFinishedError do
  defexception [:message]
end

defmodule HLS.Packager.ResumeError do
  defexception [:message]
end

defmodule HLS.Packager.PlaylistNotFoundError do
  defexception [:message]
end

defmodule HLS.Packager.AddTrackError do
  defexception [:message]
end

defmodule HLS.Packager.TrackNotFoundError do
  defexception [:message]
end
