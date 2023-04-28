defmodule HLS.Segment do
  alias HLS.Playlist.Tag

  @type t :: %__MODULE__{
          uri: URI.t(),
          # Expressed in seconds.
          duration: float(),
          relative_sequence: pos_integer(),
          absolute_sequence: pos_integer() | nil
        }

  defstruct [:uri, :duration, :relative_sequence, :absolute_sequence, :from]

  @spec from_tags([Tag.t()]) :: t()
  def from_tags(tags) do
    sequence =
      Enum.reduce(tags, nil, fn
        tag, nil ->
          tag.sequence

        tag, seq ->
          if tag.sequence != seq do
            raise ArgumentError,
                  "Attempted creating Segment with tags belonging to different sequence windows: have #{inspect(tag.sequence)}, want #{inspect(seq)}"
          end

          seq
      end)

    duration = Enum.find(tags, fn tag -> tag.id == Tag.Inf.id() end)
    uri = Enum.find(tags, fn tag -> tag.id == Tag.SegmentURI.id() end)

    %__MODULE__{
      uri: uri.value,
      duration: duration.value,
      relative_sequence: sequence
    }
  end

  def new(duration, relative_sequence, absolute_sequence, extension) do
    %__MODULE__{
      duration: duration,
      relative_sequence: relative_sequence,
      absolute_sequence: absolute_sequence
    }
    |> fill_uri(extension)
  end

  @spec update_absolute_sequence(t, pos_integer()) :: t
  def update_absolute_sequence(segment, media_sequence) do
    %__MODULE__{segment | absolute_sequence: media_sequence + segment.relative_sequence}
  end

  def generate_next_segment(
        %__MODULE__{
          relative_sequence: relative_sequence,
          absolute_sequence: absolute_sequence,
          uri: %URI{path: path}
        },
        duration
      ) do
    %__MODULE__{
      duration: duration,
      relative_sequence: relative_sequence + 1,
      absolute_sequence: absolute_sequence + 1
    }
    |> fill_uri(Path.extname(path))
  end

  def fill_uri(segment = %__MODULE__{absolute_sequence: seq}, extension) do
    uri =
      seq
      |> to_string()
      |> String.pad_leading(5, "0")
      |> List.wrap()
      |> Enum.concat([extension])
      |> Enum.join()
      |> URI.new!()

    %__MODULE__{segment | uri: uri}
  end
end
