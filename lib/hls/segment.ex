defmodule HLS.Segment do
  alias HLS.Playlist.Tag

  @type t :: %__MODULE__{
          uri: URI.t(),
          # Expressed in seconds.
          ref: reference(),
          duration: float(),
          relative_sequence: pos_integer(),
          absolute_sequence: pos_integer() | nil,
          from: pos_integer() | nil,
          discontinuity: boolean()
        }

  defstruct [:uri, :duration, :relative_sequence, :absolute_sequence, :from, :ref, :discontinuity]

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

    discontinuity =
      Enum.any?(tags, fn tag ->
        tag.id == :ext_x_discontinuity
      end)

    %__MODULE__{
      uri: uri.value,
      duration: duration.value,
      relative_sequence: sequence,
      ref: make_ref(),
      discontinuity: discontinuity
    }
  end

  @spec update_absolute_sequence(t, pos_integer()) :: t
  def update_absolute_sequence(segment, media_sequence) do
    %__MODULE__{segment | absolute_sequence: media_sequence + segment.relative_sequence}
  end
end
