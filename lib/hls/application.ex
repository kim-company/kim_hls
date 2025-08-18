defmodule HLS.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Task.Supervisor, name: HLS.Task.Supervisor}
    ]

    opts = [strategy: :one_for_one, name: HLS.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
