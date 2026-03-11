defmodule PQ do
  use GenServer

  @moduledoc """
  A persistent queue
  """

  # state structure

  @default_segment_size 10
  @default_number_of_segments 5

  defstruct name: "pq",
            base_dir: File.cwd!(),
            segment_size: @default_segment_size,
            number_of_segments: @default_number_of_segments,
            enqueue_count: 0,
            dequeue_count: 0,
            first_segment: [],
            last_segment: [],
            first_segment_id: 0,
            last_segment_id: 0

  # initialization

  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name)
    base_dir = Keyword.get(opts, :base_dir)
    segment_size = Keyword.get(opts, :segment_size)
    number_of_segments = Keyword.get(opts, :number_of_segments)

    initial_state =
      %__MODULE__{}
      |> Map.update!(:name, fn default -> name || default end)
      |> Map.update!(:base_dir, fn default -> base_dir || default end)
      |> Map.update!(:segment_size, fn default -> segment_size || default end)
      |> Map.update!(:number_of_segments, fn default -> number_of_segments || default end)
      |> load_state()

    {:ok, initial_state}
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, opts)
  end

  def start(opts) do
    GenServer.start(__MODULE__, opts, opts)
  end

  # client API

  def enqueue(pq, msg) when is_map(msg) do
    GenServer.call(pq, {:enqueue, msg})
  end

  def dequeue(pq, ack \\ true) do
    GenServer.call(pq, {:dequeue, ack})
  end

  def head(pq) do
    GenServer.call(pq, :head)
  end

  def empty?(pq) do
    GenServer.call(pq, :empty?)
  end

  def count(pq) do
    GenServer.call(pq, :count)
  end

  def reset(pq) do
    GenServer.call(pq, :reset)
  end

  # server API

  @impl true
  def handle_call({:enqueue, msg}, {_sender, _call}, state) do
    record = %{id: state.enqueue_count, ts: to_string(DateTime.utc_now()), msg: msg}
    json = JSON.encode_to_iodata!(record)
    log_enqueue(state, json)

    new_state =
      case segments_count(state) do
        1 ->
          if state.enqueue_count + 1 == state.last_segment_id + state.segment_size do
            %{
              state
              | enqueue_count: state.enqueue_count + 1,
                last_segment: [],
                last_segment_id: state.last_segment_id + state.segment_size
            }
          else
            %{
              state
              | enqueue_count: state.enqueue_count + 1,
                first_segment: state.first_segment ++ [record]
            }
          end

        _ ->
          if state.enqueue_count + 1 == state.last_segment_id + state.segment_size do
            %{
              state
              | enqueue_count: state.enqueue_count + 1,
                last_segment: [],
                last_segment_id: state.last_segment_id + state.segment_size
            }
          else
            %{
              state
              | enqueue_count: state.enqueue_count + 1,
                last_segment: state.last_segment ++ [record]
            }
          end
      end

    {:reply, true, new_state}
  end

  @impl true
  def handle_call({:dequeue, ack}, {_sender, _call}, state) do
    if queued_count(state) == 0 do
      {:reply, nil, state}
    else
      [head | rest] = state.first_segment
      record = %{id: head.id, ts: to_string(DateTime.utc_now()), ack: ack}
      json = JSON.encode_to_iodata!(record)
      log_dequeue(state, json)

      new_state =
        if rest == [] && segments_count(state) > 1 do
          case segments_count(state) do
            2 ->
              %{
                state
                | first_segment: state.last_segment,
                  first_segment_id: state.last_segment_id,
                  last_segment: [],
                  last_segment_id: 0,
                  dequeue_count: state.dequeue_count + 1
              }

            _ ->
              new_segment_id = state.first_segment_id + state.segment_size

              %{
                state
                | first_segment: load_segment(state, new_segment_id),
                  first_segment_id: new_segment_id,
                  dequeue_count: state.dequeue_count + 1
              }
          end
        else
          %{state | first_segment: rest, dequeue_count: state.dequeue_count + 1}
        end

      {:reply, head, new_state}
    end
  end

  @impl true
  def handle_call(:head, {_sender, _call}, %__MODULE__{first_segment: first_segment} = state) do
    {:reply, List.first(first_segment), state}
  end

  @impl true
  def handle_call(:empty?, {_sender, _call}, state) do
    {:reply, queued_count(state) == 0, state}
  end

  @impl true
  def handle_call(:count, {_sender, _call}, state) do
    {:reply, queued_count(state), state}
  end

  @impl true
  def handle_call(:reset, {_sender, _call}, state) do
    base_dir_path = queue_base_dir(state)

    File.ls!(base_dir_path)
    |> Enum.filter(fn file -> Path.extname(file) == ".ndjson" end)
    |> Enum.map(fn file -> File.rm!(Path.join(base_dir_path, file)) end)

    {:reply,
     %{
       state
       | enqueue_count: 0,
         dequeue_count: 0,
         first_segment: [],
         last_segment: [],
         first_segment_id: 0,
         last_segment_id: 0
     }, state}
  end

  # internals

  def queued_count(
        %__MODULE__{enqueue_count: enqueue_count, dequeue_count: dequeue_count} = _state
      ) do
    enqueue_count - dequeue_count
  end

  def segments_count(
        %__MODULE__{
          first_segment_id: first_segment_id,
          last_segment_id: last_segment_id,
          segment_size: segment_size
        } = _state
      ) do
    div(last_segment_id - first_segment_id, segment_size) + 1
  end

  def load_state(state) do
    base_dir_path = queue_base_dir(state)

    files =
      File.ls!(base_dir_path) |> Enum.filter(fn name -> String.match?(name, ~r/^.*.ndjson$/) end)

    last_dequeue_file =
      files
      |> Enum.filter(fn name -> String.match?(name, ~r/^dequeue-.*.ndjson$/) end)
      |> Enum.max_by(fn name -> segment_id_from_file(name) end, fn -> nil end)

    last_dequeue_segment_id =
      if last_dequeue_file, do: segment_id_from_file(last_dequeue_file), else: 0

    dequeue_count =
      if last_dequeue_file,
        do: (read_last_ndjson(Path.join(base_dir_path, last_dequeue_file)) |> Map.get("id")) + 1,
        else: 0

    last_enqueue_file =
      files
      |> Enum.filter(fn name -> String.match?(name, ~r/^enqueue-.*.ndjson$/) end)
      |> Enum.max_by(fn name -> segment_id_from_file(name) end, fn -> nil end)

    last_enqueue_segment_id =
      if last_enqueue_file, do: segment_id_from_file(last_enqueue_file), else: 0

    last_enqueue_segment =
      if last_enqueue_file, do: load_segment(state, last_enqueue_segment_id), else: []

    enqueue_count =
      if Enum.empty?(last_enqueue_segment),
        do: 0,
        else: (List.last(last_enqueue_segment) |> Map.get("id")) + 1

    if last_dequeue_segment_id == last_enqueue_segment_id do
      last_enqueue_segment =
        last_enqueue_segment |> Enum.drop(rem(dequeue_count, state.segment_size))

      %{
        state
        | enqueue_count: enqueue_count,
          dequeue_count: dequeue_count,
          first_segment: last_enqueue_segment,
          last_segment: [],
          first_segment_id: last_enqueue_segment_id,
          last_segment_id: 0
      }
    else
      last_dequeue_segment =
        load_segment(state, last_dequeue_segment_id)
        |> Enum.drop(rem(dequeue_count, state.segment_size))

      %{
        state
        | enqueue_count: enqueue_count,
          dequeue_count: dequeue_count,
          first_segment: last_dequeue_segment,
          last_segment: last_enqueue_segment,
          first_segment_id: last_dequeue_segment_id,
          last_segment_id: last_enqueue_segment_id
      }
    end
  end

  def load_segment(state, segment_id) do
    path = Path.join(queue_base_dir(state), "enqueue-#{segment_id}.ndjson")
    File.stream!(path) |> Enum.map(fn line -> JSON.decode!(line) end)
  end

  def log_enqueue(
        %__MODULE__{enqueue_count: enqueue_count, segment_size: segment_size} = state,
        json
      ) do
    segment_id = div(enqueue_count, segment_size)
    path = Path.join(queue_base_dir(state), "enqueue-#{segment_id}.ndjson")
    File.write!(path, [json, "\n"], [:append])
  end

  def log_dequeue(
        %__MODULE__{dequeue_count: dequeue_count, segment_size: segment_size} = state,
        json
      ) do
    segment_id = div(dequeue_count, segment_size)
    path = Path.join(queue_base_dir(state), "dequeue-#{segment_id}.ndjson")
    File.write!(path, [json, "\n"], [:append])
  end

  def gc_unused_segments(state) do
    state
  end

  def queue_base_dir(%__MODULE__{name: name, base_dir: base_dir} = _state) do
    path = Path.join(base_dir, to_string(name))

    if !File.exists?(path) do
      File.mkdir_p!(path)
    end

    path
  end

  def segment_id_from_file(name) do
    Path.rootname(name) |> String.split("-") |> List.last() |> String.to_integer()
  end

  # IO support

  def read_all_ndjson(file) do
    File.stream!(file) |> Enum.map(fn line -> JSON.decode!(line) end)
  end

  def read_last_ndjson(file) do
    File.stream!(file) |> Enum.reduce(nil, fn line, _last -> line end) |> JSON.decode!()
  end

  def append_ndjson(io_data, file) do
    File.write(file, [io_data, "\n"], [:append])
  end
end
