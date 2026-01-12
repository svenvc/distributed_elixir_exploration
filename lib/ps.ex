defmodule PS do
  use GenServer
  require Logger

  @moduledoc """
  A simple publish-subscribe server
  """

  @impl true
  def init(_) do
    {:ok, %{}}
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, nil, opts)
  end

  @impl true
  def handle_call({:subscribe, topic}, {sender, _call}, state) do
    new_state =
      Map.update(state, topic, [sender], fn subscribers ->
        if Enum.member?(subscribers, sender) do
          subscribers
        else
          [sender | subscribers]
        end
      end)

    Process.monitor(sender)

    {:reply, true, new_state}
  end

  @impl true
  def handle_call(["subscribe", topic], from, state) do
    handle_call({:subscribe, topic}, from, state)
  end

  @impl true
  def handle_call({:unsubscribe, topic}, {sender, _call}, state) do
    new_state =
      Map.update(state, topic, [], fn subscribers ->
        Enum.reject(subscribers, fn x -> x == sender end)
      end)

    {:reply, true, new_state}
  end

  @impl true
  def handle_call(["unsubscribe", topic], from, state) do
    handle_call({:unsubscribe, topic}, from, state)
  end

  @impl true
  def handle_call({:subscribers, topic}, _sender, state) do
    subscribers = Map.get(state, topic, [])

    {:reply, subscribers, state}
  end

  @impl true
  def handle_call(["subscribers", topic], from, state) do
    handle_call({:subscribers, topic}, from, state)
  end

  @impl true
  def handle_call({:broadcast, topic, message}, {sender, _call}, state) do
    Map.get(state, topic, [])
    |> Enum.each(fn subscriber ->
      if subscriber != sender do
        Process.send(subscriber, message, [])
      end
    end)

    {:reply, true, state}
  end

  @impl true
  def handle_call(["broadcast", topic, message], from, state) do
    handle_call({:broadcast, topic, message}, from, state)
  end

  @impl true
  def handle_call(_, _, state) do
    {:reply, :not_implemented, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, process, reason}, state) do
    Logger.debug("DOWN #{inspect(process)} for reason #{inspect(reason)}, unsubscribing")

    new_state =
      Map.new(state, fn {topic, subscribers} ->
        {topic, Enum.reject(subscribers, fn subscriber -> subscriber == process end)}
      end)

    {:noreply, new_state}
  end
end
