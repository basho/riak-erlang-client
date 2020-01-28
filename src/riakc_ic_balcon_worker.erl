%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2020, bet365
%%% @doc
%%%
%%% @end
%%% Created : 14. Jan 2020 15:31
%%%-------------------------------------------------------------------
-module(riakc_ic_balcon_worker).
-author("paulhunt").

-behaviour(gen_server).

%% API
-export([start_link/1, execute/2, change_connection/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(EXECUTE, execute).
-define(CHANGE_CONNECTION, change_connection).

-record(state, {
    riak_connection :: pid()
}).

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_link(WorkerArgs :: list()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(WorkerArgs) ->
    gen_server:start_link(?MODULE, [WorkerArgs], []).

-spec execute(Worker :: pid(), Fun :: function()) ->
    term().
execute(Worker, Fun) ->
    gen_server:call(Worker, {?EXECUTE, Fun}).

-spec change_connection(Worker :: pid(), Host :: string(), Port :: integer()) ->
    ok | {error, term()}.
change_connection(Worker, Host, Port) ->
    gen_server:call(Worker, {?CHANGE_CONNECTION, Host, Port}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([WorkerArgs]) ->
    process_flag(trap_exit, true),
    HostName = proplists:get_value(host, WorkerArgs),
    Port = proplists:get_value(port, WorkerArgs),
    case riakc_pb_socket:start_link(HostName, Port) of
        {ok, Pid} ->
            {ok, #state{riak_connection = Pid}};
        {error, Reason} ->
            %% TODO - Add logging for worker not starting
            {stop, {error, Reason}}
    end.

handle_call({?EXECUTE, Fun}, _From, State = #state{riak_connection = Pid}) ->
    Result = Fun(Pid),
    {reply, Result, State};
handle_call({?CHANGE_CONNECTION, Host, Port}, _From, State = #state{riak_connection = OldPid}) ->
    case riakc_pb_socket:start_link(Host, Port) of
        {ok, NewPid} ->
            riakc_pb_socket:stop(OldPid),
            NewState = State#state{riak_connection = NewPid},
            {reply, ok, NewState};
        {error, Reason} ->
            %% TODO - Add logging for new connection not being made
            {reply, {error, Reason}, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, State = #state{riak_connection = RiakConnection}) when RiakConnection == Pid ->
    %% TODO - Add logging here.
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
