%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019, bet365
%%% @doc
%%%
%%% @end
%%% Created : 13. Dec 2019 13:39
%%%-------------------------------------------------------------------
-module(riakc_ic).
-author("paulhunt").

-behaviour(gen_server).

-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

%% API
-export([
    start_link/0,
    update_nodes_list/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(ETS_IC_INFO_TABLE, ic_information).
-define(UPDATE_NODES, update_nodes).

-define(is_new_timestamp(CurrentTimestamp, NewTimestamp), NewTimestamp > CurrentTimestamp orelse
    CurrentTimestamp == undefined).

-record(riakc_ic_information, {
    nodes                   :: list(atom()),
    ring                    :: riak_pb_ring(),
    default_bucket_props    :: list({atom(), term()})
}).

-record(state, {
    update_timestamp :: integer() | undefined
}).

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec update_nodes_list(NewTimestamp :: integer(), Nodes :: list(atom())) ->
    ok.
update_nodes_list(NewTimestamp, Nodes) when erlang:is_integer(NewTimestamp) andalso erlang:is_list(Nodes) ->
    gen_server:cast(?SERVER, {?UPDATE_NODES, NewTimestamp, Nodes}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    InitInformation = get_init_information(),
    ets:new(?ETS_IC_INFO_TABLE, [set, named_table]),
    ets:insert(?ETS_IC_INFO_TABLE, InitInformation),
    {ok, #state{update_timestamp = undefined}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({?UPDATE_NODES, NewTimestamp, NodesList}, State) ->
    CurrentTimestamp = State#state.update_timestamp,
    handle_update_nodes(CurrentTimestamp, NewTimestamp, NodesList),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_init_information() ->
    %% Test connection here to get things working
    {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 10017),
    {ok, Nodes} = riakc_pb_socket:get_nodes(Pid),
    {ok, Ring} = riakc_pb_socket:get_ring(Pid),
    {ok, DefaultBucketProps} = riakc_pb_socket:get_default_bucket_props(Pid),
    #riakc_ic_information{nodes = Nodes, ring = Ring, default_bucket_props = DefaultBucketProps}.

handle_update_nodes(CurrentTimestamp, NewTimestamp, NodesList) when ?is_new_timestamp(CurrentTimestamp, NewTimestamp) ->
    %% TODO - currently assumes ic info table has been initialised. Need to think about what happens when the table can't initialise properly for whatever reason
    [IcInfo] = ets:lookup(?ETS_IC_INFO_TABLE, riakc_ic_information),
    NewIcInfo = IcInfo#riakc_ic_information{nodes = NodesList},
    ets:insert(?ETS_IC_INFO_TABLE, NewIcInfo),
    ok;
handle_update_nodes(_CurrentTimestamp, _NewTimestamp, _NodesList) ->
    ok.