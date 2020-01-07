%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2020, bet365
%%% @doc
%%%
%%% @end
%%% Created : 07. Jan 2020 13:27
%%%-------------------------------------------------------------------
-module(riakc_ic_balcon_pool_gs).
-author("paulhunt").

-behaviour(gen_server).

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================
%% TODO - Update start_link spec with Mappings type.
-spec(start_link(Name :: atom(), Mappings :: any(), RegisterModule :: atom(), StatisticModule :: atom()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Name, Mappings, RegisterModule, StatisticModule) ->
    AllPoolsSettings = riakc_config:get_pools(),
    PoolSettings = lists:keyfind(Name, 1, AllPoolsSettings),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Name, PoolSettings, Mappings, RegisterModule, StatisticModule], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
%% TODO - Complete implementation for init function.
init([Name, PoolSettings, _Mappings, _RegisterModule, _StatisticModule]) ->
    case PoolSettings of
        {Name, {_PoolArgs, _HostNames, _WorkerArgs}} ->
            {ok, #state{}};
        false ->
            {error, invalid_pool_settings}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

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
