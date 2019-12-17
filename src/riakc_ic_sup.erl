%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Dec 2019 16:03
%%%-------------------------------------------------------------------
-module(riakc_ic_sup).
-author("paulhunt").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(WORKER_MOD, riakc_ic_worker).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init([]) ->
    Pools = application:get_env(riakc, pools),
    PoolSpecs = get_pool_specs(Pools),
    {ok, {{one_for_one, 10, 10}, PoolSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_pool_specs(undefined) ->
    [];
get_pool_specs({ok, Pools}) ->
    lists:map(fun({Name, SizeArgs, WorkerArgs}) ->
        PoolArgs = [{name, {local, Name}}, {worker_module, ?WORKER_MOD}] ++ SizeArgs,
        poolboy:child_spec(Name, PoolArgs, WorkerArgs)
    end, Pools).
