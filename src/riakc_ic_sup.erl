%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019, bet365
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
-define(BALCON_POOL_GS_MOD, riakc_ic_balcon_pool_gs).

%%%===================================================================
%%% API functions
%%%===================================================================
-spec start_link() ->
    {ok, pid()} | ignore | {error, term()}.
start_link() ->
    RegistrationModule = riakc_config:get_registration_module(),
    StatisticsModule = riakc_config:get_statistics_module(),
    supervisor:start_link({local, ?SERVER}, ?MODULE, [RegistrationModule, StatisticsModule]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init([RegistrationModule, StatisticsModule]) ->
    PoolSpecs = pools_to_spec(RegistrationModule, StatisticsModule),
    ChildSpecs = [] ++ PoolSpecs,
    {ok, {{one_for_one, 1, 5}, ChildSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
pools_to_spec(RegistrationModule, StatisticsModule) ->
    ResourcesDict = RegistrationModule:create_pool_mapping(),
    dict:fold(fun(PoolName, Resource, Acc) ->
        Args = [PoolName, Resource, RegistrationModule, StatisticsModule],
        MFA = {?BALCON_POOL_GS_MOD, start_link, [Args]},
        [{PoolName, MFA, permanent, 5000, worker, [?BALCON_POOL_GS_MOD]} | Acc]
    end, [], ResourcesDict).
