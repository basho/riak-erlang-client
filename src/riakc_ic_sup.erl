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
-define(BALCON_MOD, riakc_ic_balcon).

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
    BalConSpec = riakc_ic_balcon_spec(RegistrationModule, StatisticsModule),
    ChildSpecs = [BalConSpec],
    {ok, {{one_for_one, 1, 5}, ChildSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
riakc_ic_balcon_spec(RegistrationModule, StatisticModule) ->
    ResourcesDict = RegistrationModule:create_pool_mapping(),
    Mappings = dict:fold(fun(PoolName, Resource, Acc) ->
        [{PoolName, Resource}|Acc]
                         end, [], ResourcesDict),
    Args = [Mappings, RegistrationModule, StatisticModule],
    {?BALCON_MOD, {?BALCON_MOD, start_link, Args}, permanent, 5000, worker, [?BALCON_MOD]}.
