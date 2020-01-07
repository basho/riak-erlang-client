%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2020, bet365
%%% @doc
%%%
%%% @end
%%% Created : 07. Jan 2020 12:24
%%%-------------------------------------------------------------------
-module(riakc_config).
-author("paulhunt").

%% API
-export([
    get_registration_module/0, get_registration_module/1,
    get_statistics_module/0, get_statistics_module/1,
    get_pools/0, get_pools/1
]).

-define(APP_NAME, riakc).
-define(REGISTRATION_MODULE, registration_module).
-define(STATISTICS_MODULE, statistics_module).
-define(POOL, pool).

%%%===================================================================
%%% API functions
%%%===================================================================
-spec get_registration_module() ->
    atom() | undefined.
get_registration_module() ->
    get_config_var(?APP_NAME, ?REGISTRATION_MODULE).

-spec get_registration_module(Default :: atom()) ->
    atom().
get_registration_module(Default) ->
    get_config_var(?APP_NAME, ?REGISTRATION_MODULE, Default).

-spec get_statistics_module() ->
    atom() | undefined.
get_statistics_module() ->
    get_config_var(?APP_NAME, ?STATISTICS_MODULE).

-spec get_statistics_module(Default :: atom()) ->
    atom().
get_statistics_module(Default) ->
    get_config_var(?APP_NAME, ?STATISTICS_MODULE, Default).

-spec get_pools() ->
    list() | undefined.
get_pools() ->
    get_config_var(?APP_NAME, ?POOL).

-spec get_pools(Default :: list()) ->
    list().
get_pools(Default) ->
    get_config_var(?APP_NAME, ?POOL, Default).

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_config_var(Application, VarName) ->
    case application:get_env(Application, VarName) of
        {ok, Var} ->
            Var;
        undefined ->
            undefined
    end.

get_config_var(Application, VarName, Default) ->
    application:get_env(Application, VarName, Default).