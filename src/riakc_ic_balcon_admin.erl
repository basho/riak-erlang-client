%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2020, bet365
%%% @doc
%%%
%%% @end
%%% Created : 10. Jan 2020 11:54
%%%-------------------------------------------------------------------
-module(riakc_ic_balcon_admin).
-author("paulhunt").
-include("riakc_ic.hrl").

-behaviour(gen_server).

%% API
-export([start_link/3, get_poolboy_pid/1, get_poolboy_pid/2, get_admin_poolboy_pid/1, get_admin_poolboy_pid/2,
    get_pool_info/1, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(BALCON_WORKER_MOD, riakc_ic_balcon_worker).
-define(GET_POOLBOY_PID, get_poolboy_pid).
-define(GET_ADMIN_POOLBOY_PID, get_admin_poolboy_pid).
-define(GET_POOL_NAME, get_pool_name).
-define(GET_POOL_INFO, get_pool_info).

-define(valid_config_vars(ConnectionsList, Size, AdminSize, MaxOverflow), is_list(ConnectionsList) andalso
    ConnectionsList /= [] andalso Size /= undefined andalso AdminSize /= undefined andalso MaxOverflow /= undefined).

-define(all_defined(Size, MaxOverflow, ConnectionsList), Size /= undefined andalso MaxOverflow /= undefined andalso
    ConnectionsList /= undefined).

-record(sub_pool, {
    sub_pool_name       :: atom(),
    poolboy_pid         :: pid(),
    admin_poolboy_pid   :: pid(),
    host                :: string(),
    port                :: integer(),
    size                :: integer()
}).

-record(state, {
    pool_name           :: atom(),
    ordered_sub_pools   :: list(#sub_pool{})
}).

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link(Name :: atom(), AdminName :: atom(), Args :: list(term())) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Name, AdminName, Args) ->
    gen_server:start_link({local, AdminName}, ?MODULE, [Name, Args], []).

-spec get_poolboy_pid(AdminPid :: pid()) ->
    pid().
get_poolboy_pid(AdminPid) ->
    gen_server:call(AdminPid, ?GET_POOLBOY_PID).

-spec get_poolboy_pid(AdminPid :: pid(), TargetHost :: string()) ->
    pid() | {error, term()}.
get_poolboy_pid(AdminPid, TargetHost) ->
    gen_server:call(AdminPid, {?GET_POOLBOY_PID, TargetHost}).

-spec get_admin_poolboy_pid(AdminPid :: pid()) ->
    pid().
get_admin_poolboy_pid(AdminPid) ->
    gen_server:call(AdminPid, ?GET_ADMIN_POOLBOY_PID).

-spec get_admin_poolboy_pid(AdminPid :: pid(), TargetHost :: string()) ->
    pid() | {error, term()}.
get_admin_poolboy_pid(AdminPid, TargetHost) ->
    gen_server:call(AdminPid, {?GET_ADMIN_POOLBOY_PID, TargetHost}).

-spec get_pool_info(AdminPid :: pid()) ->
    ok.
get_pool_info(AdminPid) ->
    %% TODO - Decide whether this function returns the pools info or outputs it and returns okay.
    gen_server:call(AdminPid, ?GET_POOL_INFO).

-spec stop(Name :: atom()) ->
    ok.
stop(Name) ->
    gen_server:stop(Name).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Name, Args]) ->
    process_flag(trap_exit, true),
    Size = proplists:get_value(size, Args),
    AdminSize = proplists:get_value(admin_size, Args),
    MaxOverflow = proplists:get_value(max_overflow, Args),
    ConnectionsList = proplists:get_value(connections, Args),
    init_admin(Name, Size, AdminSize, MaxOverflow, ConnectionsList).

handle_call(?GET_POOLBOY_PID, _From, State) ->
    {Result, NewState} = handle_get_poolboy_pid(State),
    {reply, Result, NewState};
handle_call({?GET_POOLBOY_PID, TargetHost}, _From, State) ->
    {Result, NewState} = handle_get_poolboy_pid(TargetHost, State),
    {reply, Result, NewState};
handle_call(?GET_ADMIN_POOLBOY_PID, _From, State) ->
    {Result, NewState} = handle_get_admin_poolboy_pid(State),
    {reply, Result, NewState};
handle_call({?GET_ADMIN_POOLBOY_PID, TargetHost}, _From, State) ->
    {Result, NewState} = handle_get_admin_poolboy_pid(TargetHost, State),
    {reply, Result, NewState};
handle_call(?GET_POOL_INFO, _From, State) ->
    #state{pool_name = PoolName, ordered_sub_pools = SubPools} = State,
    output_pool_info(PoolName, SubPools),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    SubPools = State#state.ordered_sub_pools,
    stop_poolboy_workers(SubPools),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
init_admin(Name, Size, AdminSize, MaxOverflow, ConnectionsList) when ?valid_config_vars(ConnectionsList, Size, AdminSize, MaxOverflow) ->
    SubPoolPrefix = list_to_atom(atom_to_list(Name) ++ "_sub_pool_"),
    PoolArgs = [{worker_module, ?BALCON_WORKER_MOD}, {max_overflow, MaxOverflow}],
    AdminPoolArgs = [{worker_module, ?BALCON_WORKER_MOD}, {max_overflow, 0}],
    case create_sub_pools(ConnectionsList, Size, AdminSize, SubPoolPrefix, PoolArgs, AdminPoolArgs) of
        {ok, SubPools} ->
            {ok, #state{pool_name = Name, ordered_sub_pools = SubPools}};
        {error, Reason} ->
            {stop, Reason}
    end;
init_admin(_Name, _Size, _AdminSize, _MaxOverflow, _ConnectionsList) ->
    {stop, ?INVALID_CONFIG}.

create_sub_pools(ConnectionsList, Size, AdminSize, SubPoolPrefix, PoolArgs, AdminPoolArgs) ->
    AvailableConnectionsList = lists:foldl(fun({Host, Port}, Acc) ->
        case riakc_pb_socket:start_link(Host, Port) of
            {ok, Pid} ->
                riakc_pb_socket:stop(Pid),
                [{Host, Port}|Acc];
            {error, _Reason} ->
                Acc
        end
    end, [], ConnectionsList),
    case AvailableConnectionsList of
        [] ->
            {error, no_connections_available};
        AvailableConnectionsList ->
            create_sub_pools(AvailableConnectionsList, Size, AdminSize, SubPoolPrefix, PoolArgs, AdminPoolArgs, [])
    end.

create_sub_pools([], _Size, _AdminSize, _SubPoolPrefix, _PoolArgs, _AdminPoolArgs, Acc) ->
    {ok, Acc};
create_sub_pools([{Host, Port}|OtherConnections], Size, AdminSize, SubPoolPrefix, PoolArgs, AdminPoolArgs, Acc) ->
    SubPoolName = list_to_atom(atom_to_list(SubPoolPrefix) ++ integer_to_list(length(Acc) + 1)),
    SubPoolAdminName = list_to_atom(atom_to_list(SubPoolPrefix) ++ "admin_" ++ integer_to_list(length(Acc) + 1)),
    WorkerConnections = Size div length([{Host, Port}|OtherConnections]),
    RemainingConnections = Size - WorkerConnections,
    NewSubPoolArgs = [{name, {local, SubPoolName}}, {size, WorkerConnections}] ++ PoolArgs,
    NewAdminSubPoolArgs = [{name, {local, SubPoolAdminName}}, {size, AdminSize}] ++ AdminPoolArgs,
    WorkerArgs = [{host, Host}, {port, Port}],
    {ok, PoolboyPid} = poolboy:start_link(NewSubPoolArgs, WorkerArgs),
    {ok, AdminPoolboyArgs} = poolboy:start_link(NewAdminSubPoolArgs, WorkerArgs),
    SubPool = #sub_pool{
        sub_pool_name = SubPoolName, poolboy_pid = PoolboyPid, admin_poolboy_pid = AdminPoolboyArgs, host = Host,
        port = Port, size = WorkerConnections
    },
    create_sub_pools(OtherConnections, RemainingConnections, AdminSize, SubPoolPrefix, PoolArgs, AdminPoolArgs, [SubPool|Acc]).

handle_get_poolboy_pid(#state{ordered_sub_pools = []} = State) ->
    Result = {error, no_available_pid},
    {Result, State};
handle_get_poolboy_pid(#state{ordered_sub_pools = [NextSubPool|OtherSubPools]} = State) ->
    PoolboyPid = NextSubPool#sub_pool.poolboy_pid,
    NewOrderedSubPools = OtherSubPools ++ [NextSubPool],
    Result = {ok, PoolboyPid},
    NewState = State#state{ordered_sub_pools = NewOrderedSubPools},
    {Result, NewState}.

handle_get_poolboy_pid(_TargetHost, #state{ordered_sub_pools = []} = State) ->
    Result = {error, no_available_pid},
    {Result, State};
handle_get_poolboy_pid(TargetHost, #state{ordered_sub_pools = SubPools} = State) ->
    FieldIndex = #sub_pool.host,
    SubPool = lists:keyfind(TargetHost, FieldIndex, SubPools),
    case SubPool of
        false ->
            Result = {error, target_host_unavailable},
            {Result, State};
        SubPool when is_record(SubPool, sub_pool) ->
            PoolboyPid = SubPool#sub_pool.poolboy_pid,
            Result = {ok, PoolboyPid},
            NewOrderedSubPools = (SubPools -- [SubPool]) ++ [SubPool],
            NewState = State#state{ordered_sub_pools = NewOrderedSubPools},
            {Result, NewState}
    end.

handle_get_admin_poolboy_pid(#state{ordered_sub_pools = []} = State) ->
    Result = {error, no_available_pid},
    {Result, State};
handle_get_admin_poolboy_pid(#state{ordered_sub_pools = [NextSubPool|OtherSubPools]} = State) ->
    AdminPoolboyPid = NextSubPool#sub_pool.admin_poolboy_pid,
    NewOrderedSubPools = OtherSubPools ++ [NextSubPool],
    Result = {ok, AdminPoolboyPid},
    NewState = State#state{ordered_sub_pools = NewOrderedSubPools},
    {Result, NewState}.

handle_get_admin_poolboy_pid(_TargetHost, #state{ordered_sub_pools = []} = State) ->
    Result = {error, no_available_pid},
    {Result, State};
handle_get_admin_poolboy_pid(TargetHost, #state{ordered_sub_pools = SubPools} = State) ->
    FieldIndex = #sub_pool.host,
    SubPool = lists:keyfind(TargetHost, FieldIndex, SubPools),
    case SubPool of
        false ->
            Result = {error, target_host_unavailable},
            {Result, State};
        SubPool when is_record(SubPool, sub_pool) ->
            PoolboyPid = SubPool#sub_pool.admin_poolboy_pid,
            Result = {ok, PoolboyPid},
            NewOrderedSubPools = (SubPools -- [SubPool]) ++ [SubPool],
            NewState = State#state{ordered_sub_pools = NewOrderedSubPools},
            {Result, NewState}
    end.

output_pool_info(PoolName, SubPools) ->
    io:format("======================================================~n"),
    io:format("=== ~p cluster~n", [PoolName]),
    io:format("======================================================~n"),
    lists:foreach(fun(SubPool) ->
        #sub_pool{host = Host, port = Port, size = Size} = SubPool,
        io:format("Host: ~p, Port: ~p, Connections: ~p.~n", [Host, Port, Size])
    end, SubPools),
    io:format("======================================================~n"),
    ok.

stop_poolboy_workers(SubPools) ->
    lists:foreach(fun(SubPool) ->
        PoolboyPid = SubPool#sub_pool.poolboy_pid,
        poolboy:stop(PoolboyPid)
    end, SubPools),
    ok.
