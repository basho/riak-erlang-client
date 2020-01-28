%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2020, bet365
%%% @doc
%%%
%%% @end
%%% Created : 09. Jan 2020 12:51
%%%-------------------------------------------------------------------
-module(riakc_ic_balcon).
-author("paulhunt").
-include("riakc.hrl").
-include("riakc_ic.hrl").

-behaviour(gen_server).

%% API
-export([start_link/3]).

%% API Calls
-export([transaction/2, transaction/3, checkout/1, checkin/2]).

%% Admin Calls
-export([get_info/1, rebalance/1, get_pool_info/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(MAPPING_TABLE, mapping).

-define(is_valid_bucket_or_pool_name(BucketOrPoolName), is_binary(BucketOrPoolName) orelse is_tuple(BucketOrPoolName)
    orelse is_atom(BucketOrPoolName)).

-record(pool_info, {
    pool_name :: atom(),
    admin_pid :: pid()
}).

-record(admin_info, {
    admin_pid   :: pid(),
    admin_name  :: atom(),
    pool_name   :: atom(),
    admin_args  :: list(term()),
    host_names  :: list(string()),
    worker_args :: list(term()),
    buckets     :: list(bucket_or_pool_name())
}).

-record(state, {
    admins :: list(#admin_info{})
}).

-type bucket_or_pool_name() :: atom() | bucket() | bucket_and_type().

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link(Mappings :: {atom(), list({atom(), list()})}, RegisterModule :: atom(), StatisticModule :: atom()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Mappings, RegisterModule, StatisticModule) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Mappings, RegisterModule, StatisticModule], []).

-spec transaction(BucketOrPoolName :: bucket_or_pool_name(), Fun :: function()) ->
    any() | {error, invalid_bucket_or_pool_name}.
transaction(BucketOrPoolName, Fun) ->
    handle_transaction(BucketOrPoolName, Fun).

-spec transaction(BucketOrPoolName :: bucket_or_pool_name(), Fun :: function(), Timeout :: pos_integer()) ->
    any() | {error, invalid_bucket_or_pool_name}.
transaction(BucketOrPoolName, Fun, Timeout) ->
    handle_transaction(BucketOrPoolName, Fun, Timeout).

-spec checkout(BucketOrPoolName :: bucket_or_pool_name()) ->
    pid() | {error, invalid_bucket_or_pool_name}.
checkout(BucketOrPoolName) ->
    handle_checkout(BucketOrPoolName).

-spec checkin(BucketOrPoolName :: bucket_or_pool_name(), Worker :: pid()) ->
    ok | {error, term()}.
checkin(PoolPid, Worker) ->
    handle_checkin(PoolPid, Worker).

%% TODO - Complete spec
-spec get_info(PoolPid :: any()) ->
    {ok, any()}.
get_info(_PoolPids) ->
    %% TODO - Complete function implementation.
    ok.

%% TODO - Complete spec
-spec rebalance(PoolName :: atom()) ->
    ok | {error, bad_option} | {ok, {atom(), integer(), integer(), integer()}}.
rebalance(_PoolName) ->
    %% TODO - Complete function implementation.
    ok.

-spec get_pool_info(PoolName :: atom()) ->
    ok | {error, term()}.
get_pool_info(PoolName) ->
    handle_get_pool_info(PoolName).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Mappings, RegisterModule, StatisticModule]) ->
    process_flag(trap_exit, true),
    %% TODO - Add 'admin connections' functionality, which uses connections specifically to send/receive admin requests.
    ets:new(?MAPPING_TABLE, [set, named_table, {read_concurrency, true}]),
    case init_balcon(Mappings, RegisterModule, StatisticModule) of
        {ok, State} ->
            {ok, State};
        {error, Reason} ->
            {stop, {error, Reason}}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'EXIT', _Pid, ?NO_AVAILABLE_CONNECTIONS}, State) ->
    %% TODO - Add logging for admin dying.
    {noreply, State};
handle_info({'EXIT', Pid, normal}, State) ->
    AllMappings = ets:tab2list(?MAPPING_TABLE),
    [ets:delete(?MAPPING_TABLE, BucketOrPoolName) ||
        {BucketOrPoolName, #pool_info{admin_pid = AdminPid}} <- AllMappings, AdminPid == Pid],
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
init_balcon(Mappings, _RegisterModule, _StatisticModule) ->
    AllPoolsSettings = riakc_config:get_pools([]),
    AdminInfos = lists:foldl(fun({_, {PoolName, Buckets}}, Acc) ->
        case Acc of
            {error, Reason} ->
                {error, Reason};
            Acc ->
                PoolSettings = lists:keyfind(PoolName, 1, AllPoolsSettings),
                case init_admin(PoolSettings, Buckets) of
                    AdminInfo when is_record(AdminInfo, admin_info) ->
                        [AdminInfo|Acc];
                    {error, Reason} ->
                        %% TODO - Need to add logging here (or somewhere else during init fail path) for failing to start admin.
                        stop_admins(Acc),
                        {error, Reason}
                end
        end
    end, [], Mappings),
    case AdminInfos of
        {error, Reason} ->
            {error, Reason};
        AdminInfos ->
            {ok, #state{admins = AdminInfos}}
    end.

init_admin({PoolName, AdminArgs}, Buckets) ->
    AdminName = list_to_atom(atom_to_list(PoolName) ++ "_admin"),
    case riakc_ic_balcon_admin:start_link(PoolName, AdminName, AdminArgs) of
        {ok, AdminPid} ->
            ets:insert(?MAPPING_TABLE, {PoolName, #pool_info{pool_name = PoolName, admin_pid = AdminPid}}),
            lists:foreach(fun(Bucket) ->
                ets:insert(?MAPPING_TABLE, {Bucket, #pool_info{pool_name = PoolName, admin_pid = AdminPid}})
            end, Buckets),
            #admin_info{admin_pid = AdminPid, admin_name = AdminName, pool_name = PoolName, admin_args = AdminArgs,
                buckets = Buckets};
        {error, Reason} ->
            {error, Reason}
    end;
init_admin(_InvalidPoolSettings, _Buckets) ->
    {error, ?INVALID_CONFIG}.

stop_admins([]) ->
    ok;
stop_admins([NextAdmin|OtherAdmins]) ->
    AdminName = NextAdmin#admin_info.admin_name,
    riakc_ic_balcon_admin:stop(AdminName),
    stop_admins(OtherAdmins).

get_admin_pid(BucketOrPoolName) ->
    case ets:lookup(?MAPPING_TABLE, BucketOrPoolName) of
        [{_PoolInfo, #pool_info{admin_pid = AdminPid}}] ->
            {ok, AdminPid};
        [] ->
            {error, invalid_bucket_or_pool_name}
    end.

handle_transaction(BucketOrPoolName, Fun) ->
    case get_admin_pid(BucketOrPoolName) of
        {ok, AdminPid} ->
            {ok, PoolboyPid} = riakc_ic_balcon_admin:get_poolboy_pid(AdminPid),
            WorkerFun = fun(WorkerPid) -> riakc_ic_balcon_worker:execute(WorkerPid, Fun) end,
            poolboy:transaction(PoolboyPid, WorkerFun);
        {error, Reason} ->
            {error, Reason}
    end.

handle_transaction(BucketOrPoolName, Fun, Timeout) ->
    case get_admin_pid(BucketOrPoolName) of
        {ok, AdminPid} ->
            {ok, PoolboyPid} = riakc_ic_balcon_admin:get_poolboy_pid(AdminPid),
            WorkerFun = fun(WorkerPid) -> riakc_ic_balcon_worker:execute(WorkerPid, Fun) end,
            poolboy:transaction(PoolboyPid, WorkerFun, Timeout);
        {error, Reason} ->
            {error, Reason}
    end.

handle_checkout(BucketOrPoolName) ->
    case get_admin_pid(BucketOrPoolName) of
        {ok, AdminPid} ->
            {ok, PoolboyPid} = riakc_ic_balcon_admin:get_poolboy_pid(AdminPid),
            poolboy:checkout(PoolboyPid);
        {error, Reason} ->
            {error, Reason}
    end.

handle_checkin(BucketOrPoolName, Worker) ->
    case get_admin_pid(BucketOrPoolName) of
        {ok, AdminPid} ->
            {ok, PoolboyPid} = riakc_ic_balcon_admin:get_poolboy_pid(AdminPid),
            poolboy:checkin(PoolboyPid, Worker);
        {error, Reason} ->
            {error, Reason}
    end.

handle_get_pool_info(BucketOrPoolName) ->
    case get_admin_pid(BucketOrPoolName) of
        {ok, AdminPid} ->
            riakc_ic_balcon_admin:get_pool_info(AdminPid);
        {error, Reason} ->
            {error, Reason}
    end.
