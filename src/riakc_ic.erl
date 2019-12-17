%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019, bet365
%%% @doc
%%%
%%% @end
%%% Created : 17. Dec 2019 08:54
%%%-------------------------------------------------------------------
-module(riakc_ic).
-author("paulhunt").

-include("riakc.hrl").
-include("riakc_ic.hrl").

%% API
-export([
    get/3, get/4, get/5,
    put/2, put/3, put/4,
    delete/3, delete/4, delete/5,
    delete_vclock/4, delete_vclock/5, delete_vclock/6,
    delete_obj/2, delete_obj/3, delete_obj/4,
    list_buckets/1, list_buckets/2, list_buckets/3,
    stream_list_buckets/1, stream_list_buckets/2, stream_list_buckets/3,
    legacy_list_buckets/1, legacy_list_buckets/2,
    list_keys/2, list_keys/3,
    stream_list_keys/2, stream_list_keys/3,
    get_bucket/2, get_bucket/3, get_bucket/4,
    get_bucket_type/2, get_bucket_type/3,
    set_bucket/3, set_bucket/4, set_bucket/5,
    set_bucket_type/3, set_bucket_type/4,
    reset_bucket/2, reset_bucket/3, reset_bucket/4,

    mapred/3, mapred/4, mapred/5
]).

-define(is_timeout(Timeout), Timeout == infinity orelse (erlang:is_integer(Timeout) andalso Timeout >= 0)).
-define(is_bucket_and_key(Bucket, Key), ?is_bucket(Bucket) andalso erlang:is_binary(Key)).
-define(is_bucket(Bucket), erlang:is_binary(Bucket) orelse erlang:is_tuple(Bucket)).

-type options_or_timeout() :: list() | timeout().
-type options_or_timeout_or_type() :: list() | timeout() | binary().

%%%===================================================================
%%% API Functions
%%%===================================================================
%% Get API Functions
-spec get(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key()) ->
    {ok, riakc_obj()} | {error, term()}.
get(PoolName, Bucket, Key) when ?is_bucket_and_key(Bucket, Key) ->
    get(PoolName, Bucket, Key, [], default_timeout(?GET_TIMEOUT)).

-spec get(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), OptionsOrTimeout :: options_or_timeout()) ->
    {ok, riakc_obj()} | {error, term()}.
get(PoolName, Bucket, Key, Options) when ?is_bucket_and_key(Bucket, Key) andalso erlang:is_list(Options) ->
    get(PoolName, Bucket, Key, Options, default_timeout(?GET_TIMEOUT));
get(PoolName, Bucket, Key, Timeout) when ?is_bucket_and_key(Bucket, Key) andalso ?is_timeout(Timeout) ->
    get(PoolName, Bucket, Key, [], Timeout).

-spec get(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Options :: list(), Timeout :: timeout()) ->
    {ok, riakc_obj()} | {error, term()}.
get(PoolName, Bucket, Key, Options, Timeout) when ?is_bucket_and_key(Bucket, Key) andalso erlang:is_list(Options) andalso ?is_timeout(Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?GET_REQUEST, Bucket, Key, Options, Timeout})).

%% Put API Functions
-spec put(PoolName :: poolboy:pid(), Obj :: riakc_obj()) ->
    {ok, riakc_obj()} | {error, term()} | unchanged.
put(PoolName, Obj) ->
    put(PoolName, Obj, [], default_timeout(?PUT_TIMEOUT)).

-spec put(PoolName :: poolboy:pool(), Obj :: riakc_obj(), OptionsOrTimeout :: options_or_timeout()) ->
    {ok, riakc_obj()} | {error, term()} | unchanged.
put(PoolName, Obj, Options) when erlang:is_list(Options) ->
    put(PoolName, Obj, Options, default_timeout(?PUT_TIMEOUT));
put(PoolName, Obj, Timeout) when ?is_timeout(Timeout) ->
    put(PoolName, Obj, [], Timeout).

-spec put(PoolName :: poolboy:pool(), Obj :: riakc_obj(), Options :: list(), Timeout :: timeout()) ->
    {ok, riakc_obj()} | {error, term()} | unchanged.
put(PoolName, Obj, Options, Timeout) when erlang:is_list(Options) andalso ?is_timeout(Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?PUT_REQUEST, Obj, Options, Timeout})).

%% Delete API Functions
-spec delete(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key()) ->
    ok | {error, term()}.
delete(PoolName, Bucket, Key) when ?is_bucket_and_key(Bucket, Key) ->
    delete(PoolName, Bucket, Key, [], default_timeout(?DELETE_TIMEOUT)).

-spec delete(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), OptionsOrTimeout :: options_or_timeout()) ->
    ok | {error, term()}.
delete(PoolName, Bucket, Key, Options) when ?is_bucket_and_key(Bucket, Key) andalso erlang:is_list(Options) ->
    delete(PoolName, Bucket, Key, Options, default_timeout(?DELETE_TIMEOUT));
delete(PoolName, Bucket, Key, Timeout) when ?is_bucket_and_key(Bucket, Key) andalso ?is_timeout(Timeout) ->
    delete(PoolName, Bucket, Key, [], Timeout).

-spec delete(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Options :: list(), Timeout :: timeout()) ->
    ok | {error, term()}.
delete(PoolName, Bucket, Key, Options, Timeout) when ?is_bucket_and_key(Bucket, Key) andalso erlang:is_list(Options) andalso ?is_timeout(Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?DELETE_REQUEST, Bucket, Key, Options, Timeout})).

%% Delete Vclock API Functions
-spec delete_vclock(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Vclock :: binary()) ->
    ok | {error, term()}.
delete_vclock(PoolName, Bucket, Key, Vclock) when ?is_bucket_and_key(Bucket, Key) andalso erlang:is_binary(Vclock) ->
    delete_vclock(PoolName, Bucket, Key, Vclock, [], default_timeout(?DELETE_VCLOCK_TIMEOUT)).

-spec delete_vclock(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Vclock :: binary(), OptionsOrTimeout :: options_or_timeout()) ->
    ok | {error, term()}.
delete_vclock(PoolName, Bucket, Key, Vclock, Options) when ?is_bucket_and_key(Bucket, Key) andalso erlang:is_binary(Vclock) andalso erlang:is_list(Options) ->
    delete_vclock(PoolName, Bucket, Key, Vclock, Options, default_timeout(?DELETE_VCLOCK_TIMEOUT));
delete_vclock(PoolName, Bucket, Key, Vclock, Timeout) ->
    delete_vclock(PoolName, Bucket, Key, Vclock, [], Timeout).

-spec delete_vclock(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Vclock :: binary(), Options :: list(), Timeout :: timeout()) ->
    ok | {error, term()}.
delete_vclock(PoolName, Bucket, Key, Vclock, Options, Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?DELETE_VCLOCK_REQUEST, Bucket, Key, Vclock, Options, Timeout})).

%% Delete Obj API Functions
-spec delete_obj(PoolName :: poolboy:pool(), Obj :: riakc_obj:riakc_obj()) ->
    ok | {error, term()}.
delete_obj(PoolName, Obj) ->
    delete_obj(PoolName, Obj, [], default_timeout(?DELETE_OBJ_TIMEOUT)).

-spec delete_obj(PoolName :: poolboy:pool(), Obj :: riakc_obj:riakc_obj(), OptionsOrTimeout :: options_or_timeout()) ->
    ok | {error, term()}.
delete_obj(PoolName, Obj, Options) when erlang:is_list(Options) ->
    delete_obj(PoolName, Obj, Options, default_timeout(?DELETE_OBJ_TIMEOUT));
delete_obj(PoolName, Obj, Timeout) when ?is_timeout(Timeout) ->
    delete_obj(PoolName, Obj, [], Timeout).

-spec delete_obj(PoolName :: poolboy:pool(), Obj :: riakc_obj:riakc_obj(), Options :: list(), Timeout :: timeout()) ->
    ok | {error, term()}.
delete_obj(PoolName, Obj, Options, Timeout) when erlang:is_list(Options) andalso ?is_timeout(Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?DELETE_OBJ_REQUEST, Obj, Options, Timeout})).

%% List Buckets API Functions
-spec list_buckets(PoolName :: poolboy:pool()) ->
    {ok, list(bucket())} | {error, term()}.
list_buckets(PoolName) ->
    list_buckets(PoolName, <<"default">>, []).

-spec list_buckets(PoolName :: poolboy:pool(), OptionsOrTimeoutOrType :: options_or_timeout_or_type()) ->
    {ok, list(bucket())} | {error, term()}.
list_buckets(PoolName, Options) when erlang:is_list(Options) ->
    list_buckets(PoolName, <<"default">>, Options);
list_buckets(PoolName, Timeout) when ?is_timeout(Timeout) ->
    list_buckets(PoolName, <<"default">>, [{timeout, Timeout}]);
list_buckets(PoolName, Type) when erlang:is_binary(Type) ->
    list_buckets(PoolName, Type, []).

-spec list_buckets(PoolName :: poolboy:pool(), Type :: binary(), Options :: list()) ->
    {ok, list(bucket())} | {error, term()}.
list_buckets(PoolName, Type, Options) when erlang:is_binary(Type) andalso erlang:is_list(Options) ->
    poolboy:transaction(PoolName, transaction_fun({?LIST_BUCKETS_REQUEST, Type, Options})).

%% Stream List Buckets API Functions
-spec stream_list_buckets(PoolName :: poolboy:pool()) ->
    {ok, list(bucket())} | {error, term()}.
stream_list_buckets(PoolName) ->
    stream_list_buckets(PoolName, <<"default">>, []).

-spec stream_list_buckets(PoolName :: poolboy:pool(), Options :: options_or_timeout_or_type()) ->
    {ok, list(bucket())} | {error, term()}.
stream_list_buckets(PoolName, Options) when erlang:is_list(Options) ->
    stream_list_buckets(PoolName, <<"default">>, Options);
stream_list_buckets(PoolName, Timeout) when ?is_timeout(Timeout) ->
    stream_list_buckets(PoolName, <<"default">>, [{timeout, Timeout}]);
stream_list_buckets(PoolName, Type) when erlang:is_binary(Type) ->
    stream_list_buckets(PoolName, Type, []).

-spec stream_list_buckets(PoolName :: poolboy:pool(), Type :: binary(), Options :: list()) ->
    {ok, list(bucket())} | {error, term()}.
stream_list_buckets(PoolName, Type, Options) when erlang:is_binary(Type) andalso erlang:is_list(Options) ->
    poolboy:transaction(PoolName, transaction_fun({?STREAM_LIST_BUCKETS_REQUEST, Type, Options})).

%% Legacy List Buckets API Functions
-spec legacy_list_buckets(PoolName :: poolboy:pool()) ->
    {ok, list(bucket())} | {error, term()}.
legacy_list_buckets(PoolName) ->
    legacy_list_buckets(PoolName, []).

-spec legacy_list_buckets(PoolName :: poolboy:pool(), OptionsOrTimeout :: options_or_timeout()) ->
    {ok, list(bucket())} | {error, term()}.
legacy_list_buckets(PoolName, Timeout) when ?is_timeout(Timeout) ->
    legacy_list_buckets(PoolName, [{timeout, Timeout}]);
legacy_list_buckets(PoolName, Options) when erlang:is_list(Options)->
    poolboy:transaction(PoolName, transaction_fun({?LEGACY_LIST_BUCKETS_REQUEST, Options})).

%% List Keys API Functions
-spec list_keys(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type()) ->
    {ok, list(key())} | {error, term()}.
list_keys(PoolName, Bucket) when ?is_bucket(Bucket) ->
    list_keys(PoolName, Bucket, []).

-spec list_keys(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), OptionsOrTimeout :: options_or_timeout()) ->
    {ok, list(key())} | {error, term()}.
list_keys(PoolName, Bucket, infinity) when ?is_bucket(Bucket) ->
    list_keys(PoolName, Bucket, [{timeout, undefined}]);
list_keys(PoolName, Bucket, Timeout) when ?is_bucket(Bucket) andalso ?is_timeout(Timeout) ->
    list_keys(PoolName, Bucket, [{timeout, Timeout}]);
list_keys(PoolName, Bucket, Options) when ?is_bucket(Bucket) andalso erlang:is_list(Options) ->
    poolboy:transaction(PoolName, transaction_fun({?LIST_KEYS_REQUEST, Bucket, Options})).

%% Stream List Keys API Functions
-spec stream_list_keys(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type()) ->
    {ok, req_id()} | {error, term()}.
stream_list_keys(PoolName, Bucket) when ?is_bucket(Bucket) ->
    stream_list_keys(PoolName, Bucket, []).

-spec stream_list_keys(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), OptionsOrTimeout :: options_or_timeout()) ->
    {ok, req_id()} | {error, term()}.
stream_list_keys(PoolName, Bucket, infinity) when ?is_bucket(Bucket) ->
    stream_list_keys(PoolName, Bucket, [{timeout, undefined}]);
stream_list_keys(PoolName, Bucket, Timeout) when ?is_bucket(Bucket) andalso ?is_timeout(Timeout) ->
    stream_list_keys(PoolName, Bucket, [{timeout, Timeout}]);
stream_list_keys(PoolName, Bucket, Options) when ?is_bucket(Bucket) andalso erlang:is_list(Options) ->
    poolboy:transaction(PoolName, transaction_fun({?STREAM_LIST_KEYS_REQUEST, Bucket})).

%% Get Bucket API Functions
-spec get_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type()) ->
    {ok, bucket_props()} | {error, term()}.
get_bucket(PoolName, Bucket) when ?is_bucket(Bucket)->
    get_bucket(PoolName, Bucket, default_timeout(?GET_BUCKET_TIMEOUT), default_timeout(?GET_BUCKET_CALL_TIMEOUT)).

-spec get_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Timeout :: timeout()) ->
    {ok, bucket_props()} | {error, term()}.
get_bucket(PoolName, Bucket, Timeout) when ?is_bucket(Bucket) andalso ?is_timeout(Timeout) ->
    get_bucket(PoolName, Bucket, Timeout, default_timeout(?GET_BUCKET_CALL_TIMEOUT)).

-spec get_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, bucket_props()} | {error, term()}.
get_bucket(PoolName, Bucket, Timeout, CallTimeout) when ?is_bucket(Bucket) andalso ?is_timeout(Timeout) andalso ?is_timeout(CallTimeout)->
    poolboy:transaction(PoolName, transaction_fun({?GET_BUCKET_REQUEST, Bucket, Timeout, CallTimeout})).

%% Get Bucket Type API Functions
%% TODO - Find the specs for the get_bucket_type functions.
get_bucket_type(PoolName, BucketType) when erlang:is_binary(BucketType) ->
    get_bucket_type(PoolName, BucketType, default_timeout(?GET_BUCKET_TYPE_TIMEOUT)).

get_bucket_type(PoolName, BucketType, Timeout) when erlang:is_binary(BucketType) andalso ?is_timeout(Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?GET_BUCKET_TYPE_REQUEST, BucketType, Timeout})).

%% Set Bucket API Functions
-spec set_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), BucketProps :: list()) ->
    ok | {error, term()}.
set_bucket(PoolName, Bucket, BucketProps) when ?is_bucket(Bucket) andalso erlang:is_list(BucketProps) ->
    set_bucket(PoolName, Bucket, BucketProps, default_timeout(?SET_BUCKET_TIMEOUT), default_timeout(?SET_BUCKET_CALL_TIMEOUT)).

-spec set_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), BucketProps :: list(), Timeout :: timeout()) ->
    ok | {error, term()}.
set_bucket(PoolName, Bucket, BucketProps, Timeout) when ?is_bucket(Bucket) andalso erlang:is_list(BucketProps) andalso ?is_timeout(Timeout) ->
    set_bucket(PoolName, Bucket, BucketProps, Timeout, default_timeout(?SET_BUCKET_CALL_TIMEOUT)).

-spec set_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), BucketProps :: list(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    ok | {error, term()}.
set_bucket(PoolName, Bucket, BucketProps, Timeout, CallTimeout) when ?is_bucket(Bucket) andalso erlang:is_list(BucketProps) andalso ?is_timeout(Timeout) andalso ?is_timeout(CallTimeout) ->
    poolboy:transaction(PoolName, transaction_fun({?SET_BUCKET_REQUEST, Bucket, BucketProps, Timeout, CallTimeout})).

%% Set Bucket Type API Functions
%% TODO - Find the specs for the set_bucket_type functions.
set_bucket_type(PoolName, BucketType, BucketProps) when erlang:is_binary(BucketType) andalso erlang:is_list(BucketProps) ->
    set_bucket_type(PoolName, BucketType, BucketProps, default_timeout(?SET_BUCKET_TYPE_TIMEOUT)).

set_bucket_type(PoolName, BucketType, BucketProps, Timeout) when erlang:is_binary(BucketType) andalso erlang:is_list(BucketProps) andalso ?is_timeout(Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?SET_BUCKET_TYPE_REQUEST, BucketType, BucketProps, Timeout})).

%% Reset Bucket API Functions
-spec reset_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type()) ->
    ok | {error, term()}.
reset_bucket(PoolName, Bucket) ->
    reset_bucket(PoolName, Bucket, default_timeout(?RESET_BUCKET_TIMEOUT), default_timeout(?RESET_BUCKET_CALL_TIMEOUT)).

-spec reset_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Timeout :: timeout()) ->
    ok | {error, term()}.
reset_bucket(PoolName, Bucket, Timeout) ->
    reset_bucket(PoolName, Bucket, Timeout, default_timeout(?RESET_BUCKET_CALL_TIMEOUT)).

-spec reset_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    ok | {error, term()}.
reset_bucket(PoolName, Bucket, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, transaction_fun({?RESET_BUCKET_REQUEST, Bucket, Timeout, CallTimeout})).

%% Mapred API Functions
-spec mapred(PoolName :: poolboy:pool(), Inputs :: mapred_inputs(), Query :: list(mapred_queryterm())) ->
    {ok, mapred_result()} | {error, term()}.
mapred(PoolName, Inputs, Query) ->
    mapred(PoolName, Inputs, Query, default_timeout(?MAPRED_TIMEOUT)).

-spec mapred(PoolName :: poolboy:pool(), Inputs :: mapred_inputs(), Query :: list(mapred_queryterm()), Timeout :: timeout()) ->
    {ok, mapred_result()} | {error, term()}.
mapred(PoolName, Inputs, Query, Timeout) ->
    mapred(PoolName, Inputs, Query, Timeout, default_timeout(?MAPRED_CALL_TIMEOUT)).

-spec mapred(PoolName :: poolboy:pool(), Inputs :: mapred_inputs(), Query :: list(mapred_queryterm()), Timeout :: timeout(), CallTime :: timeout()) ->
    {ok, mapred_result()} | {error, term()}.
mapred(PoolName, Inputs, Query, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, {?MAPRED_REQUEST, Inputs, Query, Timeout, CallTimeout}).

%%%===================================================================
%%% Internal Functions
%%%===================================================================
default_timeout(TimeoutName) ->
    case application:get_env(riakc, TimeoutName) of
        {ok, EnvTimeout} ->
            EnvTimeout;
        undefined ->
            case application:get_env(riakc, timeout) of
                {ok, Timeout} ->
                    Timeout;
                undefined ->
                    ?DEFAULT_PB_TIMEOUT
            end
    end.

transaction_fun(Request) ->
    fun(Worker) ->
        gen_server:call(Worker, Request)
    end.
