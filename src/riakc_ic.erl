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
    mapred/3, mapred/4, mapred/5,
    mapred_stream/4, mapred_stream/5, mapred_stream/6,
    mapred_bucket/3, mapred_bucket/4, mapred_bucket/5,
    mapred_bucket_stream/5, mapred_bucket_stream/6,
    search/3, search/4, search/5, search/6,
    get_index/4, get_index/5, get_index/6, get_index/7,
    get_index_eq/4, get_index_eq/5,
    get_index_range/5, get_index_range/6,
    cs_bucket_fold/3,
    tunnel/4,
    get_preflist/3, get_preflist/4,
    get_coverage/2, get_coverage/3,
    replace_coverage/3, replace_coverage/4,
    get_ring/1, get_ring/2,
    get_default_bucket_props/1, get_default_bucket_props/2,
    get_nodes/1, get_nodes/2
]).

%% Yokozuna admin commands
-export([
    list_search_indexes/1, list_search_indexes/2,
    create_search_index/2, create_search_index/3, create_search_index/4,
    get_search_index/2, get_search_index/3,
    delete_search_index/2, delete_search_index/3,
    set_search_index/3,
    get_search_schema/2, get_search_schema/3,
    create_search_schema/3, create_search_schema/4
]).

%% Pre-Riak 2.0 Counter API - Not for CRDT counters
-export([
    counter_incr/4, counter_incr/5,
    counter_val/3, counter_val/4
]).

%% Datatypes API
-export([
    fetch_type/3, fetch_type/4,
    update_type/4, update_type/5,
    modify_type/5
]).

-define(is_timeout(Timeout), (is_integer(Timeout) andalso Timeout >= 0) orelse Timeout == infinity).

%%%===================================================================
%%% API Functions
%%%===================================================================
%% Get API Functions
-spec get(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key()) ->
    {ok, riakc_obj()} | {error, term()}.
get(PoolName, Bucket, Key) ->
    get(PoolName, Bucket, Key, [], default_timeout(?GET_TIMEOUT)).

-spec get(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), OptionsOrTimeout :: list() | timeout()) ->
    {ok, riakc_obj()} | {error, term()}.
get(PoolName, Bucket, Key, Options) when is_list(Options) ->
    get(PoolName, Bucket, Key, Options, default_timeout(?GET_TIMEOUT));
get(PoolName, Bucket, Key, Timeout) when ?is_timeout(Timeout) ->
    get(PoolName, Bucket, Key, [], Timeout).

-spec get(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Options :: list(), Timeout :: timeout()) ->
    {ok, riakc_obj()} | {error, term()}.
get(PoolName, Bucket, Key, Options, Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?GET_REQ, Bucket, Key, Options, Timeout})).

%% Put API Functions
-spec put(PoolName :: poolboy:pool(), Obj :: riakc_obj()) ->
    ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
put(PoolName, Obj) ->
    put(PoolName, Obj, [], default_timeout(?PUT_TIMEOUT)).

-spec put(PoolName :: poolboy:pool(), Obj :: riakc_obj(), OptionsOrTimeout :: list() | timeout()) ->
    ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
put(PoolName, Obj, Options) when is_list(Options) ->
    put(PoolName, Obj, Options, default_timeout(?PUT_TIMEOUT));
put(PoolName, Obj, Timeout) when ?is_timeout(Timeout) ->
    put(PoolName, Obj, [], Timeout).

-spec put(PoolName :: poolboy:pool(), Obj :: riakc_obj(), Options :: list(), Timeout :: timeout()) ->
    ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
put(PoolName, Obj, Options, Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?PUT_REQ, Obj, Options, Timeout})).

%% Delete API Functions
-spec delete(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key()) ->
    ok | {error, term()}.
delete(PoolName, Bucket, Key) ->
    delete(PoolName, Bucket, Key, [], default_timeout(?DELETE_TIMEOUT)).

-spec delete(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), OptionsOrTimeout :: list() | timeout()) ->
    ok | {error, term()}.
delete(PoolName, Bucket, Key, Options) when is_list(Options) ->
    delete(PoolName, Bucket, Key, Options, default_timeout(?DELETE_TIMEOUT));
delete(PoolName, Bucket, Key, Timeout) when ?is_timeout(Timeout) ->
    delete(PoolName, Bucket, Key, [], Timeout).

-spec delete(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Options :: list(), Timeout :: timeout()) ->
    ok | {error, term()}.
delete(PoolName, Bucket, Key, Options, Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?DELETE_REQ, Bucket, Key, Options, Timeout})).

%% Delete Vclock API Functions
-spec delete_vclock(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Vclock :: vclock()) ->
    ok | {error, term()}.
delete_vclock(PoolName, Bucket, Key, Vclock) ->
    delete_vclock(PoolName, Bucket, Key, Vclock, [], default_timeout(?DELETE_VCLOCK_TIMEOUT)).

-spec delete_vclock(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Vclock :: vclock(), OptionsOrTimeout :: list() | timeout()) ->
    ok | {error, term()}.
delete_vclock(PoolName, Bucket, Key, Vclock, Options) when is_list(Options)->
    delete_vclock(PoolName, Bucket, Key, Vclock, Options, default_timeout(?DELETE_VCLOCK_TIMEOUT));
delete_vclock(PoolName, Bucket, Key, Vclock, Timeout) when ?is_timeout(Timeout) ->
    delete_vclock(PoolName, Bucket, Key, Vclock, [], Timeout).

-spec delete_vclock(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Vclock :: vclock(), Options :: list(), Timeout :: timeout()) ->
    ok | {error, term()}.
delete_vclock(PoolName, Bucket, Key, Vclock, Options, Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?DELETE_VCLOCK_REQ, Bucket, Key, Vclock, Options, Timeout})).

%% Delete Obj API Functions
-spec delete_obj(PoolName :: poolboy:pool(), Obj :: riakc_obj()) ->
    ok | {error, term()}.
delete_obj(PoolName, Obj) ->
    delete_obj(PoolName, Obj, [], default_timeout(?DELETE_OBJ_TIMEOUT)).

-spec delete_obj(PoolName :: poolboy:pool(), Obj :: riakc_obj(), OptionsOrTimeout :: list() | timeout()) ->
    ok | {error, term()}.
delete_obj(PoolName, Obj, Options) when is_list(Options) ->
    delete_obj(PoolName, Obj, Options, default_timeout(?DELETE_OBJ_TIMEOUT));
delete_obj(PoolName, Obj, Timeout) when ?is_timeout(Timeout) ->
    delete_obj(PoolName, Obj, [], Timeout).

-spec delete_obj(PoolName :: poolboy:pool(), Obj :: riakc_obj(), Options :: list(), Timeout :: timeout()) ->
    ok | {error, term()}.
delete_obj(PoolName, Obj, Options, Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?DELETE_OBJ_REQ, Obj, Options, Timeout})).

%% List Buckets API Functions
-spec list_buckets(PoolName :: poolboy:pool()) ->
    {ok, list(bucket())} | {error, term()}.
list_buckets(PoolName) ->
    list_buckets(PoolName, <<"default">>, []).

-spec list_buckets(PoolName :: poolboy:pool(), OptionsOrTimeoutOrType :: list() | timeout() | binary()) ->
    {ok, list(bucket())} | {error, term()}.
list_buckets(PoolName, Options) when is_list(Options) ->
    list_buckets(PoolName, <<"default">>, Options);
list_buckets(PoolName, Timeout) when ?is_timeout(Timeout) ->
    list_buckets(PoolName, <<"default">>, [{timeout, Timeout}]);
list_buckets(PoolName, Type) when is_binary(Type) ->
    list_buckets(PoolName, Type, []).

-spec list_buckets(PoolName :: poolboy:pool(), Type :: binary(), Options :: list()) ->
    {ok, list(bucket())} | {error, term()}.
list_buckets(PoolName, Type, Options) ->
    poolboy:transaction(PoolName, transaction_fun({?LIST_BUCKETS_REQ, Type, Options})).

%% Stream List Buckets API Functions
-spec stream_list_buckets(PoolName :: poolboy:pool()) ->
    {ok, non_neg_integer()} | {error, term()}.
stream_list_buckets(PoolName) ->
    stream_list_buckets(PoolName, <<"default">>, []).

-spec stream_list_buckets(PoolName :: poolboy:pool(), OptionsOrTimeoutOrType :: list() | timeout() | binary()) ->
    {ok, non_neg_integer()} | {error, term()}.
stream_list_buckets(PoolName, Options) when is_list(Options) ->
    stream_list_buckets(PoolName, <<"default">>, Options);
stream_list_buckets(PoolName, Timeout) when ?is_timeout(Timeout) ->
    stream_list_buckets(PoolName, <<"default">>, [{timeout, Timeout}]);
stream_list_buckets(PoolName, Type) when is_binary(Type) ->
    stream_list_buckets(PoolName, Type, []).

-spec stream_list_buckets(PoolName :: poolboy:pool(), Type :: binary(), Options :: list()) ->
    {ok, non_neg_integer()} | {error, term()}.
stream_list_buckets(PoolName, Type, Options) ->
    poolboy:transaction(PoolName, transaction_fun({?STREAM_LIST_BUCKETS_REQ, Type, Options})).

%% Legacy List Buckets API Functions
%% TODO - Work out spec for below function.
legacy_list_buckets(PoolName) ->
    legacy_list_buckets(PoolName, []).

legacy_list_buckets(PoolName, Timeout) when ?is_timeout(Timeout) ->
    legacy_list_buckets(PoolName, [{timeout, Timeout}]);
legacy_list_buckets(PoolName, Options) when is_list(Options) ->
    poolboy:transaction(PoolName, transaction_fun({?LEGACY_LIST_BUCKETS_REQ, Options})).

%% List Keys API Functions
-spec list_keys(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type()) ->
    {ok, list(key())} | {error, term()}.
list_keys(PoolName, Bucket) ->
    list_keys(PoolName, Bucket, []).

-spec list_keys(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), TimeoutOrOptions :: list() | timeout()) ->
    {ok, list(key())} | {error, term()}.
list_keys(PoolName, Bucket, infinity) ->
    list_keys(PoolName, Bucket, [{timeout, undefined}]);
list_keys(PoolName, Bucket, Timeout) when ?is_timeout(Timeout) ->
    list_keys(PoolName, Bucket, [{timeout, Timeout}]);
list_keys(PoolName, Bucket, Options) ->
    poolboy:transaction(PoolName, transaction_fun({?LIST_KEYS_REQ, Bucket, Options})).

%% Stream List Keys API Functions
-spec stream_list_keys(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type()) ->
    {ok, req_id()} | {error, term()}.
stream_list_keys(PoolName, Bucket) ->
    stream_list_keys(PoolName, Bucket, []).

-spec stream_list_keys(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), OptionsOrTimeout :: list() | timeout()) ->
    {ok, req_id()} | {error, term()}.
stream_list_keys(PoolName, Bucket, infinity) ->
    stream_list_keys(PoolName, Bucket, [{timeout, undefined}]);
stream_list_keys(PoolName, Bucket, Timeout) when ?is_timeout(Timeout) ->
    stream_list_keys(PoolName, Bucket, [{timeout, Timeout}]);
stream_list_keys(PoolName, Bucket, Options) ->
    poolboy:transaction(PoolName, transaction_fun({?STREAM_LIST_KEYS_REQ, Bucket, Options})).

%% Get Bucket API Functions
-spec get_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type()) ->
    {ok, bucket_props()} | {error, term()}.
get_bucket(PoolName, Bucket) ->
    get_bucket(PoolName, Bucket, default_timeout(?GET_BUCKET_TIMEOUT), default_timeout(?GET_BUCKET_CALL_TIMEOUT)).

-spec get_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Timeout :: timeout()) ->
    {ok, bucket_props()} | {error, term()}.
get_bucket(PoolName, Bucket, Timeout) ->
    get_bucket(PoolName, Bucket, Timeout, default_timeout(?GET_BUCKET_CALL_TIMEOUT)).

-spec get_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, bucket_props()} | {error, term()}.
get_bucket(PoolName, Bucket, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, transaction_fun({?GET_BUCKET_REQ, Bucket, Timeout, CallTimeout})).

%% Get Bucket Type API Functions
%% TODO - Work out the specs for these functions. Can't find what data type BucketType is.
get_bucket_type(PoolName, BucketType) ->
    get_bucket_type(PoolName, BucketType, default_timeout(?GET_BUCKET_TYPE_TIMEOUT)).

get_bucket_type(PoolName, BucketType, Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?GET_BUCKET_TYPE_REQ, BucketType, Timeout})).

%% Set Bucket API Functions
-spec set_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), BucketProps :: bucket_props()) ->
    ok | {error, term()}.
set_bucket(PoolName, Bucket, BucketProps) ->
    set_bucket(PoolName, Bucket, BucketProps, default_timeout(?SET_BUCKET_TIMEOUT), default_timeout(?SET_BUCKET_CALL_TIMEOUT)).

-spec set_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), BucketProps :: bucket_props(), Timeout :: timeout()) ->
    ok | {error, term()}.
set_bucket(PoolName, Bucket, BucketProps, Timeout) ->
    set_bucket(PoolName, Bucket, BucketProps, Timeout, default_timeout(?SET_BUCKET_CALL_TIMEOUT)).

-spec set_bucket(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), BucketProps :: bucket_props(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    ok | {error, term()}.
set_bucket(PoolName, Bucket, BucketProps, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, transaction_fun({?SET_BUCKET_REQ, Bucket, BucketProps, Timeout, CallTimeout})).

%% Set Bucket Type API Functions
%% TODO - Work out the specs for these functions. Can't find what data type BucketType is.
set_bucket_type(PoolName, BucketType, BucketProps) ->
    set_bucket_type(PoolName, BucketType, BucketProps, default_timeout(?SET_BUCKET_TYPE_TIMEOUT)).

set_bucket_type(PoolName, BucketType, BucketProps, Timeout) ->
    poolboy:transaction(PoolName, transaction_fun({?SET_BUCKET_TYPE_REQ, BucketType, BucketProps, Timeout})).

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
    poolboy:transaction(PoolName, transaction_fun({?RESET_BUCKET_REQ, Bucket, Timeout, CallTimeout})).

%% Mapred API Functions
-spec mapred(PoolName :: poolboy:pool(), Inputs :: mapred_inputs(), Query :: list(mapred_queryterm())) ->
    {ok, mapred_result()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred(PoolName, Inputs, Query) ->
    mapred(PoolName, Inputs, Query, default_timeout(?MAPRED_TIMEOUT)).

-spec mapred(PoolName :: poolboy:pool(), Inputs :: mapred_inputs(), Query :: list(mapred_queryterm()), Timeout :: timeout()) ->
    {ok, mapred_result()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred(PoolName, Inputs, Query, Timeout) ->
    mapred(PoolName, Inputs, Query, Timeout, default_timeout(?MAPRED_CALL_TIMEOUT)).

-spec mapred(PoolName :: poolboy:pool(), Inputs :: mapred_inputs(), Query :: list(mapred_queryterm()), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, mapred_result()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred(PoolName, Inputs, Query, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, {?MAPRED_REQ, Inputs, Query, Timeout, CallTimeout}).

%% Mapred Stream API Functions
-spec mapred_stream(PoolName :: poolboy:pool(), Inputs :: mapred_inputs(), Query :: list(mapred_queryterm()), ClientPid :: pid()) ->
    {ok, req_id()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred_stream(PoolName, Inputs, Query, ClientPid) ->
    mapred_stream(PoolName, Inputs, Query, ClientPid, default_timeout(?MAPRED_STREAM_TIMEOUT), default_timeout(?MAPRED_STREAM_CALL_TIMEOUT)).

-spec mapred_stream(PoolName :: poolboy:pool(), Inputs :: mapred_inputs(), Query :: list(mapred_queryterm()), ClientPid :: pid(), Timeout :: timeout()) ->
    {ok, req_id()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred_stream(PoolName, Inputs, Query, ClientPid, Timeout) ->
    mapred_stream(PoolName, Inputs, Query, ClientPid, Timeout, default_timeout(?MAPRED_STREAM_TIMEOUT)).

-spec mapred_stream(PoolName :: poolboy:pool(), Inputs :: mapred_inputs(), Query :: list(mapred_queryterm()), ClientPid :: pid(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, req_id()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred_stream(PoolName, Inputs, Query, ClientPid, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, {?MAPRED_STREAM_REQ, Inputs, Query, ClientPid, Timeout, CallTimeout}).

%% Mapred Bucket API Functions
-spec mapred_bucket(PoolName :: poolboy:pool(), Bucket :: bucket(), Query :: list(mapred_queryterm())) ->
    {ok, mapred_result()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred_bucket(PoolName, Bucket, Query) ->
    mapred_bucket(PoolName, Bucket, Query, default_timeout(?MAPRED_BUCKET_TIMEOUT)).

-spec mapred_bucket(PoolName :: poolboy:pool(), Bucket :: bucket(), Query :: list(mapred_queryterm()), Timeout :: timeout()) ->
    {ok, mapred_result()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred_bucket(PoolName, Bucket, Query, Timeout) ->
    mapred_bucket(PoolName, Bucket, Query, Timeout, default_timeout(?MAPRED_BUCKET_CALL_TIMEOUT)).

-spec mapred_bucket(PoolName :: poolboy:pool(), Bucket :: bucket(), Query :: list(mapred_queryterm()), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, mapred_result()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred_bucket(PoolName, Bucket, Query, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, {?MAPRED_BUCKET_REQ, Bucket, Query, Timeout, CallTimeout}).

%% Mapred Bucket Stream API Functions
-spec mapred_bucket_stream(PoolName :: poolboy:pool(), Bucket :: bucket(), Query :: list(mapred_queryterm()), ClientPid :: pid(), Timeout :: timeout()) ->
    {ok, req_id()} | {error, term()}.
mapred_bucket_stream(PoolName, Bucket, Query, ClientPid, Timeout) ->
    mapred_bucket_stream(PoolName, Bucket, Query, ClientPid, Timeout, default_timeout(?MAPRED_BUCKET_STREAM_CALL_TIMEOUT)).

-spec mapred_bucket_stream(PoolName :: poolboy:pool(), Bucket :: bucket(), Query :: list(mapred_queryterm()), ClientPid :: pid(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, req_id()} | {error, term()}.
mapred_bucket_stream(PoolName, Bucket, Query, ClientPid, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, {?MAPRED_BUCKET_STREAM_REQ, Bucket, Query, ClientPid, Timeout, CallTimeout}).

%% Search API Functions
-spec search(PoolName :: poolboy:pool(), Index :: binary(), SearchQuery :: binary()) ->
    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery) ->
    search(PoolName, Index,SearchQuery, [], default_timeout(?SEARCH_TIMEOUT), default_timeout(?SEARCH_CALL_TIMEOUT)).

-spec search(PoolName :: poolboy:pool(), Index :: binary(), SearchQuery :: binary(), OptionsOrTimeout :: list() | timeout()) ->
    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery, Options) when is_list(Options) ->
    search(PoolName, Index, SearchQuery, Options, default_timeout(?SEARCH_TIMEOUT), default_timeout(?SEARCH_CALL_TIMEOUT));
search(PoolName, Index, SearchQuery, Timeout) when ?is_timeout(Timeout) ->
    search(PoolName, Index, SearchQuery, [], Timeout, default_timeout(?SEARCH_CALL_TIMEOUT)).

-spec search(PoolName :: poolboy:pool(), Index :: binary(), SearchQuery :: binary(), OptionsOrTimeout :: list() | timeout(), TimeoutOrCallTimeout :: timeout()) ->
    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery, Options, Timeout) when is_list(Options) ->
    search(PoolName, Index, SearchQuery, Options, Timeout, default_timeout(?SEARCH_TIMEOUT));
search(PoolName, Index, SearchQuery, Timeout, CallTimeout) when ?is_timeout(Timeout) ->
    search(PoolName, Index, SearchQuery, [], Timeout, CallTimeout).

-spec search(PoolName :: poolboy:pool(), Index :: binary(), SearchQuery :: binary(), Options :: list(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery, Options, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, {?SEARCH_REQ, Index, SearchQuery, Options, Timeout, CallTimeout}).

%% Get Index API Functions
-spec get_index(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), Key :: key() | integer()) ->
    {ok, index_results()} | {error, term()}.
get_index(PoolName, Bucket, Index, Key) ->
    get_index_eq(PoolName, Bucket, Index, Key, []).

-spec get_index(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), StartKey :: key() | integer(), EndKey :: key() | integer()) ->
    {ok, index_results()} | {error, term()}.
get_index(PoolName, Bucket, Index, StartKey, EndKey) ->
    get_index_range(PoolName, Bucket, Index, StartKey, EndKey, []).

-spec get_index(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), Key :: key() | integer(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, index_results()} | {error, term()}.
get_index(PoolName, Bucket, Index, Key, Timeout, _CallTimeout) ->
    get_index_eq(PoolName, Bucket, Index, Key, [{timeout, Timeout}]).

-spec get_index(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), StartKey :: key() | integer(), EndKey :: key() | integer(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, index_results()} | {error, term()}.
get_index(PoolName, Bucket, Index, StartKey, EndKey, Timeout, _CallTimeout) ->
    get_index_range(PoolName, Bucket, Index, StartKey, EndKey, [{timeout, Timeout}]).

%% Get Index Eq API Functions
-spec get_index_eq(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), Key :: key() | integer()) ->
    {ok, index_results()} | {error, term()}.
get_index_eq(PoolName, Bucket, Index, Key) ->
    get_index_eq(PoolName, Bucket, Index, Key, []).

-spec get_index_eq(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), Key :: key() | integer(), Options :: list()) ->
    {ok, index_results()} | {error, term()}.
get_index_eq(PoolName, Bucket, Index, Key, Options) ->
    poolboy:transaction(PoolName, {?GET_INDEX_EQ_REQ, Bucket, Index, Key, Options}).

%% Get Index Range API Functions
-spec get_index_range(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), StartKey :: key() | integer(), EndKey :: key() | integer()) ->
    {ok, index_results()} | {error, term()}.
get_index_range(PoolName, Bucket, Index, StartKey, EndKey) ->
    get_index_range(PoolName, Bucket, Index, StartKey, EndKey, []).

-spec get_index_range(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), StartKey :: key() | integer(), EndKey :: key() | integer(), Options :: list()) ->
    {ok, index_results()} | {error, term()}.
get_index_range(PoolName, Bucket, Index, StartKey, EndKey, Options) ->
    poolboy:transaction(PoolName, {?GET_INDEX_RANGE_REQ, Bucket, Index, StartKey, EndKey, Options}).

%% Cs Bucket Fold API Function
-spec cs_bucket_fold(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Options :: list()) ->
    {ok, reference()} | {error, term()}.
cs_bucket_fold(PoolName, Bucket, Options) ->
    poolboy:transaction(PoolName, {?CS_BUCKET_FOLD_REQ, Bucket, Options}).

%% Tunnel API Function
-spec tunnel(PoolName :: poolboy:pool(), MessageId :: non_neg_integer(), Packet :: iolist(), Timeout :: timeout()) ->
    {ok, binary()} | {error, term()}.
tunnel(PoolName, MessageId, Packet, Timeout) ->
    poolboy:transaction(PoolName, {?TUNNEL_REQ, MessageId, Packet, Timeout}).

%% Get Preflist API Functions
-spec get_preflist(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key()) ->
    {ok, preflist()} | {error, term()}.
get_preflist(PoolName, Bucket, Key) ->
    get_preflist(PoolName, Bucket, Key, default_timeout(?GET_PREFLIST_TIMEOUT)).

-spec get_preflist(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Timeout :: timeout()) ->
    {ok, preflist()} | {error, term()}.
get_preflist(PoolName, Bucket, Key, Timeout) ->
    poolboy:transaction(PoolName, {?GET_PREFLIST_REQ, Bucket, Key, Timeout}).

%% Get Coverage API Functions
-spec get_coverage(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type()) ->
    {ok, term()} | {error, term()}.
get_coverage(PoolName, Bucket) ->
    get_coverage(PoolName, Bucket, undefined).

-spec get_coverage(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), MinPartitions :: undefined | non_neg_integer()) ->
    {ok, term()} | {error, term()}.
get_coverage(PoolName, Bucket, MinPartitions) ->
    poolboy:transaction(PoolName, {?GET_COVERAGE_REQ, Bucket, MinPartitions}).

%% Replace Coverage API Functions
%% TODO - Work out the specs for these functions. Can't find what data type Cover is and what the return is.
replace_coverage(PoolName, Bucket, Cover) ->
    replace_coverage(PoolName, Bucket, Cover, []).

replace_coverage(PoolName, Bucket, Cover, Other) ->
    poolboy:transaction(PoolName, {?REPLACE_COVERAGE_REQ, Bucket, Cover, Other}).

%% Get Ring API Functions
-spec get_ring(PoolName :: poolboy:pool()) ->
    {ok, tuple()}.
get_ring(PoolName) ->
    get_ring(PoolName, default_timeout(?GET_RING_TIMEOUT)).

-spec get_ring(PoolName :: poolboy:pool(), Timeout :: timeout()) ->
    {ok, tuple()}.
get_ring(PoolName, Timeout) ->
    poolboy:transaction(PoolName, {?GET_RING_REQ, Timeout}).

%% Get Default Bucket Props API Functions
-spec get_default_bucket_props(PoolName :: poolboy:pool()) ->
    {ok, bucket_props()}.
get_default_bucket_props(PoolName) ->
    get_default_bucket_props(PoolName, default_timeout(?GET_DEFAULT_BUCKET_PROPS_TIMEOUT)).

-spec get_default_bucket_props(PoolName :: poolboy:pool(), Timeout :: timeout()) ->
    {ok, bucket_props()}.
get_default_bucket_props(PoolName, Timeout) ->
    poolboy:transaction(PoolName, {?GET_DEFAULT_BUCKET_PROPS_REQ, Timeout}).

%% Get Nodes API Functions
-spec get_nodes(PoolName :: poolboy:pool()) ->
    {ok, list(atom())}.
get_nodes(PoolName) ->
    get_nodes(PoolName, default_timeout(?GET_NODES_TIMEOUT)).

-spec get_nodes(PoolName :: poolboy:pool(), Timeout :: timeout()) ->
    {ok, list(atom())}.
get_nodes(PoolName, Timeout) ->
    get_nodes(PoolName, {?GET_NODES_REQ, Timeout}).

%% List Search Indexes API Functions
-spec list_search_indexes(PoolName :: poolboy:pool()) ->
    {ok, list(search_index())} | {error, term()}.
list_search_indexes(PoolName) ->
    list_search_indexes(PoolName, []).

-spec list_search_indexes(PoolName :: poolboy:pool(), Options :: list()) ->
    {ok, list(search_index())} | {error, term()}.
list_search_indexes(PoolName, Options) ->
    poolboy:transaction(PoolName, {?LIST_SEARCH_INDEXES_REQ, Options}).

%% Create Search Index API Functions
-spec create_search_index(PoolName :: poolboy:pool(), Index :: binary()) ->
    ok | {error, term()}.
create_search_index(PoolName, Index) ->
    create_search_index(PoolName, Index, <<>>, []).

-spec create_search_index(PoolName :: poolboy:pool(), Index :: binary(), OptionsOrTimeout :: list() | timeout()) ->
    ok | {error, term()}.
create_search_index(PoolName, Index, Options) when is_list(Options) ->
    create_search_index(PoolName, Index, <<>>, Options);
create_search_index(PoolName, Index, Timeout) when ?is_timeout(Timeout)->
    create_search_index(PoolName, Index, <<>>, [{timeout, Timeout}]).

-spec create_search_index(PoolName :: poolboy:pool(), Index :: binary(), SchemaName :: binary(), OptionsOrTimeout :: list() | timeout()) ->
    ok | {error, term()}.
create_search_index(PoolName, Index, SchemaName, Options) when is_list(Options) ->
    poolboy:transaction(PoolName, {?CREATE_SEARCH_INDEX_REQ, Index, SchemaName, Options});
create_search_index(PoolName, Index, SchemaName, Timeout) when ?is_timeout(Timeout) ->
    poolboy:transaction(PoolName, {?CREATE_SEARCH_INDEX_REQ, Index, SchemaName, [{timeout, Timeout}]}).

%% Get Search Index API Functions
-spec get_search_index(PoolName :: poolboy:pool(), Index :: binary()) ->
    {ok, search_index()} | {error, term()}.
get_search_index(PoolName, Index) ->
    get_search_index(PoolName, Index, []).

-spec get_search_index(PoolName :: poolboy:pool(), Index :: binary(), Options :: list()) ->
    {ok, search_index()} | {error, term()}.
get_search_index(PoolName, Index, Options) ->
    poolboy:transaction(PoolName, {?GET_SEARCH_INDEX_REQ, Index, Options}).

%% Delete Search Index API Functions
-spec delete_search_index(PoolName :: poolboy:pool(), Index :: binary()) ->
    ok | {error, term()}.
delete_search_index(PoolName, Index) ->
    delete_search_index(PoolName, Index, []).

-spec delete_search_index(PoolName :: poolboy:pool(), Index :: binary(), Options :: list()) ->
    ok | {error, term()}.
delete_search_index(PoolName, Index, Options) ->
    poolboy:transaction(PoolName, {?DELETE_SEARCH_INDEX_REQ, Index, Options}).

%% Set Search Index API Function
-spec set_search_index(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Index :: binary()) ->
    ok | {error, term()}.
set_search_index(PoolName, Bucket, Index) ->
    poolboy:transaction(PoolName, {?SET_SEARCH_INDEX_REQ, Bucket, Index}).

%% Get Search Schema API Functions
-spec get_search_schema(PoolName :: poolboy:pool(), SchemaName :: binary()) ->
    {ok, search_schema()} | {error, term()}.
get_search_schema(PoolName, SchemaName) ->
    get_search_schema(PoolName, SchemaName, []).

-spec get_search_schema(PoolName :: poolboy:pool(), SchemaName :: binary(), Options :: list()) ->
    {ok, search_schema()} | {error, term()}.
get_search_schema(PoolName, SchemaName, Options) ->
    poolboy:transaction(PoolName, {?GET_SEARCH_SCHEMA_REQ, SchemaName, Options}).

%% Create Search Schema API Functions
-spec create_search_schema(PoolName :: poolboy:pool(), SchemaName :: binary(), Content :: binary()) ->
    ok | {error, term()}.
create_search_schema(PoolName, SchemaName, Content) ->
    create_search_schema(PoolName, SchemaName, Content, []).

-spec create_search_schema(PoolName :: poolboy:pool(), SchemaName :: binary(), Content :: binary(), Options :: list()) ->
    ok | {error, term()}.
create_search_schema(PoolName, SchemaName, Content, Options) ->
    poolboy:transaction(PoolName, {?CREATE_SEARCH_SCHEMA_REQ, SchemaName, Content, Options}).

%% Counter Incr API Functions
-spec counter_incr(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Amount :: integer()) ->
    {ok, binary()} | {error, term()}.
counter_incr(PoolName, Bucket, Key, Amount) ->
    counter_incr(PoolName, Bucket, Key, Amount, []).

-spec counter_incr(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Amount :: integer(), Options :: list()) ->
    {ok, binary()} | {error, term()}.
counter_incr(PoolName, Bucket, Key, Amount, Options) ->
    poolboy:transaction(PoolName, {?COUNTER_INCR_REQ, Bucket, Key, Amount, Options}).

%% Counter Val API Functions
-spec counter_val(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key()) ->
    {ok, integer()} | {error, term()}.
counter_val(PoolName, Bucket, Key) ->
    counter_val(PoolName, Bucket, Key, []).

-spec counter_val(PoolName :: poolboy:pool(), Bucket :: bucket() | bucket_and_type(), Key :: key(), Options :: list()) ->
    {ok, integer()} | {error, term()}.
counter_val(PoolName, Bucket, Key, Options) ->
    poolboy:transaction(PoolName, {?COUNTER_VAL_REQ, Bucket, Key, Options}).

%% Fetch Type API Functions
-spec fetch_type(PoolName :: poolboy:pool(), BucketAndType :: bucket_and_type(), Key :: binary()) ->
    {ok, riakc_datatype:datatype()} | {error, term()}.
fetch_type(PoolName, BucketAndType, Key) ->
    fetch_type(PoolName, BucketAndType, Key, []).

-spec fetch_type(PoolName :: poolboy:pool(), BucketAndType :: bucket_and_type(), Key :: binary(), Options :: list()) ->
    {ok, riakc_datatype:datatype()} | {error, term()}.
fetch_type(PoolName, BucketAndType, Key, Options) ->
    poolboy:transaction(PoolName, {?FETCH_TYPE_REQ, BucketAndType, Key, Options}).

%% Update Type API Functions
-spec update_type(PoolName :: poolboy:pool(), BucketAndType :: bucket_and_type(), Key :: binary(), Update :: riakc_datatype:update(term())) ->
    ok | {ok, binary()} | {ok, riakc_datatype:datatype()} | {ok, binary(), riakc_datatype:datatype()} | {error, term()}.
update_type(PoolName, BucketAndType, Key, Update) ->
    update_type(PoolName, BucketAndType, Key, Update, []).

-spec update_type(PoolName :: poolboy:pool(), BucketAndType :: bucket_and_type(), Key :: binary(), Update :: riakc_datatype:update(term()), list()) ->
    ok | {ok, binary()} | {ok, riakc_datatype:datatype()} | {ok, binary(), riakc_datatype:datatype()} | {error, term()}.
update_type(PoolName, BucketAndType, Key, Update, Options) ->
    poolboy:transaction(PoolName, {?UPDATE_TYPE_REQ, BucketAndType, Key, Update, Options}).

%% Modify Type API Function
-spec modify_type(PoolName :: poolboy:pool(), Fun :: fun((riakc_datatype:datatype()) -> riakc_datatype:datatype()), BucketAndType :: bucket_and_type(), Key :: binary(), ModifyOptions :: list()) ->
    ok | {ok, riakc_datatype:datatype()} | {error, term()}.
modify_type(PoolName, Fun, BucketAndType, Key, ModifyOptions) ->
    poolboy:transaction(PoolName, {?MODIFY_TYPE_REQ, Fun, BucketAndType, Key, ModifyOptions}).

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
