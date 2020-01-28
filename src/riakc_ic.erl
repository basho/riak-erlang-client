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
    get/2, get/3, get/4,
    put/2, put/3, put/4,
    delete/2, delete/3, delete/4,
    delete_vclock/3, delete_vclock/4, delete_vclock/5,
    delete_obj/2, delete_obj/3, delete_obj/4,
    list_buckets/1, list_buckets/2, list_buckets/3,
    stream_list_buckets/1, stream_list_buckets/2, stream_list_buckets/3,
    legacy_list_buckets/1, legacy_list_buckets/2,
    list_keys/1, list_keys/2,
    stream_list_keys/1, stream_list_keys/2,
    get_bucket/1, get_bucket/2, get_bucket/3,
    get_bucket_type/2, get_bucket_type/3,
    set_bucket/2, set_bucket/3, set_bucket/4,
    set_bucket_type/3, set_bucket_type/4,
    reset_bucket/1, reset_bucket/2, reset_bucket/3,
    mapred/3, mapred/4, mapred/5,
    mapred_stream/4, mapred_stream/5, mapred_stream/6,
    mapred_bucket/3, mapred_bucket/4, mapred_bucket/5,
    mapred_bucket_stream/5, mapred_bucket_stream/6,
    search/3, search/4, search/5, search/6,
    get_index/3, get_index/4, get_index/5, get_index/6,
    get_index_eq/3, get_index_eq/4,
    get_index_range/4, get_index_range/5,
    cs_bucket_fold/2,
    tunnel/4,
    get_preflist/2, get_preflist/3,
    get_coverage/1, get_coverage/2,
    replace_coverage/2, replace_coverage/3,
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
    set_search_index/2,
    get_search_schema/2, get_search_schema/3,
    create_search_schema/3, create_search_schema/4
]).

%% Pre-Riak 2.0 Counter API - Not for CRDT counters
-export([
    counter_incr/3, counter_incr/4,
    counter_val/2, counter_val/3
]).

%% Datatypes API
-export([
    fetch_type/2, fetch_type/3,
    update_type/3, update_type/4,
    modify_type/4
]).

-define(is_timeout(Timeout), (erlang:is_integer(Timeout) andalso Timeout >= 0) orelse Timeout == infinity).

%%%===================================================================
%%% API Functions
%%%===================================================================
%% Get API Functions
-spec get(Bucket :: bucket() | bucket_and_type(), Key :: key()) ->
    {ok, riakc_obj()} | {error, term()}.
get(Bucket, Key) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get(Pid, Bucket, Key) end).

-spec get(Bucket :: bucket() | bucket_and_type(), Key :: key(), OptionsOrTimeout :: list() | timeout()) ->
    {ok, riakc_obj()} | {error, term()}.
get(Bucket, Key, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get(Pid, Bucket, Key, Options) end);
get(Bucket, Key, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get(Pid, Bucket, Key, Timeout) end).

-spec get(Bucket :: bucket() | bucket_and_type(), Key :: key(), Options :: list(), Timeout :: timeout()) ->
    {ok, riakc_obj()} | {error, term()}.
get(Bucket, Key, Options, Timeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get(Pid, Bucket, Key, Options, Timeout) end).


%% Put API Functions
-spec put(PoolName :: atom(), Obj :: riakc_obj()) ->
    ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
put(PoolName, Obj) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:put(Pid, Obj) end).

-spec put(PoolName :: atom() | bucket_and_type(), Obj :: riakc_obj(), OptionsOrTimeout :: list() | timeout()) ->
    ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
put(PoolName, Obj, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:put(Pid, Obj, Options) end);
put(PoolName, Obj, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:put(Pid, Obj, Timeout) end).

-spec put(PoolName :: atom() | bucket_and_type(), Obj :: riakc_obj(), Options :: list(), Timeout :: timeout()) ->
    ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
put(PoolName, Obj, Options, Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:put(Pid, Obj, Options, Timeout) end).


%% Delete API Functions
-spec delete(Bucket :: bucket() | bucket_and_type(), Key :: key()) ->
    ok | {error, term()}.
delete(Bucket, Key) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:delete(Pid, Bucket, Key) end).

-spec delete(Bucket :: bucket() | bucket_and_type(), Key :: key(), OptionsOrTimeout :: list() | timeout()) ->
    ok | {error, term()}.
delete(Bucket, Key, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:delete(Pid, Bucket, Key, Options) end);
delete(Bucket, Key, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:delete(Pid, Bucket, Key, Timeout) end).

-spec delete(Bucket :: bucket() | bucket_and_type(), Key :: key(), Options :: list(), Timeout :: timeout()) ->
    ok | {error, term()}.
delete(Bucket, Key, Options, Timeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:delete(Pid, Bucket, Key, Options, Timeout) end).


%% Delete Vclock API Functions
-spec delete_vclock(Bucket :: bucket() | bucket_and_type(), Key :: key(), Vclock :: vclock()) ->
    ok | {error, term()}.
delete_vclock(Bucket, Key, Vclock) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:delete_vclock(Pid, Bucket, Key, Vclock) end).

-spec delete_vclock(Bucket :: bucket() | bucket_and_type(), Key :: key(), Vclock :: vclock(), OptionsOrTimeout :: list() | timeout()) ->
    ok | {error, term()}.
delete_vclock(Bucket, Key, Vclock, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:delete_vclock(Pid, Bucket, Key, Vclock, Options) end);
delete_vclock(Bucket, Key, Vclock, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:delete_vclock(Pid, Bucket, Key, Vclock, Timeout) end).

-spec delete_vclock(Bucket :: bucket() | bucket_and_type(), Key :: key(), Vclock :: vclock(), Options :: list(), Timeout :: timeout()) ->
    ok | {error, term()}.
delete_vclock(Bucket, Key, Vclock, Options, Timeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:delete_vclock(Pid, Bucket, Key, Vclock, Options, Timeout) end).


%% Delete Obj API Functions
-spec delete_obj(PoolName :: atom(), Obj :: riakc_obj()) ->
    ok | {error, term()}.
delete_obj(PoolName, Obj) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:delete_obj(Pid, Obj) end).

-spec delete_obj(PoolName :: atom(), Obj :: riakc_obj(), OptionsOrTimeout :: list() | timeout()) ->
    ok | {error, term()}.
delete_obj(PoolName, Obj, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:delete_obj(Pid, Obj, Options) end);
delete_obj(PoolName, Obj, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:delete_obj(Pid, Obj, Timeout) end).

-spec delete_obj(PoolName :: atom(), Obj :: riakc_obj(), Options :: list(), Timeout :: timeout()) ->
    ok | {error, term()}.
delete_obj(PoolName, Obj, Options, Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:delete_obj(Pid, Obj, Options, Timeout) end).


%% TODO - Double check if the spec return should be {ok, list(bucket())} or {ok, list(bucket() | bucket_and_type())}.
-spec list_buckets(PoolName :: atom()) ->
    {ok, list(bucket())} | {error, term()}.
list_buckets(PoolName) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:list_buckets(Pid) end).

-spec list_buckets(PoolName :: atom(), OptionsOrTimeoutOrType :: list() | timeout() | binary()) ->
    {ok, list(bucket())} | {error, term()}.
list_buckets(PoolName, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:list_buckets(Pid, <<"default">>, Options) end);
list_buckets(PoolName, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:list_buckets(Pid, <<"default">>, [{timeout, Timeout}]) end);
list_buckets(PoolName, Type) when erlang:is_binary(Type) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:list_buckets(Pid, Type, []) end).

-spec list_buckets(PoolName :: atom(), Type :: binary(), Options :: list()) ->
    {ok, list(bucket())} | {error, term()}.
list_buckets(PoolName, Type, Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:list_buckets(Pid, Type, Options) end).


%% Stream List Buckets API Functions
%% TODO - Work out specs for stream_list_buckets functions.
stream_list_buckets(PoolName) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:stream_list_buckets(Pid) end).

stream_list_buckets(PoolName, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:stream_list_buckets(Pid, Options) end);
stream_list_buckets(PoolName, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:stream_list_buckets(Pid, Timeout) end);
stream_list_buckets(PoolName, Type) when erlang:is_binary(Type) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:stream_list_buckets(Pid, Type) end).

stream_list_buckets(PoolName, Type, Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:stream_list_buckets(Pid, Type, Options) end).


%% Legacy List Buckets API Functions
%% TODO - Work out specs for legacy_list_buckets functions.
legacy_list_buckets(PoolName) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:legacy_list_buckets(Pid, []) end).

legacy_list_buckets(PoolName, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:legacy_list_buckets(Pid, Options) end);
legacy_list_buckets(PoolName, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:legacy_list_buckets(Pid, [{timeout, Timeout}]) end).


%% List Keys API Functions
-spec list_keys(Bucket :: bucket() | bucket_and_type()) ->
    {ok, list(key())} | {error, term()}.
list_keys(Bucket) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:list_keys(Pid, Bucket) end).

-spec list_keys(Bucket :: bucket() | bucket_and_type(), OptionsOrTimeout :: list() | timeout()) ->
    {ok, list(key())} | {error, term()}.
list_keys(Bucket, infinity) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:list_keys(Pid, Bucket, [{timeout, undefined}]) end);
list_keys(Bucket, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:list_keys(Pid, Bucket, [{timeout, Timeout}]) end);
list_keys(Bucket, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:list_keys(Pid, Bucket, Options) end).


%% Stream List Keys API Functions
-spec stream_list_keys(Bucket :: bucket() | bucket_and_type()) ->
    {ok, req_id()} | {error, term()}.
stream_list_keys(Bucket) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:stream_list_keys(Pid, Bucket) end).

-spec stream_list_keys(Bucket :: bucket() | bucket_and_type(), OptionsOrTimeout :: list() | timeout()) ->
    {ok, req_id()} | {error, term()}.
stream_list_keys(Bucket, infinity) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:stream_list_keys(Pid, Bucket, [{timeout, undefined}]) end);
stream_list_keys(Bucket, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:stream_list_keys(Pid, Bucket, [{timeout, Timeout}]) end);
stream_list_keys(Bucket, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:stream_list_keys(Pid, Bucket, Options) end).


%% Get Bucket API Functions
-spec get_bucket(Bucket :: bucket() | bucket_and_type()) ->
    {ok, bucket_props()} | {error, term()}.
get_bucket(Bucket) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_bucket(Pid, Bucket) end).

-spec get_bucket(Bucket :: bucket() | bucket_and_type(), Timeout :: timeout()) ->
    {ok, bucket_props()} | {error, term()}.
get_bucket(Bucket, Timeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_bucket(Pid, Bucket, Timeout) end).

-spec get_bucket(Bucket :: bucket() | bucket_and_type(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, bucket_props()} | {error, term()}.
get_bucket(Bucket, Timeout, CallTimeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_bucket(Pid, Bucket, Timeout, CallTimeout) end).


%% Get Bucket Type API Functions
%% TODO - Work out the specs for these functions. Can't find what data type BucketType is.
get_bucket_type(PoolName, BucketType) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:get_bucket_type(Pid, BucketType) end).

get_bucket_type(PoolName, BucketType, Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:get_bucket_type(Pid, BucketType, Timeout) end).


%% Set Bucket API Functions
-spec set_bucket(Bucket :: bucket() | bucket_and_type(), BucketProps :: bucket_props()) ->
    ok | {error, term()}.
set_bucket(Bucket, BucketProps) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:set_bucket(Pid, Bucket, BucketProps) end).

-spec set_bucket(Bucket :: bucket() | bucket_and_type(), BucketProps :: bucket_props(), Timeout :: timeout()) ->
    ok | {error, term()}.
set_bucket(Bucket, BucketProps, Timeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:set_bucket(Pid, Bucket, BucketProps, Timeout) end).

-spec set_bucket(Bucket :: bucket() | bucket_and_type(), BucketProps :: bucket_props(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    ok | {error, term()}.
set_bucket(Bucket, BucketProps, Timeout, CallTimeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:set_bucket(Pid, Bucket, BucketProps, Timeout, CallTimeout) end).


%% Set Bucket Type API Functions
%% TODO - Work out the specs for these functions. Can't find what data type BucketType is.
set_bucket_type(PoolName, BucketType, BucketProps) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:set_bucket_type(Pid, BucketType, BucketProps) end).

set_bucket_type(PoolName, BucketType, BucketProps, Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:set_bucket_type(Pid, BucketType, BucketProps, Timeout) end).


%% Reset Bucket API Functions
-spec reset_bucket(Bucket :: bucket() | bucket_and_type()) ->
    ok | {error, term()}.
reset_bucket(Bucket) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:reset_bucket(Pid, Bucket) end).

-spec reset_bucket(Bucket :: bucket() | bucket_and_type(), Timeout :: timeout()) ->
    ok | {error, term()}.
reset_bucket(Bucket, Timeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:reset_bucket(Pid, Bucket, Timeout) end).

-spec reset_bucket(Bucket :: bucket() | bucket_and_type(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    ok | {error, term()}.
reset_bucket(Bucket, Timeout, CallTimeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:reset_bucket(Pid, Bucket, Timeout, CallTimeout) end).


%% Mapred API Functions
-spec mapred(PoolName ::atom(), Inputs :: mapred_inputs(), Query :: list(mapred_queryterm())) ->
    {ok, mapred_result()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred(PoolName, Inputs, Query) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:mapred(Pid, Inputs, Query) end).

-spec mapred(PoolName ::atom(), Inputs :: mapred_inputs(), Query :: list(mapred_queryterm()), Timeout :: timeout()) ->
    {ok, mapred_result()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred(PoolName, Inputs, Query, Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:mapred(Pid, Inputs, Query, Timeout) end).

-spec mapred(PoolName ::atom(), Inputs :: mapred_inputs(), Query :: list(mapred_queryterm()), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, mapred_result()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred(PoolName, Inputs, Query, Timeout, CallTimeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:mapred(Pid, Inputs, Query, Timeout, CallTimeout) end).


%% Mapred Stream API Functions
mapred_stream(PoolName, Inputs, Query, ClientPid) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:mapred_stream(Pid, Inputs, Query, ClientPid) end).

mapred_stream(PoolName, Inputs, Query, ClientPid, Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:mapred_stream(Pid, Inputs, Query, ClientPid, Timeout) end).

mapred_stream(PoolName, Inputs, Query, ClientPid, Timeout, CallTimeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:mapred_stream(Pid, Inputs, Query, ClientPid, Timeout, CallTimeout) end).


%% Mapred Bucket API Functions
-spec mapred_bucket(PoolName :: atom(), Bucket :: bucket(), Query :: list(mapred_queryterm())) ->
    {ok, mapred_result()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred_bucket(PoolName, Bucket, Query) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:mapred_bucket(Pid, Bucket, Query) end).

-spec mapred_bucket(PoolName :: atom(), Bucket :: bucket(), Query :: list(mapred_queryterm()), Timeout :: timeout()) ->
    {ok, mapred_result()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred_bucket(PoolName, Bucket, Query, Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:mapred_bucket(Pid, Bucket, Query, Timeout) end).

-spec mapred_bucket(PoolName :: atom(), Bucket :: bucket(), Query :: list(mapred_queryterm()), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, mapred_result()} | {error, {badqterm, mapred_queryterm()}} | {error, timeout} | {error, term()}.
mapred_bucket(PoolName, Bucket, Query, Timeout, CallTimeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:mapred_bucket(Pid, Bucket, Query, Timeout, CallTimeout) end).


%% Mapred Bucket Stream API Functions
%% TODO - Work out spec for mapred_bucket_stream functions.
mapred_bucket_stream(PoolName, Bucket, Query, ClientPid, Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:mapred_bucket_stream(Pid, Bucket, Query, ClientPid, Timeout) end).

mapred_bucket_stream(PoolName, Bucket, Query, ClientPid, Timeout, CallTimeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:mapred_bucket_stream(Pid, Bucket, Query, ClientPid, Timeout, CallTimeout) end).


%% Search API Functions
-spec search(PoolName :: atom(), Index :: binary(), SearchQuery :: binary()) ->
    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:search(Pid, Index, SearchQuery) end).

-spec search(PoolName :: atom(), Index :: binary(), SearchQuery :: binary(), OptionsOrTimeout :: list() | timeout()) ->
    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:search(Pid, Index, SearchQuery, Options) end);
search(PoolName, Index, SearchQuery, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:search(Pid, Index, SearchQuery, Timeout) end).

-spec search(PoolName :: atom(), Index :: binary(), SearchQuery :: binary(), OptionsOrTimeout :: list() | timeout(), TimeoutOrCallTimeout :: timeout()) ->
    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery, Options, Timeout) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:search(Pid, Index, SearchQuery, Options, Timeout) end);
search(PoolName, Index, SearchQuery, Timeout, CallTimeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:search(Pid, Index, SearchQuery, Timeout, CallTimeout) end).

-spec search(PoolName :: atom(), Index :: binary(), SearchQuery :: binary(), Options :: list(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery, Options, Timeout, CallTimeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:search(Pid, Index, SearchQuery, Options, Timeout, CallTimeout) end).


%% Get Index API Functions
-spec get_index(Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), Key :: key() | integer()) ->
    {ok, index_results()} | {error, term()}.
get_index(Bucket, Index, Key) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_index(Pid, Bucket, Index, Key) end).

-spec get_index(Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), StartKey :: key() | integer(), EndKey :: key() | integer()) ->
    {ok, index_results()} | {error, term()}.
get_index(Bucket, Index, StartKey, EndKey) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_index(Pid, Bucket, Index, StartKey, EndKey) end).

-spec get_index(Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), Key :: key() | integer(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, index_results()} | {error, term()}.
get_index(Bucket, Index, Key, Timeout, CallTimeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_index(Pid, Bucket, Index, Key, Timeout, CallTimeout) end).

-spec get_index(Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), StartKey :: key() | integer(), EndKey :: key() | integer(), Timeout :: timeout(), CallTimeout :: timeout()) ->
    {ok, index_results()} | {error, term()}.
get_index(Bucket, Index, StartKey, EndKey, Timeout, CallTimeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_index(Pid, Bucket, Index, StartKey, EndKey, Timeout, CallTimeout) end).


%% Get Index Eq API Functions
-spec get_index_eq(Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), Key :: key() | integer()) ->
    {ok, index_results()} | {error, term()}.
get_index_eq(Bucket, Index, Key) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_index_eq(Pid, Bucket, Index, Key) end).

-spec get_index_eq(Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), Key :: key() | integer(), Options :: list()) ->
    {ok, index_results()} | {error, term()}.
get_index_eq(Bucket, Index, Key, Options) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_index_eq(Pid, Bucket, Index, Key, Options) end).


%% Get Index Range API Functions
-spec get_index_range(Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), StartKey :: key() | integer(), EndKey :: key() | integer()) ->
    {ok, index_results()} | {error, term()}.
get_index_range(Bucket, Index, StartKey, EndKey) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_index_range(Pid, Bucket, Index, StartKey, EndKey) end).

-spec get_index_range(Bucket :: bucket() | bucket_and_type(), Index :: binary() | tuple(), StartKey :: key() | integer(), EndKey :: key() | integer(), Options :: list()) ->
    {ok, index_results()} | {error, term()}.
get_index_range(Bucket, Index, StartKey, EndKey, Options) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_index_range(Pid, Bucket, Index, StartKey, EndKey, Options) end).


%% Cs Bucket Fold API Function
-spec cs_bucket_fold(Bucket :: bucket() | bucket_and_type(), Options :: list()) ->
    {ok, reference()} | {error, term()}.
cs_bucket_fold(Bucket, Options) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:cs_bucket_fold(Pid, Bucket, Options) end).


%% Tunnel API Function
-spec tunnel(PoolName :: atom(), MessageId :: non_neg_integer(), Packet :: iolist(), Timeout :: timeout()) ->
    {ok, binary()} | {error, term()}.
tunnel(PoolName, MessageId, Packet, Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:tunnel(Pid, MessageId, Packet, Timeout) end).


%% Get Preflist API Functions
-spec get_preflist(Bucket :: bucket() | bucket_and_type(), Key :: key()) ->
    {ok, preflist()} | {error, term()}.
get_preflist(Bucket, Key) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_preflist(Pid, Bucket, Key) end).

-spec get_preflist(Bucket :: bucket() | bucket_and_type(), Key :: key(), Timeout :: timeout()) ->
    {ok, preflist()} | {error, term()}.
get_preflist(Bucket, Key, Timeout) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_preflist(Pid, Bucket, Key, Timeout) end).


%% Get Coverage API Functions
-spec get_coverage(Bucket :: bucket() | bucket_and_type()) ->
    {ok, term()} | {error, term()}.
get_coverage(Bucket) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_coverage(Pid, Bucket) end).

-spec get_coverage(Bucket :: bucket() | bucket_and_type(), MinPartitions :: undefined | non_neg_integer()) ->
    {ok, term()} | {error, term()}.
get_coverage(Bucket, MinPartitions) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:get_coverage(Pid, Bucket, MinPartitions) end).


%% Replace Coverage API Functions
%% TODO - Work out the specs for these functions. Can't find what data type Cover is and what the return is.
replace_coverage(Bucket, Cover) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:replace_coverage(Pid, Bucket, Cover) end).

replace_coverage(Bucket, Cover, Other) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:replace_coverage(Pid, Bucket, Cover, Other) end).


%% Get Ring API Functions
-spec get_ring(PoolName :: atom()) ->
    {ok, tuple()} | {error, term()}.
get_ring(PoolName) ->
    Fun = fun(Pid) -> riakc_pb_socket:get_ring(Pid) end,
    riakc_ic_balcon:transaction_admin(PoolName, Fun).

-spec get_ring(PoolName :: atom(), Timeout :: timeout()) ->
    {ok, tuple()} | {error, term()}.
get_ring(PoolName, Timeout) ->
    riakc_ic_balcon:transaction_admin(PoolName, fun(Pid) -> riakc_pb_socket:get_ring(Pid, Timeout) end).


%% Get Default Bucket Props API Functions
-spec get_default_bucket_props(PoolName :: atom()) ->
    {ok, bucket_props()} | {error, term()}.
get_default_bucket_props(PoolName) ->
    riakc_ic_balcon:transaction_admin(PoolName, fun(Pid) -> riakc_pb_socket:get_default_bucket_props(Pid) end).

-spec get_default_bucket_props(PoolName :: atom(), Timeout :: timeout()) ->
    {ok, bucket_props()} | {error, term()}.
get_default_bucket_props(PoolName, Timeout) ->
    riakc_ic_balcon:transaction_admin(PoolName, fun(Pid) -> riakc_pb_socket:get_default_bucket_props(Pid, Timeout) end).


%% Get Nodes API Functions
-spec get_nodes(PoolName :: atom()) ->
    {ok, list(atom())} | {error, term()}.
get_nodes(PoolName) ->
    riakc_ic_balcon:transaction_admin(PoolName, fun(Pid) -> riakc_pb_socket:get_nodes(Pid) end).

-spec get_nodes(PoolName :: atom(), Timeout :: timeout()) ->
    {ok, list(atom())} | {error, term()}.
get_nodes(PoolName, Timeout) ->
    riakc_ic_balcon:transaction_admin(PoolName, fun(Pid) -> riakc_pb_socket:get_nodes(Pid, Timeout) end).


%% List Search Indexes API Functions
-spec list_search_indexes(PoolName :: atom()) ->
    {ok, list(search_index())} | {error, term()}.
list_search_indexes(PoolName) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:list_search_indexes(Pid) end).

-spec list_search_indexes(PoolName :: atom(), Options :: list()) ->
    {ok, list(search_index())} | {error, term()}.
list_search_indexes(PoolName, Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:list_search_indexes(Pid, Options) end).


%% Create Search Index API Functions
-spec create_search_index(PoolName :: atom(), Index :: binary()) ->
    ok | {error, term()}.
create_search_index(PoolName, Index) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:create_search_index(Pid, Index) end).

-spec create_search_index(PoolName :: atom(), Index :: binary(), OptionsOrTimeout :: list() | timeout()) ->
    ok | {error, term()}.
create_search_index(PoolName, Index, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:create_search_index(Pid, Index, Options) end);
create_search_index(PoolName, Index, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:create_search_index(Pid, Index, Timeout) end).

-spec create_search_index(PoolName :: atom(), Index :: binary(), SchemaName :: binary(), OptionsOrTimeout :: list() | timeout()) ->
    ok | {error, term()}.
create_search_index(PoolName, Index, SchemaName, Options) when erlang:is_list(Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:create_search_index(Pid, Index, SchemaName, Options) end);
create_search_index(PoolName, Index, SchemaName, Timeout) when ?is_timeout(Timeout) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:create_search_index(Pid, Index, SchemaName, Timeout) end).


%% Get Search Index API Functions
-spec get_search_index(PoolName :: atom(), Index :: binary()) ->
    {ok, search_index()} | {error, term()}.
get_search_index(PoolName, Index) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:get_search_index(Pid, Index) end).

-spec get_search_index(PoolName :: atom(), Index :: binary(), Options :: list()) ->
    {ok, search_index()} | {error, term()}.
get_search_index(PoolName, Index, Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:get_search_index(Pid, Index, Options) end).


%% Delete Search Index API Functions
-spec delete_search_index(PoolName :: atom(), Index :: binary()) ->
    ok | {error, term()}.
delete_search_index(PoolName, Index) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:delete_search_index(Pid, Index) end).

-spec delete_search_index(PoolName :: atom(), Index :: binary(), Options :: list()) ->
    ok | {error, term()}.
delete_search_index(PoolName, Index, Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:delete_search_index(Pid, Index, Options) end).


%% Set Search Index API Function
-spec set_search_index(Bucket :: bucket() | bucket_and_type(), Index :: binary()) ->
    ok | {error, term()}.
set_search_index(Bucket, Index) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:set_search_index(Pid, Bucket, Index) end).


%% Get Search Schema API Functions
-spec get_search_schema(PoolName :: atom(), SchemaName :: binary()) ->
    {ok, search_schema()} | {error, term()}.
get_search_schema(PoolName, SchemaName) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:get_search_schema(Pid, SchemaName) end).

-spec get_search_schema(PoolName :: atom(), SchemaName :: binary(), Options :: list()) ->
    {ok, search_schema()} | {error, term()}.
get_search_schema(PoolName, SchemaName, Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:get_search_schema(Pid, SchemaName, Options) end).


%% Create Search Schema API Functions
-spec create_search_schema(PoolName :: atom(), SchemaName :: binary(), Content :: binary()) ->
    ok | {error, term()}.
create_search_schema(PoolName, SchemaName, Content) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:create_search_schema(Pid, SchemaName, Content) end).

-spec create_search_schema(PoolName :: atom(), SchemaName :: binary(), Content :: binary(), Options :: list()) ->
    ok | {error, term()}.
create_search_schema(PoolName, SchemaName, Content, Options) ->
    riakc_ic_balcon:transaction(PoolName, fun(Pid) -> riakc_pb_socket:create_search_schema(Pid, SchemaName, Content, Options) end).


%% Counter Incr API Functions
-spec counter_incr(Bucket :: bucket() | bucket_and_type(), Key :: key(), Amount :: integer()) ->
    {ok, binary()} | {error, term()}.
counter_incr(Bucket, Key, Amount) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:counter_incr(Pid, Bucket, Key, Amount) end).

-spec counter_incr(Bucket :: bucket() | bucket_and_type(), Key :: key(), Amount :: integer(), Options :: list()) ->
    {ok, binary()} | {error, term()}.
counter_incr(Bucket, Key, Amount, Options) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:counter_incr(Pid, Bucket, Key, Amount, Options) end).


%% Counter Val API Functions
-spec counter_val(Bucket :: bucket() | bucket_and_type(), Key :: key()) ->
    {ok, integer()} | {error, term()}.
counter_val(Bucket, Key) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:counter_val(Pid, Bucket, Key) end).

-spec counter_val(Bucket :: bucket() | bucket_and_type(), Key :: key(), Options :: list()) ->
    {ok, integer()} | {error, term()}.
counter_val(Bucket, Key, Options) ->
    riakc_ic_balcon:transaction(Bucket, fun(Pid) -> riakc_pb_socket:counter_val(Pid, Bucket, Key, Options) end).


%% Fetch Type API Functions
-spec fetch_type(BucketAndType :: bucket_and_type(), Key :: binary()) ->
    {ok, riakc_datatype:datatype()} | {error, term()}.
fetch_type(BucketAndType, Key) ->
    riakc_ic_balcon:transaction(BucketAndType, fun(Pid) -> riakc_pb_socket:fetch_type(Pid, BucketAndType, Key) end).

-spec fetch_type(BucketAndType :: bucket_and_type(), Key :: binary(), Options :: list()) ->
    {ok, riakc_datatype:datatype()} | {error, term()}.
fetch_type(BucketAndType, Key, Options) ->
    riakc_ic_balcon:transaction(BucketAndType, fun(Pid) -> riakc_pb_socket:fetch_type(Pid, BucketAndType, Key, Options) end).


%% Update Type API Functions
-spec update_type(BucketAndType :: bucket_and_type(), Key :: binary(), Update :: riakc_datatype:update(term())) ->
    ok | {ok, binary()} | {ok, riakc_datatype:datatype()} | {ok, binary(), riakc_datatype:datatype()} | {error, term()}.
update_type(BucketAndType, Key, Update) ->
    riakc_ic_balcon:transaction(BucketAndType, fun(Pid) -> riakc_pb_socket:update_type(Pid, BucketAndType, Key, Update) end).

-spec update_type(BucketAndType :: bucket_and_type(), Key :: binary(), Update :: riakc_datatype:update(term()), list()) ->
    ok | {ok, binary()} | {ok, riakc_datatype:datatype()} | {ok, binary(), riakc_datatype:datatype()} | {error, term()}.
update_type(BucketAndType, Key, Update, Options) ->
    riakc_ic_balcon:transaction(BucketAndType, fun(Pid) -> riakc_pb_socket:update_type(Pid, BucketAndType, Key, Update, Options) end).


%% Modify Type API Function
-spec modify_type(Fun :: fun((riakc_datatype:datatype()) -> riakc_datatype:datatype()), BucketAndType :: bucket_and_type(), Key :: binary(), ModifyOptions :: list()) ->
    ok | {ok, riakc_datatype:datatype()} | {error, term()}.
modify_type(BucketAndType, Fun, Key, ModifyOptions) ->
    riakc_ic_balcon:transaction(BucketAndType, fun(Pid) -> riakc_pb_socket:modify_type(Pid, Fun, BucketAndType, Key, ModifyOptions) end).
