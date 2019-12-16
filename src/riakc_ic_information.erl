%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019, bet365
%%% @doc
%%%
%%% @end
%%% Created : 30. Oct 2019 10:34
%%%-------------------------------------------------------------------
-module(riakc_ic_information).
-author("paulhunt").

-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include("riakc.hrl").

%% API
-export([
	get_hash_fun/1,
	get_preflist/3,
	get_bucket_type_info/1
]).

-type riakc_preflist() :: list({{pos_integer(), atom()}, atom()}).

-define(DEFAULT_HASH_FUN, {riak_core_util, chash_std_keyfun}).

-define(get_hash_fun_guard(Ring, DefaultProps), erlang:is_record(Ring, riak_pb_ring) andalso erlang:is_list(DefaultProps)).

%% ====================================================================
%% API functions
%% ====================================================================
-spec get_hash_fun(Bucket :: bucket() | bucket_and_type()) ->
	{atom(), atom()}.
get_hash_fun(_Bucket) ->
	?DEFAULT_HASH_FUN.

-spec get_preflist(Bucket :: binary(), Key :: binary(), Options :: list(term())) ->
    riakc_preflist().
get_preflist(Bucket, Key, Options) ->
    {ok, Ring} = riakc_ic:get_ring(),
    {ok, DefaultBucketProps} = riakc_ic:get_default_bucket_props(),
    {ok, UpNodes} = riakc_ic:get_up_nodes(),
    handle_get_preflist(Bucket, Key, Options, Ring, DefaultBucketProps, UpNodes).

%% TODO - Add functionality for the get_bucket_type_info function below.
get_bucket_type_info(_Pid) ->
	ok.

%% ====================================================================
%% Internal functions
%% ====================================================================
handle_get_preflist(Bucket, Key, Options, Ring, DefaultBucketProps, UpNodes) ->
    BucketKey = {Bucket, Key},
    BucketProps = get_bucket_props(Bucket, Ring, DefaultBucketProps),
    ChRing = Ring#riak_pb_ring.chring,
    DocIdx = riakc_chash:chash_key(BucketKey),
    BucketNval = get_option(n_val, BucketProps),
    Nval = get_nval(Options, BucketNval),
    SloppyQuorum = get_option(sloppy_quorum, Options),
    Preflist2 = calculate_preflist(SloppyQuorum, DocIdx, Nval, ChRing, UpNodes),
    [IndexNode || {IndexNode, _Type} <- Preflist2].

%% ====================================================================
%% Helper functions
%% ====================================================================
get_bucket_props({<<"default">>, Name}, Ring, DefaultBucketProps) ->
    get_bucket_props(Name, Ring, DefaultBucketProps);
get_bucket_props({_Type, _Name}, _Ring, DefaultBucketProps) ->
    %% TODO - Report error or find way of getting bucket props for non-default buckets, as non default buckets aren't stored in ring.
    DefaultBucketProps;
get_bucket_props(Bucket, Ring, DefaultBucketProps) ->
    Meta = case dict:find({bucket, Bucket}, Ring#riak_pb_ring.meta) of
               error -> undefined;
               {ok, '$removed'} -> undefined;
               {ok, {_, Value, _}} when Value =:= '$removed' -> undefined;
               {ok, {_, Value, _}} -> {ok, Value}
           end,
    get_bucket_props_1(Bucket, Meta, DefaultBucketProps).

get_bucket_props_1(Name, undefined, DefaultBucketProps) ->
    [{name, Name} | DefaultBucketProps];
get_bucket_props_1(_Name, {ok, Bucket}, _DefaultBucketProps) ->
    Bucket.

get_option(Name, Options) ->
    case lists:keyfind(Name, 1, Options) of
        {_, Val} ->
            Val;
        false ->
            undefined
    end.

get_nval(Options, BucketNval) ->
    case get_option(n_val, Options) of
        undefined ->
            BucketNval;
        Nval when erlang:is_integer(Nval) andalso Nval > 0 andalso Nval =< BucketNval ->
            Nval;
        BadNval ->
            {error, {n_val_violation, BadNval}}
    end.

calculate_preflist(SloppyQuorum, DocIdx, Nval, ChRing, UpNodes) when SloppyQuorum == false ->
    ChBin = riakc_chash:chashring_to_chashbin(ChRing),
    Iterator = riakc_chash:iterator(DocIdx, ChBin),
    {Primaries, _} = riakc_chash:iterator_pop(Nval, Iterator),
    {Up, _} = check_up(Primaries, UpNodes, [], []),
    Up;
calculate_preflist(_SloppyQuorum, DocIdx, Nval, ChRing, UpNodes) ->
    ChBin = riakc_chash:chashring_to_chashbin(ChRing),
    Iterator = riakc_chash:iterator(DocIdx, ChBin),
    {Primaries, Iterator2} = riakc_chash:iterator_pop(Nval, Iterator),
    {Up, Pangs} = check_up(Primaries, UpNodes, [], []),
    Up ++ find_fallbacks_chbin(Pangs, Iterator2, UpNodes, []).

check_up([], _UpNodes, Up, Pangs) ->
    {lists:reverse(Up), lists:reverse(Pangs)};
check_up([{Partition, Node} | Rest], UpNodes, Up, Pangs) ->
    case lists:member(Node, UpNodes) of
        true ->
            check_up(Rest, UpNodes, [{{Partition, Node}, primary} | Up], Pangs);
        false ->
            check_up(Rest, UpNodes, Up, [{Partition, Node} | Pangs])
    end.

find_fallbacks_chbin([], _Fallbacks, _UpNodes, Secondaries) ->
    lists:reverse(Secondaries);
find_fallbacks_chbin(_, done, _UpNodes, Secondaries) ->
    lists:reverse(Secondaries);
find_fallbacks_chbin([{Partition, _Node} | Rest] = Pangs, Iterator, UpNodes, Secondaries) ->
    {_, FallbackNode} = riakc_chash:iterator_value(Iterator),
    Iterator2 = riakc_chash:iterator_next(Iterator),
    case lists:member(FallbackNode, UpNodes) of
        true ->
            NewSecondaries = [{{Partition, FallbackNode}, fallback} | Secondaries],
            find_fallbacks_chbin(Rest, Iterator2, UpNodes, NewSecondaries);
        false ->
            find_fallbacks_chbin(Pangs, Iterator2, UpNodes, Secondaries)
    end.
