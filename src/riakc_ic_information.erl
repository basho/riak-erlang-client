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
	get_hash_fun/3,
	get_preflist/5,
	get_bucket_type_info/1
]).

-type ring() :: #riak_pb_ring{}.
-type riakc_bucket_props() :: list({atom(), term()}).
-type riakc_preflist() :: list({{pos_integer(), atom()}, atom()}).

-define(DEFAULT_HASH_FUN, {riak_core_util, chash_std_keyfun}).

-define(get_hash_fun_guard(Ring, DefaultProps), erlang:is_record(Ring, riak_pb_ring) andalso erlang:is_list(DefaultProps)).

%% ====================================================================
%% API functions
%% ====================================================================
-spec get_hash_fun(Bucket :: bucket() | bucket_and_type(), Ring :: ring(), DefaultBucketProps :: riakc_bucket_props()) ->
	{atom(), atom()}.
get_hash_fun(_Bucket, Ring, DefaultBucketProps) when ?get_hash_fun_guard(Ring, DefaultBucketProps) ->
	?DEFAULT_HASH_FUN.

-spec get_preflist(Bucket :: binary(), Key :: binary(), Ring :: ring(), Options :: list(term()), DefaultBucketProps :: riakc_bucket_props()) ->
    riakc_preflist().
get_preflist(Bucket, Key, Ring, Options, DefaultBucketProps) ->
    BucketKey = {Bucket, Key},
    BucketProps = get_bucket_props(Bucket, Ring, DefaultBucketProps),
    ChRing = Ring#riak_pb_ring.chring,
    DocIdx = riakc_chash:chash_key(BucketKey),
    BucketNval = get_option(n_val, BucketProps),
    Nval = get_nval(Options, BucketNval),
    Preflist2 = case get_option(sloppy_quorum, Options) of
                    false ->
                        get_primary_apl(DocIdx, Nval, ChRing);
                    _ ->
                        UpNodes = get_up_nodes(),
                        get_apl_ann(DocIdx, Nval, UpNodes)
                end,
    [IndexNode || {IndexNode, _Type} <- Preflist2].

%% TODO - Add functionality for the get_bucket_type_info function below
get_bucket_type_info(_Pid) ->
	ok.

%% ====================================================================
%% Internal functions
%% ====================================================================
get_option(Name, Options) ->
    get_option(Name, Options, undefined).

get_option(Name, Options, Default) ->
    case lists:keyfind(Name, 1, Options) of
        {_, Val} ->
            Val;
        false ->
            Default
    end.

get_bucket_props({<<"default">>, Name}, Ring, DefaultBucketProps) ->
    get_bucket_props(Name, Ring, DefaultBucketProps);
get_bucket_props({_Type, _Name}, _Ring, _DefaultBucketProps) ->
    %% TODO - Report error or find way of getting bucket props for non-default buckets, as non default buckets aren't stored in ring
    ok;
get_bucket_props(Bucket, Ring, DefaultBucketProps) ->
    Meta = get_meta({bucket, Bucket}, Ring),
    get_bucket_props_1(Bucket, Meta, DefaultBucketProps).

get_bucket_props_1(Name, undefined, DefaultBucketProps) ->
    [{name, Name} | DefaultBucketProps];
get_bucket_props_1(_Name, {ok, Bucket}, _DefaultBucketProps) ->
    Bucket.

get_meta(Key, Ring) ->
    case dict:find(Key, Ring#riak_pb_ring.meta) of
        error -> undefined;
        {ok, '$removed'} -> undefined;
        {ok, Meta} when Meta#riakc_meta_entry.value =:= '$removed' -> undefined;
        {ok, Meta} -> {ok, Meta#riakc_meta_entry.value}
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

get_primary_apl(DocIdx, Nval, ChRing) ->
    ChBin = riakc_chash:chashring_to_chashbin(ChRing),
    UpNodes = get_up_nodes(),
    get_primary_apl_chbin(DocIdx, Nval, ChBin, UpNodes).

get_apl_ann(_DocIdx, _Nval, _UpNodes) ->
    %% TODO - Fill in implementation for this function.
    ok.

get_up_nodes() ->
    %% TODO - Fill in implementation for this function.
    ok.

get_primary_apl_chbin(DocIdx, Nval, ChBin, UpNodes) ->
    Iterator = riakc_chash:iterator(DocIdx, ChBin),
    {Primaries, _} = riakc_chash:iterator_pop(Nval, Iterator),
    {Up, _} = check_up(Primaries, UpNodes, [], []),
    Up.

check_up([], _UpNodes, Up, Pangs) ->
    {lists:reverse(Up), lists:reverse(Pangs)};
check_up([{Partition, Node} | Rest], UpNodes, Up, Pangs) ->
    case lists:member(Node, UpNodes) of
        true ->
            check_up(Rest, UpNodes, [{{Partition, Node}, primary} | Up], Pangs);
        false ->
            check_up(Rest, UpNodes, Up, [{Partition, Node} | Pangs])
    end.
