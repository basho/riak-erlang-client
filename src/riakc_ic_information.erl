%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2020, bet365
%%% @doc
%%%
%%% @end
%%% Created : 28. Jan 2020 09:22
%%%-------------------------------------------------------------------
-module(riakc_ic_information).
-author("paulhunt").

-include("riakc.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0, update_nodes/1, get_preflist/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type riakc_preflist() :: list({{pos_integer(), atom(), atom()}}).

-define(SERVER, ?MODULE).
-define(UPDATE_NODES, update_nodes).
-define(GET_PREFLIST, get_preflist).

-record(state, {
    ring                    :: riak_pb_ring(),
    default_bucket_props    :: list({atom(), term()}),
    nodes_list              :: list(atom())
}).

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec update_nodes(Nodes :: list(atom())) ->
    ok.
update_nodes(Nodes) ->
    gen_server:cast(?SERVER, {?UPDATE_NODES, Nodes}),
    ok.

-spec get_preflist(Bucket :: bucket() | bucket_and_type(), Key :: binary(), Options :: list(term())) ->
    riakc_preflist().
get_preflist(Bucket, Key, Options) ->
    gen_server:call(?SERVER, {?GET_PREFLIST, Bucket, Key, Options}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    State = get_init_information(),
    {ok, State}.

handle_call({?GET_PREFLIST, Bucket, Key, Options}, _From, State) ->
    handle_get_preflist(Bucket, Key, Options, State),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({?UPDATE_NODES, Nodes}, State) ->
    {ok, NewState} = handle_update_nodes(Nodes, State),
    {noreply, NewState};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_init_information() ->
    %% Create connection here just to get things working and tested. Normally will be given an 'admin' connection to do this with.
    {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 10017),
    {ok, Nodes} = riakc_pb_socket:get_nodes(Pid),
    {ok, Ring} = riakc_pb_socket:get_ring(Pid),
    {ok, DefaultBucketProps} = riakc_pb_socket:get_default_bucket_props(Pid),
    #state{nodes_list = Nodes, ring = Ring, default_bucket_props = DefaultBucketProps}.

handle_update_nodes(Nodes, State) ->
    NewState = State#state{nodes_list = Nodes},
    {ok, NewState}.

handle_get_preflist(Bucket, Key, Options, State) ->
    #state{ring = Ring, nodes_list = UpNodes, default_bucket_props = DefaultBucketProps} = State,
    BucketKey = {Bucket, Key},
    BucketProps = get_bucket_props(Bucket, Ring, DefaultBucketProps),
    ChRing = Ring#riak_pb_ring.chring,
    DocIdx = riakc_chash:chash_key(BucketKey),
    Nval = get_nval(Options, BucketProps),
    SloppyQuorum = proplists:get_value(sloppy_quorum, Options),
    Preflist2 = calculate_preflist(SloppyQuorum, DocIdx, Nval, ChRing, UpNodes),
    [IndexNode || {IndexNode, _Type} <- Preflist2].

get_bucket_props({<<"default">>, Name}, Ring, DefaultBucketProps) ->
    get_bucket_props(Name, Ring, DefaultBucketProps);
get_bucket_props({_Type, _Name}, _Ring, DefaultBucketProps) ->
    %% TODO - Report error or find way of getting bucket props for non-default buckets, as non default buckets aren't
    %% TODO         stored in ring. Potentially could create a get request for the bucket props, then store bucket
    %% TODO         props for that bucket to be used for any other calls for that bucket.
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

get_nval(Options, BucketProps) ->
    BucketNval = proplists:get_value(n_val, BucketProps),
    case proplists:get_value(n_val, Options) of
        undefined ->
            BucketNval;
        Nval when is_integer(Nval) andalso Nval > 0 andalso Nval =< BucketNval ->
            Nval;
        BadNval ->
            {error, n_val_violation, BadNval}
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

find_fallbacks_chbin(Pangs, Fallbacks, _UpNodes, Secondaries) when Pangs == [] orelse Fallbacks == done ->
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
