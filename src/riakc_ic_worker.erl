%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019, bet365
%%% @doc
%%%
%%% @end
%%% Created : 17. Dec 2019 08:49
%%%-------------------------------------------------------------------
-module(riakc_ic_worker).
-author("paulhunt").

-behaviour(gen_server).
-behaviour(poolboy_worker).

-include("riakc_ic.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {riak_conn}).

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_link(Args :: list()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Args) ->
    gen_server:start_link(?SERVER, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init(Args) ->
    erlang:process_flag(trap_exit, true),
    HostName = proplists:get_value(hostname, Args),
    Port = proplists:get_value(port, Args),
    {ok, RiakConn} = riakc_pb_socket:start_link(HostName, Port),
    {ok, #state{riak_conn = RiakConn}}.

handle_call({?GET_REQ, Bucket, Key, Options, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get(RiakConn, Bucket, Key, Options, Timeout),
    {reply, Result, State};
handle_call({?PUT_REQ, Obj, Options, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:put(RiakConn, Obj, Options, Timeout),
    {reply, Result, State};
handle_call({?DELETE_REQ, Bucket, Key, Options, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:delete(RiakConn, Bucket, Key, Options, Timeout),
    {reply, Result, State};
handle_call({?DELETE_VCLOCK_REQ, Bucket, Key, Vclock, Options, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:delete_vclock(RiakConn, Bucket, Key, Vclock, Options, Timeout),
    {reply, Result, State};
handle_call({?DELETE_OBJ_REQ, Obj, Options, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:delete_obj(RiakConn, Obj, Options, Timeout),
    {reply, Result, State};
handle_call({?LIST_BUCKETS_REQ, Type, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:list_buckets(RiakConn, Type, Options),
    {reply, Result, State};
handle_call({?STREAM_LIST_BUCKETS_REQ, Type, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:stream_list_buckets(RiakConn, Type, Options),
    {reply, Result, State};
handle_call({?LEGACY_LIST_BUCKETS_REQ, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:legacy_list_buckets(RiakConn, Options),
    {reply, Result, State};
handle_call({?LIST_KEYS_REQ, Bucket, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:list_keys(RiakConn, Bucket, Options),
    {reply, Result, State};
handle_call({?STREAM_LIST_KEYS_REQ, Bucket, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:stream_list_keys(RiakConn, Bucket, Options),
    {reply, Result, State};
handle_call({?GET_BUCKET_REQ, Bucket, Timeout, CallTimeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_bucket(RiakConn, Bucket, Timeout, CallTimeout),
    {reply, Result, State};
handle_call({?GET_BUCKET_TYPE_REQ, BucketType, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_bucket_type(RiakConn, BucketType, Timeout),
    {reply, Result, State};
handle_call({?SET_BUCKET_REQ, Bucket, BucketProps, Timeout, CallTimeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:set_bucket(RiakConn, Bucket, BucketProps, Timeout, CallTimeout),
    {reply, Result, State};
handle_call({?SET_BUCKET_TYPE_REQ, Bucket, BucketProps, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:set_bucket_type(RiakConn, Bucket, BucketProps, Timeout),
    {reply, Result, State};
handle_call({?RESET_BUCKET_REQ, Bucket, Timeout, CallTimeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:reset_bucket(RiakConn, Bucket, Timeout, CallTimeout),
    {reply, Result, State};
handle_call({?MAPRED_REQ, Inputs, Query, Timeout, CallTimeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:mapred(RiakConn, Inputs, Query, Timeout, CallTimeout),
    {reply, Result, State};
handle_call({?MAPRED_STREAM_REQ, Inputs, Query, ClientPid, Timeout, CallTimeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:mapred_stream(RiakConn, Inputs, Query, ClientPid, Timeout, CallTimeout),
    {reply, Result, State};
handle_call({?MAPRED_BUCKET_REQ, Bucket, Query, Timeout, CallTimeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:mapred_bucket(RiakConn, Bucket, Query, Timeout, CallTimeout),
    {reply, Result, State};
handle_call({?MAPRED_BUCKET_STREAM_REQ, Bucket, Query, ClientPid, Timeout, CallTimeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:mapred_bucket_stream(RiakConn, Bucket, Query, ClientPid, Timeout, CallTimeout),
    {reply, Result, State};
handle_call({?SEARCH_REQ, Index, SearchQuery, Options, Timeout, CallTimeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:search(RiakConn, Index, SearchQuery, Options, Timeout, CallTimeout),
    {reply, Result, State};
handle_call({?GET_INDEX_EQ_REQ, Bucket, Index, Key, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_index_eq(RiakConn, Bucket, Index, Key, Options),
    {reply, Result, State};
handle_call({?GET_INDEX_RANGE_REQ, Bucket, Index, StartKey, EndKey, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_index_range(RiakConn, Bucket, Index, StartKey, EndKey, Options),
    {reply, Result, State};
handle_call({?CS_BUCKET_FOLD_REQ, Bucket, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:cs_bucket_fold(RiakConn, Bucket, Options),
    {reply, Result, State};
handle_call({?TUNNEL_REQ, MessageId, Packet, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:tunnel(RiakConn, MessageId, Packet, Timeout),
    {reply, Result, State};
handle_call({?GET_PREFLIST_REQ, Bucket, Key, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_preflist(RiakConn, Bucket, Key, Timeout),
    {reply, Result, State};
handle_call({?GET_COVERAGE_REQ, Bucket, MinPartitions}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_coverage(RiakConn, Bucket, MinPartitions),
    {reply, Result, State};
handle_call({?REPLACE_COVERAGE_REQ, Bucket, Cover, Other}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:replace_coverage(RiakConn, Bucket, Cover, Other),
    {reply, Result, State};
handle_call({?GET_RING_REQ, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_ring(RiakConn, Timeout),
    {reply, Result, State};
handle_call({?GET_DEFAULT_BUCKET_PROPS_REQ, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_default_bucket_props(RiakConn, Timeout),
    {reply, Result, State};
handle_call({?GET_NODES_REQ, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_nodes(RiakConn, Timeout),
    {reply, Result, State};
handle_call({?LIST_SEARCH_INDEXES_REQ, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:list_search_indexes(RiakConn, Timeout),
    {reply, Result, State};
handle_call({?CREATE_SEARCH_INDEX_REQ, Index, SchemaName, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:create_search_index(RiakConn, Index, SchemaName, Options),
    {reply, Result, State};
handle_call({?GET_SEARCH_INDEX_REQ, Index, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_search_index(RiakConn, Index, Options),
    {reply, Result, State};
handle_call({?DELETE_SEARCH_INDEX_REQ, Index, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:delete_search_index(RiakConn, Index, Options),
    {reply, Result, State};
handle_call({?SET_SEARCH_INDEX_REQ, Bucket, Index}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:set_search_index(RiakConn, Bucket, Index),
    {reply, Result, State};
handle_call({?GET_SEARCH_SCHEMA_REQ, SchemaName, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_search_schema(RiakConn, SchemaName, Options),
    {reply, Result, State};
handle_call({?CREATE_SEARCH_SCHEMA_REQ, SchemaName, Content, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:create_search_schema(RiakConn, SchemaName, Content, Options),
    {reply, Result, State};
handle_call({?COUNTER_INCR_REQ, Bucket, Key, Amount, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:counter_incr(RiakConn, Bucket, Key, Amount, Options),
    {reply, Result, State};
handle_call({?COUNTER_VAL_REQ, Bucket, Key, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:counter_val(RiakConn, Bucket, Key, Options),
    {reply, Result, State};
handle_call({?FETCH_TYPE_REQ, BucketAndType, Key, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:fetch_type(RiakConn, BucketAndType, Key, Options),
    {reply, Result, State};
handle_call({?UPDATE_TYPE_REQ, BucketAndType, Key, Update, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:update_type(RiakConn, BucketAndType, Key, Update, Options),
    {reply, Result, State};
handle_call({?MODIFY_TYPE_REQ, Fun, BucketAndType, Key, ModifyOptions}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:modify_type(RiakConn, Fun, BucketAndType, Key, ModifyOptions),
    {reply, Result, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

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
