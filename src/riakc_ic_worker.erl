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

handle_call({?GET_REQUEST, Bucket, Key, Options, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get(RiakConn, Bucket, Key, Options, Timeout),
    {reply, Result, State};
handle_call({?PUT_REQUEST, Obj, Options, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:put(RiakConn, Obj, Options, Timeout),
    {reply, Result, State};
handle_call({?DELETE_REQUEST, Bucket, Key, Options, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:delete(RiakConn, Bucket, Key, Options, Timeout),
    {reply, Result, State};
handle_call({?DELETE_VCLOCK_REQUEST, Bucket, Key, Vclock, Options, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:delete_vclock(RiakConn, Bucket, Key, Vclock, Options, Timeout),
    {reply, Result, State};
handle_call({?DELETE_OBJ_REQUEST, Obj, Options, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:delete_obj(RiakConn, Obj, Options, Timeout),
    {reply, Result, State};
handle_call({?LIST_BUCKETS_REQUEST, Type, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:list_buckets(RiakConn, Type, Options),
    {reply, Result, State};
handle_call({?STREAM_LIST_BUCKETS_REQUEST, Type, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:stream_list_buckets(RiakConn, Type, Options),
    {reply, Result, State};
handle_call({?LEGACY_LIST_BUCKETS_REQUEST, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:legacy_list_buckets(RiakConn, Options),
    {reply, Result, State};
handle_call({?LIST_KEYS_REQUEST, Bucket, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:list_keys(RiakConn, Bucket, Options),
    {reply, Result, State};
handle_call({?STREAM_LIST_KEYS_REQUEST, Bucket, Options}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:stream_list_keys(RiakConn, Bucket, Options),
    {reply, Result, State};
handle_call({?GET_BUCKET_REQUEST, Bucket, Timeout, CallTimeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_bucket(RiakConn, Bucket, Timeout, CallTimeout),
    {reply, Result, State};
handle_call({?GET_BUCKET_TYPE_REQUEST, BucketType, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:get_bucket_type(RiakConn, BucketType, Timeout),
    {reply, Result, State};
handle_call({?SET_BUCKET_REQUEST, Bucket, BucketProps, Timeout, CallTimeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:set_bucket(RiakConn, Bucket, BucketProps, Timeout, CallTimeout),
    {reply, Result, State};
handle_call({?SET_BUCKET_TYPE_REQUEST, Bucket, BucketProps, Timeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:set_bucket_type(RiakConn, Bucket, BucketProps, Timeout),
    {reply, Result, State};
handle_call({?RESET_BUCKET_REQUEST, Bucket, Timeout, CallTimeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:reset_bucket(RiakConn, Bucket, Timeout, CallTimeout),
    {reply, Result, State};
handle_call({?MAPRED_REQUEST, Inputs, Query, Timeout, CallTimeout}, _From, State = #state{riak_conn = RiakConn}) ->
    Result = riakc_pb_socket:mapred(RiakConn, Inputs, Query, Timeout, CallTimeout),
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
