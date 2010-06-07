%% -------------------------------------------------------------------
%%
%% riakc_pb_socket: protocol buffer client
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riakc_pb_socket).
-include_lib("kernel/include/inet.hrl").
-include("riakclient_pb.hrl").

-behaviour(gen_server).

-export([start_link/2,
         start/2,
         ping/1,
         get_client_id/1,
         set_client_id/2,
         get_server_info/1,
         get/3, get/4,
         put/2, put/3,
         delete/3, delete/4,
         list_buckets/1,
         list_keys/2,
         stream_list_keys/2,
         get_bucket/2,
         set_bucket/3,
         mapred/3, mapred/4,
         mapred_stream/4, mapred_stream/5,
         mapred_bucket/3, mapred_bucket/4,
         mapred_bucket_stream/5]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {address, port, sock, hello, req, ctx, from, queue}).

-define(PROTO_MAJOR, 1).
-define(PROTO_MINOR, 0).
-define(DEFAULT_TIMEOUT, 60000).

-type address() :: string() | atom() | ip_address().
-type portnum() :: non_neg_integer().
-type client_id() :: binary().
-type bucket() :: binary().
-type key() :: binary().
-type riakc_obj() :: tuple().
-type riak_pbc_options() :: list().
-type req_id() :: non_neg_integer().
-type rpb_req() :: tuple().
-type ctx() :: any().
-type rpb_resp() :: tuple().
-type server_prop() :: {node, binary()} | {server_version, binary()}.
-type server_info() :: [server_prop()].
-type bucket_prop() :: {n_val, pos_integer()} | {allow_mult, boolean()}.
-type bucket_props() :: [bucket_prop()].

%% @doc Create a linked process to talk with the riak server on Address:Port
%%      Client id will be assigned by the server.
-spec start_link(address(), portnum()) -> {ok, pid()} | {error, term()}.
start_link(Address, Port) ->
    gen_server:start_link(?MODULE, [Address, Port], []).

%% @doc Create a process to talk with the riak server on Address:Port
%%      Client id will be assigned by the server.
-spec start(address(), portnum()) -> {ok, pid()} | {error, term()}.
start(Address, Port) ->
    gen_server:start(?MODULE, [Address, Port], []).

%% @doc Ping the server
-spec ping(pid()) -> ok | {error, term()}.
ping(Pid) ->
    gen_server:call(Pid, {req, rpbpingreq}).

%% @doc Get the client id for this connection
-spec get_client_id(pid()) -> {ok, client_id()} | {error, term()}.
get_client_id(Pid) ->
    gen_server:call(Pid, {req, rpbgetclientidreq}).

%% @doc Set the client id for this connection
-spec set_client_id(pid(), client_id()) -> {ok, client_id()} | {error, term()}.
set_client_id(Pid, ClientId) ->
    gen_server:call(Pid, {req, #rpbsetclientidreq{client_id = ClientId}}).

%% @doc Get the server information for this connection
-spec get_server_info(pid()) -> {ok, server_info()} | {error, term()}.
get_server_info(Pid) ->
    gen_server:call(Pid, {req, rpbgetserverinforeq}).

%% @doc Get bucket/key from the server
%%      Will return {error, notfound} if the key is not on the server
-spec get(pid(), bucket() | string(), key() | string()) -> {ok, riakc_obj()} | {error, term()}.
get(Pid, Bucket, Key) ->
    get(Pid, Bucket, Key, []).

%% @doc Get bucket/key from the server supplying options
%%      [{r, 1}] would set r=1 for the request
-spec get(pid(), bucket() | string(), key() | string(), riak_pbc_options()) ->
                 {ok, riakc_obj()} | {error, term()}.
get(Pid, Bucket, Key, Options) ->
    Req = get_options(Options, #rpbgetreq{bucket = Bucket, key = Key}),
    gen_server:call(Pid, {req, Req}).

%% @doc Put the metadata/value in the object under bucket/key
-spec put(pid(), riakc_obj()) -> ok | {ok, riakc_obj()} | {error, term()}.
put(Pid, Obj) ->
    put(Pid, Obj, []).

%% @doc Put the metadata/value in the object under bucket/key with options
%%      [{w,2}] sets w=2,
%%      [{dw,1}] set dw=1,
%%      [{return_body, true}] returns the updated metadata/value
-spec put(pid(), riakc_obj(), riak_pbc_options()) -> ok | {ok, riakc_obj()} | {error, term()}.
put(Pid, Obj, Options) ->
    Content = riakc_pb:pbify_rpbcontent({riakc_obj:get_update_metadata(Obj),
                                         riakc_obj:get_update_value(Obj)}),
    Req = put_options(Options,
                      #rpbputreq{bucket = riakc_obj:bucket(Obj),
                                 key = riakc_obj:key(Obj),
                                 vclock = riakc_obj:vclock(Obj),
                                 content = Content}),
    gen_server:call(Pid, {req, Req}).

%% @doc Delete the key/value
-spec delete(pid(), bucket() | string(), key() | string()) -> ok | {error, term()}.
delete(Pid, Bucket, Key) ->
    delete(Pid, Bucket, Key, []).

%% @doc Delete the key/value with options
%%      [{rw,2}] sets rw=2
-spec delete(pid(), bucket() | string(), key() | string(), riak_pbc_options()) ->
                    ok | {error, term()}.
delete(Pid, Bucket, Key, Options) ->
    Req = delete_options(Options, #rpbdelreq{bucket = Bucket, key = Key}),
    gen_server:call(Pid, {req, Req}).

%% @doc List all buckets on the server
-spec list_buckets(pid()) -> {ok, [bucket()]} | {error, term()}.
list_buckets(Pid) ->
    gen_server:call(Pid, {req, rpblistbucketsreq}).

%% @doc List all keys in a bucket
-spec list_keys(pid(), bucket()) -> {ok, [key()]}.
list_keys(Pid, Bucket) ->
    {ok, ReqId} = stream_list_keys(Pid, Bucket),
    wait_for_listkeys(ReqId, ?DEFAULT_TIMEOUT).

%% @doc Stream list of keys in the bucket to the calling process.  The
%%      process receives these messages.
%%        {ReqId, {keys, [key()]}}
%%        {ReqId, done}
-spec stream_list_keys(pid(), bucket()) -> {ok, req_id()} | {error, term()}.
stream_list_keys(Pid, Bucket) ->
    ReqMsg = #rpblistkeysreq{bucket = Bucket},
    ReqId = mk_reqid(),
    gen_server:call(Pid, {req, ReqMsg, {ReqId, self()}}).

%% @doc Get bucket properties
-spec get_bucket(pid(), bucket()) -> {ok, bucket_props()} | {error, term()}.
get_bucket(Pid, Bucket) ->
    Req = #rpbgetbucketreq{bucket = Bucket},
    gen_server:call(Pid, {req, Req}).

%% @doc Set bucket properties
-spec set_bucket(pid(), bucket(), bucket_props()) -> ok | {error, term()}.
set_bucket(Pid, Bucket, BucketProps) ->
    PbProps = riakc_pb:pbify_rpbbucketprops(BucketProps),
    Req = #rpbsetbucketreq{bucket = Bucket, props = PbProps},
    gen_server:call(Pid, {req, Req}).

%% @spec mapred(Pid :: pid(),
%%              Inputs :: list(),
%%              Query :: [riak_kv_mapred_query:mapred_queryterm()]) ->
%%       {ok, riak_kv_mapred_query:mapred_result()} |
%%       {error, {bad_qterm, riak_kv_mapred_query:mapred_queryterm()}} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Perform a map/reduce job across the cluster.
%%      See the map/reduce documentation for explanation of behavior.
%% @equiv mapred(Inputs, Query, default_timeout())
mapred(Pid, Inputs, Query) ->
    mapred(Pid, Inputs, Query, ?DEFAULT_TIMEOUT).

%% @spec mapred(Pid :: pid(),
%%              Inputs :: list(),
%%              Query :: [riak_kv_mapred_query:mapred_queryterm()],
%%              TimeoutMillisecs :: integer()  | 'infinity') ->
%%       {ok, riak_kv_mapred_query:mapred_result()} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Perform a map/reduce job across the cluster.
%%      See the map/reduce documentation for explanation of behavior.
mapred(Pid, Inputs, Query, Timeout) ->
    {ok, ReqId} = mapred_stream(Pid, Inputs, Query, self(), Timeout),
    wait_for_mapred(ReqId, Timeout).

%% @spec mapred_stream(Pid :: pid(),
%%                     Inputs :: list(),
%%                     Query :: [riak_kv_mapred_query:mapred_queryterm()],
%%                     ClientPid :: pid()) ->
%%       {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
%%       {error, Err :: term()}
%% @doc Perform a streaming map/reduce job across the cluster sending results
%%      to ClientPid.
%%      See the map/reduce documentation for explanation of behavior.
mapred_stream(Pid, Inputs, Query, ClientPid) ->
    mapred_stream(Pid, Inputs, Query, ClientPid,?DEFAULT_TIMEOUT).

%% @spec mapred_stream(Pid :: pid(),
%%                     Inputs :: list(),
%%                     Query :: [riak_kv_mapred_query:mapred_queryterm()],
%%                     ClientPid :: pid(),
%%                     TimeoutMillisecs :: integer() | 'infinity') ->
%%       {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
%%       {error, Err :: term()}
%% @doc Perform a streaming map/reduce job with a timeout across the cluster.
%%      sending results to ClientPid.
%%      See the map/reduce documentation for explanation of behavior.
mapred_stream(Pid, Inputs, Query, ClientPid, Timeout) ->
    MapRed = [{'inputs', Inputs},
              {'query', Query},
              {'timeout', Timeout}],
    send_mapred_req(Pid, MapRed, ClientPid).

%% @spec mapred_bucket(Pid :: pid(),
%%                     Bucket :: bucket(),
%%                     Query :: [riak_kv_mapred_query:mapred_queryterm()]) ->
%%       {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
%%       {error, Err :: term()}
%% @doc Perform a map/reduce job against a bucket across the cluster.
%%      See the map/reduce documentation for explanation of behavior.
mapred_bucket(Pid, Bucket, Query) ->
    mapred_bucket(Pid, Bucket, Query, ?DEFAULT_TIMEOUT).

%% @spec mapred_bucket(Pid :: pid(),
%%                     Bucket :: bucket(),
%%                     Query :: [riak_kv_mapred_query:mapred_queryterm()],
%%                     TimeoutMillisecs :: integer() | 'infinity') ->
%%       {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
%%       {error, Err :: term()}
%% @doc Perform a map/reduce job against a bucket with a timeout
%%      across the cluster.
%%      See the map/reduce documentation for explanation of behavior.
mapred_bucket(Pid, Bucket, Query, Timeout) ->
    {ok, ReqId} = mapred_bucket_stream(Pid, Bucket, Query, self(), Timeout),
    wait_for_mapred(ReqId, Timeout).

%% @spec mapred_bucket_stream(Pid :: pid(),
%%                            Bucket :: bucket(),
%%                            Query :: [riak_kv_mapred_query:mapred_queryterm()],
%%                            ClientPid :: pid(),
%%                            TimeoutMillisecs :: integer() | 'infinity') ->
%%       {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
%%       {error, Err :: term()}
%% @doc Perform a streaming map/reduce job against a bucket with a timeout
%%      across the cluster.
%%      See the map/reduce documentation for explanation of behavior.
mapred_bucket_stream(Pid, Bucket, Query, ClientPid, Timeout) ->
    MapRed = [{'inputs', Bucket},
              {'query', Query},
              {'timeout', Timeout}],
    send_mapred_req(Pid, MapRed, ClientPid).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

%% @private
init([Address, Port]) ->
    case gen_tcp:connect(Address, Port,
                         [binary, {active, once}, {packet, 4}, {header, 1}]) of
        {ok, Sock} ->
            {ok, #state{address = Address, port = Port, sock = Sock, queue = queue:new()}};
        {error, Reason} ->
            {stop, Reason}
    end.

%% @private
handle_call({req, Req}, From, State) when State#state.req =/= undefined ->
    {noreply, queue_request(Req, undefined, From, State)};
handle_call({req, Req, Ctx}, From, State) when State#state.req =/= undefined ->
    {noreply, queue_request(Req, Ctx, From, State)};
handle_call({req, Req}, From, State) ->
    {noreply, send_request(Req, undefined, From, State)};
handle_call({req, Req, Ctx}, From, State) ->
    {noreply, send_request(Req, Ctx, From, State)}.

%% @private
handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};
handle_info({tcp, _Socket, Data}, State=#state{sock=Sock, req = Req, ctx = Ctx, from = From}) ->
    [MsgCode|MsgData] = Data,
    Resp = riakc_pb:decode(MsgCode, MsgData),
    case Resp of
        #rpberrorresp{} ->
            on_error(Req, Ctx, Resp, From),
            NewState = dequeue_request(State#state{req = undefined, from = undefined});

        _ ->
            case process_response(Req, Ctx, Resp, State) of
                {reply, Response, NewState0} ->
                    %% Send reply and get ready for the next request - send the next one if it's queued up
                    gen_server:reply(From, Response),
                    NewState = dequeue_request(NewState0#state{req = undefined, from = undefined});

                {noreply, NewState0} ->
                    %% Request has completed with no reply needed, send the next next one if queued up
                    NewState = dequeue_request(NewState0#state{req = undefined, from = undefined});

                {pending, NewState} ->
                    ok %% Request is still pending - do not queue up a new one
            end
    end,
    inet:setopts(Sock, [{active, once}]),
    {noreply, NewState};

handle_info(_, State) ->
    {noreply, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) -> ok.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ====================================================================
%% internal functions
%% ====================================================================

get_options([], Req) ->
    Req;
get_options([{r, R} | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{r = R}).

put_options([], Req) ->
    Req;
put_options([{w, W} | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{w = W});
put_options([{dw, DW} | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{dw = DW});
put_options([return_body | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{return_body = 1}).

delete_options([], Req) ->
    Req;
delete_options([{rw, RW} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{rw = RW}).


%% Process response from the server - passes back in the request and
%% context the request was issued with.
%% Return noreply if the request is completed, but no reply needed
%%        reply if the request is completed with a reply to the caller
%%        pending if the request has not completed yet (streaming op)
%% @private
-spec process_response(rpb_req(), ctx(), rpb_resp(), #state{}) ->
                              {noreply, #state{}} |
                              {reply, term(), #state{}} |
                              {pending, #state{}}.
process_response(rpbpingreq, _Ctx, rpbpingresp, State) ->
    {reply, pong, State};
process_response(rpbgetclientidreq, undefined,
                 #rpbgetclientidresp{client_id = ClientId}, State) ->
    {reply, {ok, ClientId}, State};
process_response(#rpbsetclientidreq{}, undefined,
                 rpbsetclientidresp, State) ->
    {reply, ok, State};
process_response(rpbgetserverinforeq, undefined,
                 #rpbgetserverinforesp{node = Node, server_version = ServerVersion}, State) ->
    case Node of
        undefined ->
            NodeInfo = [];
        Node ->
            NodeInfo = [{node, Node}]
    end,
    case ServerVersion of
        undefined ->
            VersionInfo = [];
        ServerVersion ->
            VersionInfo = [{server_version, ServerVersion}]
    end,
    {reply, {ok, NodeInfo++VersionInfo}, State};
process_response(#rpbgetreq{}, undefined, rpbgetresp, State) ->
    %% server just returned the rpbgetresp code - no message was encoded
    {reply, {error, notfound}, State};
process_response(#rpbgetreq{bucket = Bucket, key = Key}, _Ctx,
                 #rpbgetresp{content = RpbContents, vclock = Vclock}, State) ->
    Contents = riakc_pb:erlify_rpbcontents(RpbContents),
    {reply, {ok, riakc_obj:new_obj(Bucket, Key, Vclock, Contents)}, State};

process_response(#rpbputreq{}, undefined, rpbputresp, State) ->
    %% server just returned the rpbputresp code - no message was encoded
    {reply, ok, State};
process_response(#rpbputreq{bucket = Bucket, key = Key}, _Ctx,
                 #rpbputresp{contents = RpbContents, vclock = Vclock}, State) ->
    Contents = riakc_pb:erlify_rpbcontents(RpbContents),
    {reply, {ok, riakc_obj:new_obj(Bucket, Key, Vclock, Contents)}, State};

process_response(#rpbdelreq{}, undefined, rpbdelresp, State) ->
    %% server just returned the rpbdelresp code - no message was encoded
    {reply, ok, State};

process_response(rpblistbucketsreq, undefined,
                 #rpblistbucketsresp{buckets = Buckets}, State) ->
    {reply, {ok, Buckets}, State};

process_response(#rpblistkeysreq{}, {ReqId, Client},
                 #rpblistkeysresp{done = Done, keys = Keys}, State) ->
    case Keys of
        undefined ->
            ok;
        _ ->
            Client ! {ReqId, {keys, Keys}}
    end,
    case Done of
        1 ->
            Client ! {ReqId, done},
            {noreply, State};
        _ ->
            {pending, State}
    end;

process_response(#rpbgetbucketreq{}, undefined,
                 #rpbgetbucketresp{props = PbProps}, State) ->
    Props = riakc_pb:erlify_rpbbucketprops(PbProps),
    {reply, {ok, Props}, State};

process_response(#rpbsetbucketreq{}, undefined,
                 rpbsetbucketresp, State) ->
    {reply, ok, State};

process_response(#rpbmapredreq{content_type = ContentType}, {ReqId, Client},
                 #rpbmapredresp{done = Done, phase=PhaseId, response=Data}, State) ->
    case Data of
        undefined ->
            ok;
        _ ->
            Response = decode_mapred_resp(Data, ContentType),
            Client ! {ReqId, {mapred, PhaseId, Response}}
    end,
    case Done of
        1 ->
            Client ! {ReqId, done},
            {noreply, State};
        _ ->
            {pending, State}
    end.

%%
%% Called after sending a message - supports returning a
%% request id for streaming calls
%% @private
after_send(#rpblistkeysreq{}, {ReqId, _Client}, State) ->
    {reply, {ok, ReqId}, State};
after_send(rpblistbucketsreq, {ReqId, _Client}, State) ->
    {reply, {ok, ReqId}, State};
after_send(#rpbmapredreq{}, {ReqId, _Client}, State) ->
    {reply, {ok, ReqId}, State};
after_send(_Req, _Ctx, State) ->
    {noreply, State}.

%%
%% Called after receiving an error message - supports retruning
%% an error for streamign calls
%% @private
on_error(#rpblistkeysreq{}, {ReqId, Client},  ErrMsg, undefined) ->
    Client ! { ReqId, fmt_err_msg(ErrMsg)};
on_error(rpblistbucketsreq, {ReqId, Client}, ErrMsg, undefined) ->
    Client ! { ReqId, fmt_err_msg(ErrMsg)};
on_error(#rpbmapredreq{}, {ReqId, Client}, ErrMsg, undefined) ->
    Client ! { ReqId, fmt_err_msg(ErrMsg)};
on_error(_Req, _Ctx, ErrMsg, From) when From =/= undefined ->
    gen_server:reply(From, fmt_err_msg(ErrMsg)).

%% Format the PB encoded error message
fmt_err_msg(ErrMsg) ->
    case ErrMsg#rpberrorresp.errcode of
        Code when Code =:= 1; Code =:= undefined ->
            {error, ErrMsg#rpberrorresp.errmsg};
        Code ->
            {error, {Code, ErrMsg#rpberrorresp.errmsg}}
    end.

%% deliberately crash if the handling an error response after
%% the client has been replied to

%% Common code for sending a single bucket or multiple inputs map/request
%% @private
send_mapred_req(Pid, MapRed, ClientPid) ->
    ReqMsg = #rpbmapredreq{request = encode_mapred_req(MapRed),
                           content_type = <<"application/x-erlang-binary">>},
    ReqId = mk_reqid(),
    gen_server:call(Pid, {req, ReqMsg, {ReqId, ClientPid}}).

%% Send a request to the server and prepare the state for the response
%% @private
send_request(Req, Ctx, From, State) ->
    Pkt = riakc_pb:encode(Req),
    gen_tcp:send(State#state.sock, Pkt),
    case after_send(Req, Ctx, State) of
        {reply, Response, NewState} ->
            %% Respond after send - process_response will use an alternate mechanism
            %% to send additional information (callback or message)
            gen_server:reply(From, Response),
            NewState#state{req = Req, ctx = Ctx, from = undefined};
        {noreply, NewState} ->
            NewState#state{req = Req, ctx = Ctx, from = From}
    end.

%% Queue up a request if one is pending
%% @private
queue_request(Req, Ctx, From, State) ->
    State#state{queue = queue:in({Req, Ctx, From}, State#state.queue)}.

%% Try and dequeue request and send onto the server if one is waiting
%% @private
dequeue_request(State) ->
    case queue:out(State#state.queue) of
        {empty, _} ->
            State;
        {{value, {Req, Ctx, From}}, Q2} ->
            send_request(Req, Ctx, From, State#state{queue = Q2})
    end.

%% @private
mk_reqid() -> erlang:phash2(erlang:now()). % only has to be unique per-pid

%% @private
wait_for_listkeys(ReqId, Timeout) ->
    wait_for_listkeys(ReqId,Timeout,[]).
%% @private
wait_for_listkeys(ReqId,Timeout,Acc) ->
    receive
        {ReqId, done} -> {ok, lists:flatten(Acc)};
        {ReqId, {keys,Res}} -> wait_for_listkeys(ReqId,Timeout,[Res|Acc]);
        {ReqId, {error, Reason}} -> {error, Reason}
    after Timeout ->
            {error, {timeout, Acc}}
    end.


%% @private
wait_for_mapred(ReqId, Timeout) ->
    wait_for_mapred(ReqId,Timeout,orddict:new()).
%% @private
wait_for_mapred(ReqId, Timeout, Acc) ->
    receive
        {ReqId, done} -> {ok, orddict:to_list(Acc)};
        {ReqId, {mapred,Phase,Res}} ->
            wait_for_listkeys(ReqId,Timeout,orddict:append_list(Phase,Res,Acc));
        {ReqId, {error, Reason}} -> {error, Reason}
    after Timeout ->
            {error, {timeout, orddict:to_list(Acc)}}
    end.


%% Encode the map/reduce request using term to binary
%% @private
encode_mapred_req(Req) ->
    term_to_binary(Req).

%% Decode a partial phase response
%% @private
decode_mapred_resp(Data, <<"application/x-erlang-binary">>) ->
    try
        binary_to_term(Data)
    catch
        _:Error -> % On error, merge in with the other results
            [{error, Error}]
    end.

%% ====================================================================
%% unit tests
%% ====================================================================

%% Tests disabled until they can be prevented from running when included
%% as a dependency.
%%
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_IP, {127,0,0,1}).
-define(TEST_PORT, 8087).
-define(TEST_RIAK_NODE, 'riak@127.0.0.1').
-define(TEST_EUNIT_NODE, 'eunit@127.0.0.1').
-define(TEST_COOKIE, 'riak').

reset_riak() ->
    ?assertEqual(ok, maybe_start_network()),

    %% Until there is a good way to empty the vnodes, require the
    %% test to run with ETS and kill the vnode master/sup to empty all the ETS tables
    %% and the ring manager to remove any bucket properties
    ok = rpc:call(?TEST_RIAK_NODE, application, set_env, [riak_kv, storage_backend, riak_kv_ets_backend]),

    ok = supervisor:terminate_child({riak_kv_sup, ?TEST_RIAK_NODE}, riak_kv_vnode_master),
    ok = supervisor:terminate_child({riak_kv_sup, ?TEST_RIAK_NODE}, riak_kv_vnode_sup),
    ok = supervisor:terminate_child({riak_core_sup, ?TEST_RIAK_NODE}, riak_core_ring_manager),

    {ok, _} = supervisor:restart_child({riak_core_sup, ?TEST_RIAK_NODE}, riak_core_ring_manager),
    {ok, _} = supervisor:restart_child({riak_kv_sup, ?TEST_RIAK_NODE}, riak_kv_vnode_sup),
    {ok, _} = supervisor:restart_child({riak_kv_sup, ?TEST_RIAK_NODE}, riak_kv_vnode_master).


pause_riak_pb_sockets() ->
    Children = supervisor:which_children({riak_kv_pb_socket_sup, ?TEST_RIAK_NODE}),
    Pids = [Pid || {_,Pid,_,_} <- Children],
    [rpc:call(?TEST_RIAK_NODE, sys, suspend, [Pid]) || Pid <- Pids].

resume_riak_pb_sockets() ->
    Children = supervisor:which_children({riak_kv_pb_socket_sup, ?TEST_RIAK_NODE}),
    Pids = [Pid || {_,Pid,_,_} <- Children],
    [rpc:call(?TEST_RIAK_NODE, sys, resume, [Pid]) || Pid <- Pids].

maybe_start_network() ->
    %% Try to spin up net_kernel
    os:cmd("epmd -daemon"),
    case net_kernel:start([?TEST_EUNIT_NODE]) of
        {ok, _} ->
            erlang:set_cookie(?TEST_RIAK_NODE, ?TEST_COOKIE),
            ok;
        {error, {already_started, _}} ->
            ok;
        X ->
            X
    end.

bad_connect_test() ->
    %% Start with an unlikely port number
    {error, econnrefused} = start({127,0,0,1}, 65535).


pb_socket_test_() ->
    {setup,
     fun() ->
             %% Grab the riakclient_pb.proto file
             code:add_pathz("../ebin"),
             ok = maybe_start_network()
     end,
     {generator,
     fun() ->
             case catch net_adm:ping(?TEST_RIAK_NODE) of
                 pong ->
                     live_node_tests();
                 _ ->
                     [] %% {skipped, need_live_server};
             end
     end}}.

live_node_tests() ->
    [{"ping",
      ?_test( begin
                  {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                  pong = ?MODULE:ping(Pid)
              end)},
     {"set client id",
      ?_test(
         begin
             {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
             {ok, <<OrigId:32>>} = ?MODULE:get_client_id(Pid),

             NewId = <<(OrigId+1):32>>,
             ok = ?MODULE:set_client_id(Pid, NewId),
             {ok, NewId} = ?MODULE:get_client_id(Pid)
         end)},

     {"version",
      ?_test(
         begin
             {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
             {ok, ServerInfo} = ?MODULE:get_server_info(Pid),
             [{node, _}, {server_version, _}] = lists:sort(ServerInfo)
         end)},

     {"get_should_read_put_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 O0 = riakc_obj:new(<<"b">>, <<"k">>),
                 O = riakc_obj:update_value(O0, <<"v">>),
                 {ok, PO} = ?MODULE:put(Pid, O, [return_body]),
                 {ok, GO} = ?MODULE:get(Pid, <<"b">>, <<"k">>),
                 ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
             end)},

     {"get should read put with options",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 O0 = riakc_obj:new(<<"b">>, <<"k">>),
                 O = riakc_obj:update_value(O0, <<"v">>),
                 {ok, PO} = ?MODULE:put(Pid, O, [{w, 1}, {dw, 1}, return_body]),
                 {ok, GO} = ?MODULE:get(Pid, <<"b">>, <<"k">>, [{r, 1}]),
                 ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
             end)},

     {"update_should_change_value_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 O0 = riakc_obj:new(<<"b">>, <<"k">>),
                 O = riakc_obj:update_value(O0, <<"v">>),
                 {ok, PO} = ?MODULE:put(Pid, O, [return_body]),
                 PO2 = riakc_obj:update_value(PO, <<"v2">>),
                 ok = ?MODULE:put(Pid, PO2),
                 {ok, GO} = ?MODULE:get(Pid, <<"b">>, <<"k">>),
                 ?assertEqual(<<"v2">>, riakc_obj:get_value(GO))
             end)},

     {"key_should_be_missing_after_delete_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 %% Put key/value
                 O0 = riakc_obj:new(<<"b">>, <<"k">>),
                 O = riakc_obj:update_value(O0, <<"v">>),
                 {ok, _PO} = ?MODULE:put(Pid, O, [return_body]),
                 %% Prove it really got stored
                 {ok, GO1} = ?MODULE:get(Pid, <<"b">>, <<"k">>),
                 ?assertEqual(<<"v">>, riakc_obj:get_value(GO1)),
                 %% Delete and check no longer found
                 ok = ?MODULE:delete(Pid, <<"b">>, <<"k">>),
                 {error, notfound} = ?MODULE:get(Pid, <<"b">>, <<"k">>)
             end)},

    {"delete missing key test",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                  %% Delete and check no longer found
                 ok = ?MODULE:delete(Pid, <<"notabucket">>, <<"k">>, [{rw, 1}]),
                 {error, notfound} = ?MODULE:get(Pid, <<"notabucket">>, <<"k">>)
             end)},

     {"list_buckets_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 Bs = lists:sort([list_to_binary(["b"] ++ integer_to_list(N)) || N <- lists:seq(1, 10)]),
                 F = fun(B) ->
                             O=riakc_obj:new(B, <<"key">>),
                             ?MODULE:put(Pid, riakc_obj:update_value(O, <<"val">>))
                     end,
                 [F(B) || B <- Bs],
                 {ok, LBs} = ?MODULE:list_buckets(Pid),
                 ?assertEqual(Bs, lists:sort(LBs))
             end)},

     {"list_keys_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 Bucket = <<"listkeys">>,
                 Ks = lists:sort([list_to_binary(integer_to_list(N)) || N <- lists:seq(1, 10)]),
                 F = fun(K) ->
                             O=riakc_obj:new(Bucket, K),
                             ?MODULE:put(Pid, riakc_obj:update_value(O, <<"val">>))
                     end,
                 [F(K) || K <- Ks],
                 {ok, LKs} = ?MODULE:list_keys(Pid, Bucket),
                 ?assertEqual(Ks, lists:sort(LKs))
             end)},

     {"get bucket properties test",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 {ok, Props} = get_bucket(Pid, <<"b">>),
                 ?assertEqual([{allow_mult,false},
                               {n_val,3}],
                              lists:sort(Props))
             end)},

     {"get bucket properties test",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 ok = set_bucket(Pid, <<"b">>, [{n_val, 2}, {allow_mult, true}]),
                 {ok, Props} = get_bucket(Pid, <<"b">>),
                 ?assertEqual([{allow_mult,true},
                               {n_val,2}],
                              lists:sort(Props))
             end)},

     {"allow_mult should allow dupes",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid1} = start_link(?TEST_IP, ?TEST_PORT),
                 {ok, Pid2} = start_link(?TEST_IP, ?TEST_PORT),
                 ok = set_bucket(Pid1, <<"multibucket">>, [{allow_mult, true}]),
                 ?MODULE:delete(Pid1, <<"multibucket">>, <<"foo">>),
                 {error, notfound} = ?MODULE:get(Pid1, <<"multibucket">>, <<"foo">>),
                 O = riakc_obj:new(<<"multibucket">>, <<"foo">>),
                 O1 = riakc_obj:update_value(O, <<"pid1">>),
                 O2 = riakc_obj:update_value(O, <<"pid2">>),
                 ok = ?MODULE:put(Pid1, O1),

                 ok = ?MODULE:put(Pid2, O2),
                 {ok, O3} = ?MODULE:get(Pid1, <<"multibucket">>, <<"foo">>),
                 ?assertEqual([<<"pid1">>, <<"pid2">>], lists:sort(riakc_obj:get_values(O3))),
                 O4 = riakc_obj:update_value(O3, <<"resolved">>),
                 ok = ?MODULE:put(Pid1, O4),
                 {ok, GO} = ?MODULE:get(Pid1, <<"multibucket">>, <<"foo">>),
                 ?assertEqual([<<"resolved">>], lists:sort(riakc_obj:get_values(GO))),
                 ?MODULE:delete(Pid1, <<"multibucket">>, <<"foo">>)
             end)},

     {"queue test",
      ?_test(begin
                 %% Would really like this in a nested {setup, blah} structure
                 %% but eunit does not allow
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 pause_riak_pb_sockets(),
                 Me = self(),
                 %% this request will block as
                 spawn(fun() -> Me ! {1, ping(Pid)} end),
                 %% this request should be queued as socket will not be created
                 spawn(fun() -> Me ! {2, ping(Pid)} end),
                 resume_riak_pb_sockets(),
                 receive {1,Ping1} -> ?assertEqual(Ping1, pong) end,
                 receive {2,Ping2} -> ?assertEqual(Ping2, pong) end
             end)},

     {"javascript_source_map_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 B = <<"bucket">>,
                 K = <<"foo">>,
                 O=riakc_obj:new(B, K),
                 ?MODULE:put(Pid, riakc_obj:update_value(O, <<"2">>, "application/json")),

                 ?assertEqual({ok, [{0, [2]}]},
                              ?MODULE:mapred(Pid,
                                             [{B, K}],
                                             [{map, {jsanon, <<"function (v) { return [JSON.parse(v.values[0].data)]; }">>},
                                               undefined, true}]))
             end)},

     {"javascript_named_map_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 B = <<"bucket">>,
                 K = <<"foo">>,
                 O=riakc_obj:new(B, K),
                 ?MODULE:put(Pid, riakc_obj:update_value(O, <<"99">>, "application/json")),

                 ?assertEqual({ok, [{0, [99]}]},
                              ?MODULE:mapred(Pid,
                                             [{B, K}],
                                             [{map, {jsfun, <<"Riak.mapValuesJson">>},
                                               undefined, true}]))
             end)},

     {"javascript_source_map_reduce_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 Store = fun({K,V}) ->
                                 O=riakc_obj:new(<<"bucket">>, K),
                                 ?MODULE:put(Pid,riakc_obj:update_value(O, V, "application/json"))
                         end,
                 [Store(KV) || KV <- [{<<"foo">>, <<"2">>},
                                      {<<"bar">>, <<"3">>},
                                      {<<"baz">>, <<"4">>}]],

                 ?assertEqual({ok, [{1, 3}]},
                              ?MODULE:mapred(Pid,
                                             [{<<"bucket">>, <<"foo">>},
                                              {<<"bucket">>, <<"bar">>},
                                              {<<"bucket">>, <<"baz">>}],
                                             [{map, {jsanon, <<"function (v) { return [1]; }">>},
                                               undefined, false},
                                              {reduce, {jsanon, <<"function(v) { return v.length; } ">>},
                                               undefined, true}]))
             end)},

     {"javascript_named_map_reduce_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 Store = fun({K,V}) ->
                                 O=riakc_obj:new(<<"bucket">>, K),
                                 ?MODULE:put(Pid,riakc_obj:update_value(O, V, "application/json"))
                         end,
                 [Store(KV) || KV <- [{<<"foo">>, <<"2">>},
                                      {<<"bar">>, <<"3">>},
                                      {<<"baz">>, <<"4">>}]],

                 ?assertEqual({ok, [{1, [9]}]},
                              ?MODULE:mapred(Pid,
                                             [{<<"bucket">>, <<"foo">>},
                                              {<<"bucket">>, <<"bar">>},
                                              {<<"bucket">>, <<"baz">>}],
                                             [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                              {reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}]))
             end)},

     {"javascript_bucket_map_reduce_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 Store = fun({K,V}) ->
                                 O=riakc_obj:new(<<"bucket">>, K),
                                 ?MODULE:put(Pid,riakc_obj:update_value(O, V, "application/json"))
                         end,
                 [Store(KV) || KV <- [{<<"foo">>, <<"2">>},
                                      {<<"bar">>, <<"3">>},
                                      {<<"baz">>, <<"4">>}]],

                 ?assertEqual({ok, [{1, [9]}]},
                              ?MODULE:mapred_bucket(Pid, <<"bucket">>,
                                                    [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                                     {reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}]))
             end)},

     {"javascript_arg_map_reduce_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 O=riakc_obj:new(<<"bucket">>, <<"foo">>),
                 ?MODULE:put(Pid, riakc_obj:update_value(O, <<"2">>, "application/json")),
                 ?assertEqual({ok, [{1, [10]}]},
                              ?MODULE:mapred(Pid,
                                             [{{<<"bucket">>, <<"foo">>}, 5},
                                              {{<<"bucket">>, <<"foo">>}, 10},
                                              {{<<"bucket">>, <<"foo">>}, 15},
                                              {{<<"bucket">>, <<"foo">>}, -15},
                                              {{<<"bucket">>, <<"foo">>}, -5}],
                                             [{map, {jsanon, <<"function(v, arg) { return [arg]; }">>},
                                               undefined, false},
                                              {reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}]))
             end)},

     {"erlang_map_reduce_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 Store = fun({K,V}) ->
                                 O=riakc_obj:new(<<"bucket">>, K),
                                 ?MODULE:put(Pid,riakc_obj:update_value(O, V, "application/json"))
                         end,
                 [Store(KV) || KV <- [{<<"foo">>, <<"2">>},
                                      {<<"bar">>, <<"3">>},
                                      {<<"baz">>, <<"4">>}]],

                 {ok, [{1, Results}]} = ?MODULE:mapred(Pid,
                                                       [{<<"bucket">>, <<"foo">>},
                                                        {<<"bucket">>, <<"bar">>},
                                                        {<<"bucket">>, <<"baz">>}],
                                                       [{map, {modfun, riak_kv_mapreduce,
                                                               map_object_value},
                                                         undefined, false},
                                                        {reduce, {modfun, riak_kv_mapreduce,
                                                                  reduce_set_union},
                                                         undefined, true}]),
                 ?assertEqual([<<"2">>, <<"3">>, <<"4">>], lists:sort(Results))
             end)},
     {"missing_key_erlang_map_reduce_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 {ok, Results} = ?MODULE:mapred(Pid, [{<<"bucket">>, <<"foo">>},
                                                       {<<"bucket">>, <<"bar">>},
                                                      {<<"bucket">>, <<"baz">>}],
                                                [{map, {modfun, riak_kv_mapreduce,
                                                        map_object_value},
                                                  undefined, false},
                                                 {reduce, {modfun, riak_kv_mapreduce,
                                                           reduce_set_union},
                                                  undefined, true}]),
                 [{1, [{not_found, {_, _}}|_]}] = Results end)},
     {"missing_key_javascript_map_reduce_test()",
      ?_test(begin
                 reset_riak(),
                 {ok, Pid} = {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 {ok, Results} = ?MODULE:mapred(Pid, [{<<"bucket">>, <<"foo">>},
                                                       {<<"bucket">>, <<"bar">>},
                                                      {<<"bucket">>, <<"baz">>}],
                                                [{map, {jsfun, <<"Riak.mapValuesJson">>},
                                                  undefined, false},
                                                 {reduce, {jsfun, <<"Riak.reduceSort">>},
                                                  undefined, true}]),
                 [{1, [{not_found, {_, _}}|_]}] = Results end)},
     {"map reduce bad inputs",
      ?_test(begin
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 ?assertEqual({error, <<"{inputs,{\"Inputs must be a binary bucket or a list of target tuples:\",\n         undefined}}">>},
                              ?MODULE:mapred(Pid, undefined,
                                             [{map, {jsfun, <<"Riak.mapValuesJson">>},
                                               undefined, false},
                                              {reduce, {jsfun, <<"Riak.reduceSum">>},
                                               undefined, true}]))
             end)},
     {"map reduce bad input keys",
      ?_test(begin
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 Res = ?MODULE:mapred(Pid, [<<"b">>], % no {B,K} tuple
                                      [{map, {jsfun, <<"Riak.mapValuesJson">>},
                                        undefined, false},
                                       {reduce, {jsfun, <<"Riak.reduceSum">>},
                                        undefined, true}]),
                 ?assertEqual({error,<<"{inputs,{\"Inputs target tuples must be {B,K} or {{B,K},KeyData}:\",[<<\"b\">>]}}">>},
                              Res)
             end)},
     {"map reduce bad query",
      ?_test(begin
                 {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
                 Res = ?MODULE:mapred(Pid, [{<<"b">>,<<"k">>}], % no {B,K} tuple
                                      undefined),
                 ?assertEqual({error,<<"{'query',{\"Query takes a list of step tuples\",undefined}}">>},
                              Res)
             end)}

     ].

-endif.
