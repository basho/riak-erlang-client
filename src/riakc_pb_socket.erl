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
-include("riakclient_pb.hrl").

-behaviour(gen_server).

-export([start_link/2, start_link/3,
         start/2, start/3,
         ping/1,
         get/3, get/4,
         put/2, put/3,
         delete/3, delete/4,
         get_bucket_props/2,
         set_bucket_props/3,
         list_buckets/1,
         list_keys/2,
         stream_list_keys/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {ip, port, sock, hello, req, ctx, from, queue}).

-define(PROTO_MAJOR, 1).
-define(PROTO_MINOR, 0).
-define(DEFAULT_TIMEOUT, 60000).

-type bucket() :: binary().
-type key() :: binary().
-type errstr() :: {error, binary()}.
-type riakc_obj() :: tuple().
-type riak_pbc_options() :: list().
-type riak_pbc_props() :: [{binary(),term()}].

start_link(Ip, Port) ->
    gen_server:start_link(?MODULE, [Ip, Port], []).

start_link(Ip, Port, ClientId) ->
    gen_server:start_link(?MODULE, [Ip, Port, ClientId], []).

start(Ip, Port) ->
    gen_server:start(?MODULE, [Ip, Port], []).

start(Ip, Port, ClientId) ->
    gen_server:start(?MODULE, [Ip, Port, ClientId], []).

ping(Pid) ->
    gen_server:call(Pid, {req, rpbpingreq}).

-spec get(pid(), bucket() | string(), key() | string()) -> {ok, riakc_obj()} | errstr().
get(Pid, Bucket, Key) ->
    get(Pid, Bucket, Key, []).

-spec get(pid(), bucket() | string(), key() | string(), riak_pbc_options()) -> 
                 {ok, riakc_obj()} | errstr().
get(Pid, Bucket, Key, Options) ->
    Req = #rpbgetreq{bucket = Bucket, key = Key, options = riakc_pb:pbify_rpboptions(Options)},
    gen_server:call(Pid, {req, Req}).

-spec put(pid(), riakc_obj()) -> ok | {ok, riakc_obj()} | errstr().
put(Pid, Obj) ->
    put(Pid, Obj, []).

-spec put(pid(), riakc_obj(), riak_pbc_options()) -> ok | {ok, riakc_obj()} | errstr().
put(Pid, Obj, Options) ->
    Req = #rpbputreq{bucket = riakc_obj:bucket(Obj), 
                     key = riakc_obj:key(Obj),
                     vclock = riakc_obj:vclock(Obj),
                     content = riakc_pb:pbify_rpbcontent({riakc_obj:get_update_metadata(Obj),
                                                          riakc_obj:get_update_value(Obj)}),
                     options = riakc_pb:pbify_rpboptions(Options)},
    gen_server:call(Pid, {req, Req}).
 
-spec delete(pid(), bucket() | string(), key() | string()) -> ok | errstr().
delete(Pid, Bucket, Key) ->
    delete(Pid, Bucket, Key, []).

-spec delete(pid(), bucket() | string(), key() | string(), riak_pbc_options()) -> ok | errstr().
delete(Pid, Bucket, Key, Options) ->
    Req = #rpbdelreq{bucket = Bucket, key = Key, options = riakc_pb:pbify_rpboptions(Options)},
    gen_server:call(Pid, {req, Req}).

-spec get_bucket_props(pid(), bucket() | string()) -> {ok, riak_pbc_props()} | errstr(). 
get_bucket_props(Pid, Bucket) ->
    Req = #rpbgetbucketpropsreq{bucket = Bucket},
    gen_server:call(Pid, {req, Req}).

-spec set_bucket_props(pid(), bucket() | string(), riak_pbc_props()) -> ok | errstr(). 
set_bucket_props(Pid, Bucket, Props) ->
    Req = #rpbsetbucketpropsreq{bucket = Bucket, properties = riakc_pb:pbify_bucket_props(Props)},
    gen_server:call(Pid, {req, Req}).

list_buckets(Pid) ->
    {ok, ReqId} = stream_list_buckets(Pid),
    wait_for_listbuckets(ReqId, ?DEFAULT_TIMEOUT).

stream_list_buckets(Pid) ->
    ReqMsg = rpblistbucketsreq,
    ReqId = mk_reqid(),
    gen_server:call(Pid, {req, ReqMsg, {ReqId, self()}}).

list_keys(Pid, Bucket) ->
    {ok, ReqId} = stream_list_keys(Pid, Bucket),
    wait_for_listkeys(ReqId, ?DEFAULT_TIMEOUT).

stream_list_keys(Pid, Bucket) ->
    ReqMsg = #rpblistkeysreq{bucket = Bucket},
    ReqId = mk_reqid(),
    gen_server:call(Pid, {req, ReqMsg, {ReqId, self()}}).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([Ip, Port]) ->
    init([Ip, Port, undefined]);
init([Ip, Port, ClientId]) ->
    case gen_tcp:connect(Ip, Port,
                         [binary, {active, once}, {packet, 4}, {header, 1}]) of
        {ok, Sock} ->
            Req = #rpbhelloreq{proto_major = 1, client_id = ClientId},
            {ok, send_request(Req, undefined, undefined,
                              #state{ip = Ip, port = Port,
                                     sock = Sock, queue = queue:new()})};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({req, Req}, From, State) when State#state.req =/= undefined ->
    {noreply, queue_request(Req, undefined, From, State)};
handle_call({req, Req, Ctx}, From, State) when State#state.req =/= undefined ->
    {noreply, queue_request(Req, Ctx, From, State)};
handle_call({req, Req}, From, State) ->
    {noreply, send_request(Req, undefined, From, State)};
handle_call({req, Req, Ctx}, From, State) ->
    {noreply, send_request(Req, Ctx, From, State)}.
 
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

handle_cast(_Msg, State) -> 
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ====================================================================
%% internal functions
%% ====================================================================

-type rpb_req() :: tuple().
-type ctx() :: any().
-type rpb_resp() :: tuple().
-spec process_response(rpb_req(), ctx(), rpb_resp(), #state{}) ->
                              {noreply, #state{}} | 
                              {reply, term(), #state{}} | 
                              {pending, #state{}}.

process_response(#rpbhelloreq{}, undefined, #rpbhelloresp{} = Resp, State) ->
    {noreply, State#state{hello = Resp}};
process_response(rpbpingreq, _Ctx, rpbpingresp, State) ->
    {reply, ok, State};

process_response(#rpbgetreq{}, undefined, rpbgetresp, State) ->
    %% server just returned the rpbgetresp code - no message was encoded
    {reply, {error, notfound}, State};
process_response(#rpbgetreq{bucket = Bucket, key = Key}, _Ctx, 
                 #rpbgetresp{content = RpbContents, vclock = Vclock}, State) ->
    Contents = riakc_pb:erlify_rpbcontents(RpbContents),
    {reply, {ok, riakc_obj:new(Bucket, Key, Vclock, Contents)}, State};

process_response(#rpbputreq{}, undefined, rpbputresp, State) ->
    %% server just returned the rpbputresp code - no message was encoded
    {reply, ok, State};
process_response(#rpbputreq{bucket = Bucket, key = Key}, _Ctx, 
                 #rpbputresp{contents = RpbContents, vclock = Vclock}, State) ->
    Contents = riakc_pb:erlify_rpbcontents(RpbContents),
    {reply, {ok, riakc_obj:new(Bucket, Key, Vclock, Contents)}, State};

process_response(#rpbdelreq{}, undefined, rpbdelresp, State) ->
    %% server just returned the rpbdelresp code - no message was encoded
    {reply, ok, State};

process_response(#rpbgetbucketpropsreq{}, undefined,
                 #rpbgetbucketpropsresp{properties = Props}, State) ->
    {reply, {ok, riakc_pb:erlify_bucket_props(Props)}, State};

process_response(#rpbsetbucketpropsreq{}, undefined, rpbsetbucketpropsresp, State) ->
    {reply, ok, State};

process_response(rpblistbucketsreq, {ReqId, Client},
                 #rpblistbucketsresp{done = Done, buckets = Buckets}, State) ->
    case Buckets of
        undefined ->
            ok;
        _ ->
            Client ! {ReqId, {buckets, Buckets}}
    end,
    case Done of
        1 ->
            Client ! {ReqId, done},
            {noreply, State};
        _ ->
            {pending, State}
    end;

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
    end.

after_send(#rpblistkeysreq{}, {ReqId, _Client}, State) ->
    {reply, {ok, ReqId}, State};
after_send(rpblistbucketsreq, {ReqId, _Client}, State) ->
    {reply, {ok, ReqId}, State};
after_send(_Req, _Ctx, State) ->
    {noreply, State}.

on_error(#rpblistkeysreq{}, ErrMsg, {ReqId, Client}, undefined) ->
    Client ! { ReqId, {error, ErrMsg}};
on_error(rpblistbucketsreq, ErrMsg, {ReqId, Client}, undefined) ->
    Client ! { ReqId, {error, ErrMsg}};
on_error(_Req, ErrMsg, _Ctx, From) when From =/= undefined ->
    gen_server:reply(From, {error, ErrMsg}).
%% deliberately crash if the handling an error response after
%% the client has been replied to

send_request(Req, Ctx, From, State) ->
    Pkt = riakc_pb:encode(Req),
    ok = gen_tcp:send(State#state.sock, Pkt),
    case after_send(Req, Ctx, State) of
        {reply, Response, NewState} ->
            %% Respond after send - process_response will use an alternate mechanism
            %% to send additional information (callback or message)
            gen_server:reply(From, Response),
            NewState#state{req = Req, ctx = Ctx, from = undefined};
        {noreply, NewState} ->
            NewState#state{req = Req, ctx = Ctx, from = From}
    end.

queue_request(Req, Ctx, From, State) ->
    State#state{queue = queue:in({Req, Ctx, From}, State#state.queue)}.

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
wait_for_listbuckets(ReqId, Timeout) ->
    wait_for_listbuckets(ReqId,Timeout,[]).
%% @private
wait_for_listbuckets(ReqId,Timeout,Acc) ->
    receive
        {ReqId, done} -> {ok, lists:flatten(Acc)};
        {ReqId, {buckets, Res}} -> wait_for_listbuckets(ReqId,Timeout,[Res|Acc])
    after Timeout ->
            {error, timeout, Acc}
    end.

%% @private
wait_for_listkeys(ReqId, Timeout) ->
    wait_for_listkeys(ReqId,Timeout,[]).
%% @private
wait_for_listkeys(ReqId,Timeout,Acc) ->
    receive
        {ReqId, done} -> {ok, lists:flatten(Acc)};
        {ReqId,{keys,Res}} -> wait_for_listkeys(ReqId,Timeout,[Res|Acc])
    after Timeout ->
            {error, timeout, Acc}
    end.

%% ====================================================================
%% unit tests
%% ====================================================================

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
    %% test to run with ETS and kill the vnode sup to empty all the ETS tables
    ok = rpc:call(?TEST_RIAK_NODE, application, set_env, [riak_kv, storage_backend, riak_kv_ets_backend]),
    ok = supervisor:terminate_child({riak_kv_sup, ?TEST_RIAK_NODE}, riak_kv_vnode_sup),
    {ok, _} = supervisor:restart_child({riak_kv_sup, ?TEST_RIAK_NODE}, riak_kv_vnode_sup).

maybe_start_network() ->
    %% Try to spin up net_kernel
    case net_kernel:start([?TEST_EUNIT_NODE]) of
        {ok, _} ->
            erlang:set_cookie(?TEST_RIAK_NODE, ?TEST_COOKIE),
            ok;
        {error, {already_started, _}} ->
            ok;
        X ->
            X
    end.

get_should_read_put_test() ->
    reset_riak(),
    {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
    O0 = riakc_obj:new(<<"b">>, <<"k">>),
    O = riakc_obj:update_value(O0, <<"v">>),
    {ok, PO} = ?MODULE:put(Pid, O, [return_body]),
    {ok, GO} = ?MODULE:get(Pid, <<"b">>, <<"k">>),
    ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO)).

update_should_change_value_test() ->
    reset_riak(),
    {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
    O0 = riakc_obj:new(<<"b">>, <<"k">>),
    O = riakc_obj:update_value(O0, <<"v">>),
    {ok, PO} = ?MODULE:put(Pid, O, [return_body]),
    PO2 = riakc_obj:update_value(PO, <<"v2">>),
    ok = ?MODULE:put(Pid, PO2),
    {ok, GO} = ?MODULE:get(Pid, <<"b">>, <<"k">>),
    ?assertEqual(<<"v2">>, riakc_obj:get_value(GO)).

key_should_be_missing_after_delete_test() ->
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
    {error, notfound} = ?MODULE:get(Pid, <<"b">>, <<"k">>).

allow_mult_should_allow_dupes_test() ->
    reset_riak(),
    {ok, Pid1} = start_link(?TEST_IP, ?TEST_PORT),
    {ok, Pid2} = start_link(?TEST_IP, ?TEST_PORT),
    ok = set_bucket_props(Pid1, <<"multibucket">>, [{allow_mult, true}]),
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
    ?MODULE:delete(Pid1, <<"multibucket">>, <<"foo">>).

list_buckets_test() ->
    reset_riak(),
    {ok, Pid} = start_link(?TEST_IP, ?TEST_PORT),
    Bs = lists:sort([list_to_binary(["b"] ++ integer_to_list(N)) || N <- lists:seq(1, 10)]),
    F = fun(B) ->
                O=riakc_obj:new(B, <<"key">>),
                ?MODULE:put(Pid, riakc_obj:update_value(O, <<"val">>))
        end,
    [F(B) || B <- Bs],
    {ok, LBs} = ?MODULE:list_buckets(Pid),
    ?assertEqual(Bs, lists:sort(LBs)).

    
list_keys_test() ->
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
    ?assertEqual(Ks, lists:sort(LKs)).
                         
       
                               
-endif.

