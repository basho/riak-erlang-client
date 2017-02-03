%% -------------------------------------------------------------------
%%
%% riakc_pb_socket_tests: protocol buffer client tests
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
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
-ifdef(TEST).

-module(riakc_pb_socket_tests).

-compile(export_all).

-include("riakc.hrl").

-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("eunit/include/eunit.hrl").

bad_connect_test() ->
    %% Start with an unlikely port number
    ?assertEqual({error, {tcp, econnrefused}}, riakc_pb_socket:start({127,0,0,1}, 65535)).

queue_disconnected_test() ->
    %% Start with an unlikely port number
    {ok, Pid} = riakc_pb_socket:start({127,0,0,1}, 65535, [queue_if_disconnected]),
    ?assertEqual({error, timeout}, riakc_pb_socket:ping(Pid, 10)),
    ?assertEqual({error, timeout}, riakc_pb_socket:list_keys(Pid, <<"b">>, 10)),
    riakc_pb_socket:stop(Pid).

auto_reconnect_bad_connect_test() ->
    %% Start with an unlikely port number
    {ok, Pid} = riakc_pb_socket:start({127,0,0,1}, 65535, [auto_reconnect]),
    ?assertEqual({false, []}, riakc_pb_socket:is_connected(Pid)),
    ?assertEqual({error, disconnected}, riakc_pb_socket:ping(Pid)),
    ?assertEqual({error, disconnected}, riakc_pb_socket:list_keys(Pid, <<"b">>)),
    riakc_pb_socket:stop(Pid).

server_closes_socket_test() ->
    %% Silence SASL junk when socket closes.
    error_logger:tty(false),
    %% Set up a dummy socket to send requests on
    {ok, Listen} = gen_tcp:listen(0, [binary, {packet, 4}, {active, false}]),
    {ok, Port} = inet:port(Listen),
    {ok, Pid} = riakc_pb_socket:start("127.0.0.1", Port),
    {ok, Sock} = gen_tcp:accept(Listen),
    ?assertMatch(true, riakc_pb_socket:is_connected(Pid)),

    %% Send a ping request in another process so the test doesn't block
    Self = self(),
    spawn(fun() -> Self ! riakc_pb_socket:ping(Pid, infinity) end),

    %% Make sure request received then close the socket
    {ok, _ReqMsg} = gen_tcp:recv(Sock, 0),
    ok = gen_tcp:close(Sock),
    ok = gen_tcp:close(Listen),
    receive
        Msg1 -> % result of ping from spawned process above
            ?assertEqual({error, disconnected}, Msg1)
    end,
    %% Wait for spawned process to exit
    Mref = erlang:monitor(process, Pid),
    receive
        Msg2 ->
            ?assertMatch({'DOWN', Mref, process, _, _}, Msg2)
    end.

auto_reconnect_server_closes_socket_test() ->
    %% Set up a dummy socket to send requests on
    {ok, Listen} = gen_tcp:listen(0, [binary, {packet, 4}, {active, false}]),
    {ok, Port} = inet:port(Listen),
    {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", Port, [auto_reconnect]),
    {ok, Sock} = gen_tcp:accept(Listen),
    ?assertMatch(true, riakc_pb_socket:is_connected(Pid)),

    %% Send a ping request in another process so the test doesn't block
    Self = self(),
    spawn(fun() -> Self ! riakc_pb_socket:ping(Pid, infinity) end),

    %% Make sure request received then close the socket
    {ok, _ReqMsg} = gen_tcp:recv(Sock, 0),
    ok = gen_tcp:close(Sock),
    ok = gen_tcp:close(Listen),
    receive
        Msg ->
            ?assertEqual({error, disconnected}, Msg)
    end,
    %% Server will not have had a chance to reconnect yet, reason counters empty.
    ?assertMatch({false, []}, riakc_pb_socket:is_connected(Pid)),
    riakc_pb_socket:stop(Pid).

dead_socket_pid_returns_to_caller_test() ->
    %% Set up a dummy socket to send requests on
    {ok, Listen} = gen_tcp:listen(0, [binary, {packet, 4}, {active, false}]),
    {ok, Port} = inet:port(Listen),
    {ok, Pid} = riakc_pb_socket:start("127.0.0.1", Port),
    {ok, Sock} = gen_tcp:accept(Listen),
    ?assertMatch(true, riakc_pb_socket:is_connected(Pid)),

    %% Send a ping request in another process so the test doesn't block
    Self = self(),
    spawn(fun() -> Self ! (catch riakc_pb_socket:ping(Pid, infinity)) end),

    %% Make sure request received then kill the process
    {ok, _ReqMsg} = gen_tcp:recv(Sock, 0),
    exit(Pid, kill),
    receive
        Msg ->
            ?assertMatch({'EXIT', {killed, _}}, Msg)
    end,
    %% Cleanup
    ok = gen_tcp:close(Sock),
    ok = gen_tcp:close(Listen).

adding_hll_to_map_throws_error_test() ->
    UpdateFun = fun(H) ->
                    riakc_hll:add_elements([<<"X">>, <<"Y">>], H)
                end,
    HllKey = {<<"hll">>, hll},
    ?assertError(badarg, riakc_map:update(HllKey, UpdateFun, riakc_map:new())).

%%
%% Tests to run against a live node - NB the node gets reconfigured and generally messed with
%%
integration_tests() ->
    [{"ping",
      ?_test( begin
                  {ok, Pid} = riakc_test_utils:start_link(),
                  ?assertEqual(pong, riakc_pb_socket:ping(Pid)),
                  ?assertEqual(true, riakc_pb_socket:is_connected(Pid)),
                  riakc_pb_socket:stop(Pid)
              end)},

     {"reconnect test",
      ?_test( begin
                  %% Make sure originally there
                  {ok, Pid} = riakc_test_utils:start_link(),

                  %% Change the options to allow reconnection/queueing
                  riakc_pb_socket:set_options(Pid, [queue_if_disconnected]),

                  %% Kill the socket
                  riakc_test_utils:kill_riak_pb_sockets(),
                  ?assertEqual(pong, riakc_pb_socket:ping(Pid)),
                  riakc_pb_socket:stop(Pid)
              end)},

     {"set client id",
      ?_test(
         begin
             {ok, Pid} = riakc_test_utils:start_link(),
             {ok, <<OrigId:32>>} = riakc_pb_socket:get_client_id(Pid),

             NewId = <<(OrigId+1):32>>,
             ok = riakc_pb_socket:set_client_id(Pid, NewId),
             {ok, NewId} = riakc_pb_socket:get_client_id(Pid)
         end)},

     {"version",
      ?_test(
         begin
             {ok, Pid} = riakc_test_utils:start_link(),
             {ok, ServerInfo} = riakc_pb_socket:get_server_info(Pid),
             [{node, _}, {server_version, _}] = lists:sort(ServerInfo)
         end)},

     {"get_should_read_put_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 O0 = riakc_obj:new(<<"b">>, <<"k">>),
                 O = riakc_obj:update_value(O0, <<"v">>),
                 {ok, PO} = riakc_pb_socket:put(Pid, O, [return_body]),
                 {ok, GO} = riakc_pb_socket:get(Pid, <<"b">>, <<"k">>),
                 ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
             end)},

     {"get should read put with timeout",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 O0 = riakc_obj:new(<<"b">>, <<"k">>),
                 O = riakc_obj:update_value(O0, <<"v">>),
                 {ok, PO} = riakc_pb_socket:put(Pid, O, [{w, 1}, {dw, 1}, return_body]),
                 {ok, GO} = riakc_pb_socket:get(Pid, <<"b">>, <<"k">>, 500),
                 ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
             end)},

     {"get should read put with options",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 O0 = riakc_obj:new(<<"b">>, <<"k">>),
                 O = riakc_obj:update_value(O0, <<"v">>),
                 {ok, PO} = riakc_pb_socket:put(Pid, O, [{w, 1}, {dw, 1}, return_body]),
                 {ok, GO} = riakc_pb_socket:get(Pid, <<"b">>, <<"k">>, [{r, 1}]),
                 ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
             end)},

     {"get should read put with non integer options",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 O0 = riakc_obj:new(<<"b">>, <<"k">>),
                 O = riakc_obj:update_value(O0, <<"v">>),
                 {ok, PO} = riakc_pb_socket:put(Pid, O, [{w, all}, {dw, quorum}, return_body]),
                 {ok, GO} = riakc_pb_socket:get(Pid, <<"b">>, <<"k">>, [{r, one}]),
                 ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
             end)},

     {"put and delete with timeout",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 PO = riakc_obj:new(<<"b">>, <<"puttimeouttest">>, <<"value">>),
                 ok = riakc_pb_socket:put(Pid, PO, 500),
                 {ok, GO} = riakc_pb_socket:get(Pid, <<"b">>, <<"puttimeouttest">>, 500),
                 ?assertEqual(<<"value">>, riakc_obj:get_value(GO)),
                 ok = riakc_pb_socket:delete(Pid, <<"b">>, <<"puttimeouttest">>, 500),
                 {error, notfound} = riakc_pb_socket:get(Pid, <<"b">>, <<"puttimeouttest">>)
             end)},

     {"update_should_change_value_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 O0 = riakc_obj:new(<<"b">>, <<"k">>),
                 O = riakc_obj:update_value(O0, <<"v">>),
                 {ok, PO} = riakc_pb_socket:put(Pid, O, [return_body]),
                 PO2 = riakc_obj:update_value(PO, <<"v2">>),
                 ok = riakc_pb_socket:put(Pid, PO2),
                 {ok, GO} = riakc_pb_socket:get(Pid, <<"b">>, <<"k">>),
                 ?assertEqual(<<"v2">>, riakc_obj:get_value(GO))
             end)},

     {"key_should_be_missing_after_delete_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 %% Put key/value
                 O0 = riakc_obj:new(<<"b">>, <<"k">>),
                 O = riakc_obj:update_value(O0, <<"v">>),
                 {ok, _PO} = riakc_pb_socket:put(Pid, O, [return_body]),
                 %% Prove it really got stored
                 {ok, GO1} = riakc_pb_socket:get(Pid, <<"b">>, <<"k">>),
                 ?assertEqual(<<"v">>, riakc_obj:get_value(GO1)),
                 %% Delete and check no longer found
                 ok = riakc_pb_socket:delete(Pid, <<"b">>, <<"k">>),
                 {error, notfound} = riakc_pb_socket:get(Pid, <<"b">>, <<"k">>)
             end)},

    {"delete missing key test",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                  %% Delete and check no longer found
                 ok = riakc_pb_socket:delete(Pid, <<"notabucket">>, <<"k">>, [{rw, 1}]),
                 {error, notfound} = riakc_pb_socket:get(Pid, <<"notabucket">>, <<"k">>)
             end)},

     {"empty_list_buckets_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 ?assertEqual({ok, []}, riakc_pb_socket:list_buckets(Pid))
             end)},

     {"list_buckets_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Bs = lists:sort([list_to_binary(["b"] ++ integer_to_list(N)) || N <- lists:seq(1, 10)]),
                 F = fun(B) ->
                             O=riakc_obj:new(B, <<"key">>),
                             riakc_pb_socket:put(Pid, riakc_obj:update_value(O, <<"val">>))
                     end,
                 [F(B) || B <- Bs],
                 {ok, LBs} = riakc_pb_socket:list_buckets(Pid),
                 ?assertEqual(Bs, lists:sort(LBs))
             end)},

     {"list_keys_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Bucket = <<"listkeys">>,
                 Ks = lists:sort([list_to_binary(integer_to_list(N)) || N <- lists:seq(1, 10)]),
                 F = fun(K) ->
                             O=riakc_obj:new(Bucket, K),
                             riakc_pb_socket:put(Pid, riakc_obj:update_value(O, <<"val">>))
                     end,
                 [F(K) || K <- Ks],
                 {ok, LKs} = riakc_pb_socket:list_keys(Pid, Bucket),
                 ?assertEqual(Ks, lists:sort(LKs)),

                 %% Make sure it works with an infinite timeout (will reset the timeout
                 %% timer after each packet)
                 {ok, LKs2} = riakc_pb_socket:list_keys(Pid, Bucket, infinity),
                 ?assertEqual(Ks, lists:sort(LKs2))
             end)},

     {"get bucket properties test",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 {ok, Props} = riakc_pb_socket:get_bucket(Pid, <<"b">>),
                 ?assertEqual(3, proplists:get_value(n_val, Props)),
                 ?assertEqual(false, proplists:get_value(allow_mult, Props))
             end)},

     {"set bucket properties test",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 ok = riakc_pb_socket:set_bucket(Pid, <<"b">>, [{n_val, 2}, {allow_mult, false}]),
                 {ok, Props} = riakc_pb_socket:get_bucket(Pid, <<"b">>),
                 ?assertEqual(2, proplists:get_value(n_val, Props)),
                 ?assertEqual(false, proplists:get_value(allow_mult, Props))
             end)},

     {"allow_mult should allow dupes",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid1} = riakc_test_utils:start_link(),
                 {ok, Pid2} = riakc_test_utils:start_link(),
                 ok = riakc_pb_socket:set_bucket(Pid1, <<"multibucket">>, [{allow_mult, true}]),
                 riakc_pb_socket:delete(Pid1, <<"multibucket">>, <<"foo">>),
                 {error, notfound} = riakc_pb_socket:get(Pid1, <<"multibucket">>, <<"foo">>),
                 O = riakc_obj:new(<<"multibucket">>, <<"foo">>),
                 O1 = riakc_obj:update_value(O, <<"pid1">>),
                 O2 = riakc_obj:update_value(O, <<"pid2">>),
                 ok = riakc_pb_socket:put(Pid1, O1),

                 ok = riakc_pb_socket:put(Pid2, O2),
                 {ok, O3} = riakc_pb_socket:get(Pid1, <<"multibucket">>, <<"foo">>),
                 ?assertEqual([<<"pid1">>, <<"pid2">>], lists:sort(riakc_obj:get_values(O3))),
                 O4 = riakc_obj:update_value(riakc_obj:select_sibling(1, O3), <<"resolved">>),
                 ok = riakc_pb_socket:put(Pid1, O4),
                 {ok, GO} = riakc_pb_socket:get(Pid1, <<"multibucket">>, <<"foo">>),
                 ?assertEqual([<<"resolved">>], lists:sort(riakc_obj:get_values(GO))),
                 riakc_pb_socket:delete(Pid1, <<"multibucket">>, <<"foo">>)
             end)},

     {"update object test",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 O0 = riakc_obj:new(<<"b">>, <<"k">>, <<"d">>),
                 io:format("O0: ~p\n", [O0]),
                 {ok, O1} = riakc_pb_socket:put(Pid, O0, [return_body]),
                 io:format("O1: ~p\n", [O1]),
                 M1 = riakc_obj:get_metadata(O1),
                 M2 = dict:store(?MD_LINKS, [{{<<"b">>, <<"k1">>}, <<"t1">>}], M1),
                 O2 = riakc_obj:update_metadata(O1, M2),
                 riakc_pb_socket:put(Pid, O2)
             end)},

     {"queue test",
      ?_test(begin
                 %% Would really like this in a nested {setup, blah} structure
                 %% but eunit does not allow
                 {ok, Pid} = riakc_test_utils:start_link(),
                 riakc_test_utils:pause_riak_pb_listener(),
                 Me = self(),
                 %% this request will block as
                 spawn(fun() -> Me ! {1, riakc_pb_socket:ping(Pid)} end),
                 %% this request should be queued as socket will not be created
                 spawn(fun() -> Me ! {2, riakc_pb_socket:ping(Pid)} end),
                 riakc_test_utils:resume_riak_pb_listener(),
                 receive {1,Ping1} -> ?assertEqual(Ping1, pong) end,
                 receive {2,Ping2} -> ?assertEqual(Ping2, pong) end
             end)},

    {"timeout queue test",
      ?_test(begin
                 %% Would really like this in a nested {setup, blah} structure
                 %% but eunit does not allow
                 riakc_test_utils:pause_riak_pb_listener(),
                 {ok, Pid} = riakc_test_utils:start_link([queue_if_disconnected]),
                 Me = self(),
                 %% this request will block as
                 spawn(fun() -> Me ! {1, riakc_pb_socket:ping(Pid, 0)} end),
                 %% this request should be queued as socket will not be created
                 spawn(fun() -> Me ! {2, riakc_pb_socket:ping(Pid, 0)},  Me ! running end),
                 receive running -> ok end,
                 riakc_test_utils:resume_riak_pb_listener(),
                 receive {1,Ping1} -> ?assertEqual({error, timeout}, Ping1) end,
                 receive {2,Ping2} -> ?assertEqual({error, timeout}, Ping2) end
             end)},

    {"ignore stale tref test",
      ?_test(begin
                 %% Would really like this in a nested {setup, blah} structure
                 %% but eunit does not allow
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Pid ! {req_timeout, make_ref()},
                 ?assertEqual(pong, riakc_pb_socket:ping(Pid))
             end)},

   {"infinite timeout ping test",
      ?_test(begin
                 %% Would really like this in a nested {setup, blah} structure
                 %% but eunit does not allow
                 {ok, Pid} = riakc_test_utils:start_link(),
                 ?assertEqual(pong, riakc_pb_socket:ping(Pid, infinity)),
                 ?assertEqual(pong, riakc_pb_socket:ping(Pid, undefined))
             end)},

     {"javascript_source_map_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 B = <<"bucket">>,
                 K = <<"foo">>,
                 O = riakc_obj:new(B, K),
                 riakc_pb_socket:put(Pid, riakc_obj:update_value(O, <<"2">>, "application/json")),
                 Inputs = [{B, K}],
                 Args = [{map, {jsanon, <<"function (v) { return [JSON.parse(v.values[0].data)]; }">>}, undefined, true}],
                 Want = {ok, [{0, [2]}]},
                 Got = riakc_pb_socket:mapred(Pid, Inputs, Args),
                 ?assertMatch(Want, Got)
             end)},

     {"javascript_source_map_with_bucket_type_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 BT = <<"plain">>,
                 B = <<"my_bucket">>,
                 BWT = {BT, B}, % NB: Bucket With Type
                 K = <<"baz">>,
                 Value = <<"bat">>,
                 O = riakc_obj:new(BWT, K),
                 O2 = riakc_obj:update_value(O, Value, "application/json"),
                 ?assertMatch(BT, riakc_obj:bucket_type(O2)),
                 ?assertMatch(BWT, riakc_obj:bucket(O2)),
                 ?assertMatch(K, riakc_obj:key(O2)),
                 ?assertMatch(ok, riakc_pb_socket:put(Pid, O2)),
                 {ok, GotObj} = riakc_pb_socket:get(Pid, BWT, K),
                 ?assertMatch(BT, riakc_obj:bucket_type(GotObj)),
                 ?assertMatch(BWT, riakc_obj:bucket(GotObj)),
                 ?assertMatch(K, riakc_obj:key(GotObj)),
                 % NB: see basho/riak_kv#1623
                 Inputs = [{{BWT, K}, ignored},{{BWT, K}, ignored}],
                 Args = [{map, {jsanon, <<"function (v) { return [v.values[0].data]; }">>}, undefined, true}],
                 Want = {ok, [{0, [Value, Value]}]},
                 Got = riakc_pb_socket:mapred(Pid, Inputs, Args),
                 ?assertMatch(Want, Got)
             end)},

     {"javascript_named_map_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 B = <<"bucket">>,
                 K = <<"foo">>,
                 O=riakc_obj:new(B, K),
                 riakc_pb_socket:put(Pid, riakc_obj:update_value(O, <<"99">>, "application/json")),

                 ?assertEqual({ok, [{0, [99]}]},
                              riakc_pb_socket:mapred(Pid,
                                             [{B, K}],
                                             [{map, {jsfun, <<"Riak.mapValuesJson">>},
                                               undefined, true}]))
             end)},

     {"javascript_source_map_reduce_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Store = fun({K,V}) ->
                                 O=riakc_obj:new(<<"bucket">>, K),
                                 riakc_pb_socket:put(Pid,riakc_obj:update_value(O, V, "application/json"))
                         end,
                 [Store(KV) || KV <- [{<<"foo">>, <<"2">>},
                                      {<<"bar">>, <<"3">>},
                                      {<<"baz">>, <<"4">>}]],

                 ?assertEqual({ok, [{1, [3]}]},
                              riakc_pb_socket:mapred(Pid,
                                             [{<<"bucket">>, <<"foo">>},
                                              {<<"bucket">>, <<"bar">>},
                                              {<<"bucket">>, <<"baz">>}],
                                             [{map, {jsanon, <<"function (v) { return [1]; }">>},
                                               undefined, false},
                                              {reduce, {jsanon,
                                                        <<"function(v) {
                                                             total = v.reduce(
                                                               function(prev,curr,idx,array) {
                                                                 return prev+curr;
                                                               }, 0);
                                                             return [total];
                                                           }">>},
                                               undefined, true}]))
             end)},

     {"javascript_named_map_reduce_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Store = fun({K,V}) ->
                                 O=riakc_obj:new(<<"bucket">>, K),
                                 riakc_pb_socket:put(Pid,riakc_obj:update_value(O, V, "application/json"))
                         end,
                 [Store(KV) || KV <- [{<<"foo">>, <<"2">>},
                                      {<<"bar">>, <<"3">>},
                                      {<<"baz">>, <<"4">>}]],

                 ?assertEqual({ok, [{1, [9]}]},
                              riakc_pb_socket:mapred(Pid,
                                             [{<<"bucket">>, <<"foo">>},
                                              {<<"bucket">>, <<"bar">>},
                                              {<<"bucket">>, <<"baz">>}],
                                             [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                              {reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}]))
             end)},

     {"javascript_bucket_map_reduce_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Store = fun({K,V}) ->
                                 O=riakc_obj:new(<<"bucket">>, K),
                                 riakc_pb_socket:put(Pid,riakc_obj:update_value(O, V, "application/json"))
                         end,
                 [Store(KV) || KV <- [{<<"foo">>, <<"2">>},
                                      {<<"bar">>, <<"3">>},
                                      {<<"baz">>, <<"4">>}]],

                 ?assertEqual({ok, [{1, [9]}]},
                              riakc_pb_socket:mapred_bucket(Pid, <<"bucket">>,
                                                    [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                                     {reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}]))
             end)},

     {"javascript_bucket_type_map_reduce_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 BT = {<<"plain">>,<<"bucket">>},
                 Store = fun({K, V}) ->
                                 O=riakc_obj:new(BT, K),
                                 riakc_pb_socket:put(Pid,riakc_obj:update_value(O, V, "application/json"))
                         end,
                 [Store(KV) || KV <- [{<<"foo">>, <<"2">>},
                                      {<<"bar">>, <<"3">>},
                                      {<<"baz">>, <<"4">>}]],
                 Want = {ok, [{1, [9]}]},
                 Query = [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                {reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}],
                 Got = riakc_pb_socket:mapred_bucket(Pid, BT, Query),
                 ?assertMatch(Want, Got)
             end)},

     {"javascript_arg_map_reduce_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 O=riakc_obj:new(<<"bucket">>, <<"foo">>),
                 riakc_pb_socket:put(Pid, riakc_obj:update_value(O, <<"2">>, "application/json")),
                 ?assertEqual({ok, [{1, [10]}]},
                              riakc_pb_socket:mapred(Pid,
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
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Store = fun({K,V}) ->
                                 O=riakc_obj:new(<<"bucket">>, K),
                                 riakc_pb_socket:put(Pid,riakc_obj:update_value(O, V, "application/json"))
                         end,
                 [Store(KV) || KV <- [{<<"foo">>, <<"2">>},
                                      {<<"bar">>, <<"3">>},
                                      {<<"baz">>, <<"4">>}]],

                 {ok, [{1, Results}]} = riakc_pb_socket:mapred(Pid,
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
     {"erlang_map_reduce_binary_2i_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Store = fun({K,V,I}) ->
                                 O=riakc_obj:new(<<"bucket">>, K),
                                 MD=riakc_obj:add_secondary_index(dict:new(), I),
                                 O2=riakc_obj:update_metadata(O,MD),
                                 riakc_pb_socket:put(Pid,riakc_obj:update_value(O2, V, "application/json"))
                         end,
                 [Store(KV) || KV <- [{<<"foo">>, <<"2">>, {{binary_index, "idx"}, [<<"a">>]}},
                                      {<<"bar">>, <<"3">>, {{binary_index, "idx"}, [<<"b">>]}},
                                      {<<"baz">>, <<"4">>, {{binary_index, "idx"}, [<<"a">>]}}]],

                 {ok, [{1, Results}]} = riakc_pb_socket:mapred(Pid,
                                                       {index,<<"bucket">>,{binary_index, "idx"}, <<"a">>},
                                                       [{map, {modfun, riak_kv_mapreduce,
                                                               map_object_value},
                                                         undefined, false},
                                                        {reduce, {modfun, riak_kv_mapreduce,
                                                                  reduce_set_union},
                                                         undefined, true}]),
                 ?assertEqual([<<"2">>, <<"4">>], lists:sort(Results))
             end)},
     {"erlang_map_reduce_integer_2i_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Store = fun({K,V,I}) ->
                                 O=riakc_obj:new(<<"bucket">>, K),
                                 MD=riakc_obj:add_secondary_index(dict:new(), I),
                                 O2=riakc_obj:update_metadata(O,MD),
                                 riakc_pb_socket:put(Pid,riakc_obj:update_value(O2, V, "application/json"))
                         end,
                 [Store(KV) || KV <- [{<<"foo">>, <<"2">>, {{integer_index, "idx"}, [4]}},
                                      {<<"bar">>, <<"3">>, {{integer_index, "idx"}, [7]}},
                                      {<<"baz">>, <<"4">>, {{integer_index, "idx"}, [4]}}]],

                 {ok, [{1, Results}]} = riakc_pb_socket:mapred(Pid,
                                                       {index,<<"bucket">>,{integer_index, "idx"},3,5},
                                                       [{map, {modfun, riak_kv_mapreduce,
                                                               map_object_value},
                                                         undefined, false},
                                                        {reduce, {modfun, riak_kv_mapreduce,
                                                                  reduce_set_union},
                                                         undefined, true}]),
                 ?assertEqual([<<"2">>, <<"4">>], lists:sort(Results))
             end)},
     {"missing_key_erlang_map_reduce_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = {ok, Pid} = riakc_test_utils:start_link(),
                 {ok, Results} = riakc_pb_socket:mapred(Pid, [{<<"bucket">>, <<"foo">>},
                                                      {<<"bucket">>, <<"bar">>},
                                                      {<<"bucket">>, <<"baz">>}],
                                                [{map, {modfun, riak_kv_mapreduce,
                                                        map_object_value},
                                                  <<"include_notfound">>, false},
                                                 {reduce, {modfun, riak_kv_mapreduce,
                                                           reduce_set_union},
                                                  undefined, true}]),
                 [{1, [{error, notfound}|_]}] = Results end)},
     {"missing_key_javascript_map_reduce_test()",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = {ok, Pid} = riakc_test_utils:start_link(),
                 {ok, Results} = riakc_pb_socket:mapred(Pid, [{<<"bucket">>, <<"foo">>},
                                                      {<<"bucket">>, <<"bar">>},
                                                      {<<"bucket">>, <<"baz">>}],
                                                [{map, {jsfun, <<"Riak.mapValuesJson">>},
                                                  undefined, false},
                                                 {reduce, {jsfun, <<"Riak.reduceSort">>},
                                                  undefined, true}]),
                 [{1, [{not_found, {_, _},<<"undefined">>}|_]}] = Results end)},
     {"map reduce bad inputs",
      ?_test(begin
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Res = riakc_pb_socket:mapred(Pid, undefined,
                                             [{map, {jsfun, <<"Riak.mapValuesJson">>},
                                               undefined, false},
                                              {reduce, {jsfun, <<"Riak.reduceSum">>},
                                               undefined, true}]),
                 ?assertEqual({error, <<"{inputs,{\"Inputs must be a binary bucket, a tuple of bucket and key-filters, a list of target tuples, or a search, index, or modfun tuple:\",\n         undefined}}">>},
                              Res )
             end)},
     {"map reduce bad input keys",
      ?_test(begin
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Res = riakc_pb_socket:mapred(Pid, [<<"b">>], % no {B,K} tuple
                                      [{map, {jsfun, <<"Riak.mapValuesJson">>},
                                        undefined, false},
                                       {reduce, {jsfun, <<"Riak.reduceSum">>},
                                        undefined, true}]),
                 ?assertEqual({error,<<"{inputs,{\"Inputs target tuples must be {B,K} or {{B,K},KeyData}:\",[<<\"b\">>]}}">>},
                              Res)
             end)},
     {"map reduce bad query",
      ?_test(begin
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Res = riakc_pb_socket:mapred(Pid, [{<<"b">>,<<"k">>}], % no {B,K} tuple
                                      undefined),
                 ?assertEqual({error,<<"{query,{\"Query takes a list of step tuples\",undefined}}">>},
                              Res)
             end)},
     {"get should convert erlang terms",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 TestNode = riakc_test_utils:test_riak_node(),
                 MyBin = <<"some binary">>,
                 MyTerm = [<<"b">>,<<"a_term">>,{some_term, ['full', "of", 123, 654.321]}],
                 BinObj = rpc:call(TestNode, riak_object, new,
                                   [<<"b">>, <<"a_bin">>, MyBin]),
                 TermObj = rpc:call(TestNode, riak_object, new,
                                    [<<"b">>, <<"a_term">>, MyTerm]),
                 {ok, C} = rpc:call(TestNode, riak, local_client, []),
                 %% parameterized module trickery - stick it as the last argument
                 ok = rpc:call(TestNode, riak_client, put, [BinObj, 1, C]),
                 ok = rpc:call(TestNode, riak_client, put, [TermObj, 1, C]),

                 {ok, Pid} = riakc_test_utils:start_link(),
                 {ok, GotBinObj} = riakc_pb_socket:get(Pid, <<"b">>, <<"a_bin">>),
                 {ok, GotTermObj} = riakc_pb_socket:get(Pid, <<"b">>, <<"a_term">>),

                 ?assertEqual(riakc_obj:get_value(GotBinObj), MyBin),
                 ?assertEqual(riakc_obj:get_content_type(GotTermObj),
                              "application/x-erlang-binary"),
                 ?assertEqual(binary_to_term(riakc_obj:get_value(GotTermObj)), MyTerm)
             end)},
     {"putting without a key should generate one",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    PO = riakc_obj:new(<<"b">>, undefined, <<"value">>),
                    Res1 = riakc_pb_socket:put(Pid, PO),
                    Res2 = riakc_pb_socket:put(Pid, PO),
                    ?assertMatch({ok, _K}, Res1),
                    ?assertMatch({ok, _K}, Res2),
                    {ok, K1} = Res1,
                    {ok, K2} = Res2,
                    ?assertMatch(true, is_binary(K1)),
                    ?assertMatch(true, is_binary(K2)),
                    % Make sure the same key isn't generated twice
                    ?assert(Res1 =/= Res2)
             end)},
     {"putting without a key should generate one with return_body",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    PO = riakc_obj:new(<<"b">>, undefined, <<"value">>),
                    {ok, Obj1} = riakc_pb_socket:put(Pid, PO, [return_body]),
                    {ok, Obj2} = riakc_pb_socket:put(Pid, PO, [return_body]),
                    %% Make sure the same key isn't generated twice
                    ?assertEqual(riakc_obj, element(1, Obj1)),
                    ?assertEqual(riakc_obj, element(1, Obj2)),
                    ?assert(riakc_obj:key(Obj1) /= riakc_obj:key(Obj2))
             end)},
     {"conditional gets should return unchanged if the vclock matches",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    PO = riakc_obj:new(<<"b">>, <<"key">>, <<"value">>),
                    riakc_pb_socket:put(Pid, PO),
                    {ok, Obj} = riakc_pb_socket:get(Pid, <<"b">>, <<"key">>),
                    VClock = riakc_obj:vclock(Obj),
                    %% object hasn't changed
                    ?assertEqual(unchanged, riakc_pb_socket:get(Pid, <<"b">>, <<"key">>,
                            [{if_modified, VClock}])),
                    %% change the object and make sure unchanged isn't returned
                    P1 = riakc_obj:update_value(Obj, <<"newvalue">>),
                    riakc_pb_socket:put(Pid, P1),
                    ?assertMatch({ok, _}, riakc_pb_socket:get(Pid, <<"b">>, <<"key">>,
                            [{if_modified, VClock}]))
             end)},
     {"the head get option should return the object metadata without the value",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    PO = riakc_obj:new(<<"b">>, <<"key">>, <<"value">>),
                    riakc_pb_socket:put(Pid, PO),
                    {ok, Obj} = riakc_pb_socket:get(Pid, <<"b">>, <<"key">>, [head]),
                    ?assertEqual(<<>>, riakc_obj:get_value(Obj)),
                    {ok, Obj2} = riakc_pb_socket:get(Pid, <<"b">>, <<"key">>, []),
                    ?assertEqual(<<"value">>, riakc_obj:get_value(Obj2))
             end)},
     {"conditional put should allow you to avoid overwriting a value if it already exists",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    PO = riakc_obj:new(<<"b">>, <<"key">>, <<"value">>),
                    ?assertEqual(ok, riakc_pb_socket:put(Pid, PO, [if_none_match])),
                    ?assertEqual({error, <<"match_found">>}, riakc_pb_socket:put(Pid, PO, [if_none_match]))
             end)},
     {"conditional put should allow you to avoid overwriting a value if its been updated",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    PO = riakc_obj:new(<<"b">>, <<"key">>, <<"value">>),
                    {ok, Obj} = riakc_pb_socket:put(Pid, PO, [return_body]),
                    Obj2 = riakc_obj:update_value(Obj, <<"newvalue">>),
                    ?assertEqual(ok, riakc_pb_socket:put(Pid, Obj2, [if_not_modified])),
                    ?assertEqual({error, <<"modified">>}, riakc_pb_socket:put(Pid, Obj2, [if_not_modified]))
             end)},
     {"if_not_modified should fail if the object is not found",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    PO = riakc_obj:new(<<"b">>, <<"key">>, <<"value">>),
                    ?assertEqual({error, <<"notfound">>}, riakc_pb_socket:put(Pid, PO, [if_not_modified]))
             end)},
     {"return_head should empty out the value in the riak object",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    PO = riakc_obj:new(<<"b">>, <<"key">>, <<"value">>),
                    {ok, Obj} = riakc_pb_socket:put(Pid, PO, [return_head]),
                    ?assertEqual(<<>>, riakc_obj:get_value(Obj))
             end)},
     {"return_head should empty out all values when there's siblings",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    ok = riakc_pb_socket:set_bucket(Pid, <<"b">>, [{allow_mult, true}]),
                    PO = riakc_obj:new(<<"b">>, <<"key">>, <<"value">>),
                    {ok, Obj} = riakc_pb_socket:put(Pid, PO, [return_head]),
                    ?assertEqual(<<>>, riakc_obj:get_value(Obj)),
                    {ok, Obj2} = riakc_pb_socket:put(Pid, PO, [return_head]),
                    ?assertEqual([<<>>, <<>>], riakc_obj:get_values(Obj2))
             end)},

    {"user metadata manipulation",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    O0 = riakc_obj:new(<<"b">>, <<"key0">>, <<"value0">>),
                    MD0 = riakc_obj:get_update_metadata(O0),
                    MD1 = riakc_obj:set_user_metadata_entry(MD0, {<<"Key1">>,<<"Val1">>}),
                    O1 = riakc_obj:update_metadata(O0, MD1),
                    ?assertEqual(ok, riakc_pb_socket:put(Pid, O1)),
                    {ok, O2} = riakc_pb_socket:get(Pid, <<"b">>, <<"key0">>),
                    MD2 = riakc_obj:get_update_metadata(O2),
                    ?assertEqual([{<<"Key1">>,<<"Val1">>}], riakc_obj:get_user_metadata_entries(MD2)),
                    MD3 = riakc_obj:set_user_metadata_entry(MD2, {<<"Key2">>,<<"Val2">>}),
                    O3 = riakc_obj:update_metadata(O2, MD3),
                    ?assertEqual(ok, riakc_pb_socket:put(Pid, O3)),
                    {ok, O4} = riakc_pb_socket:get(Pid, <<"b">>, <<"key0">>),
                    ?assertEqual(2, length(riakc_obj:get_user_metadata_entries(riakc_obj:get_update_metadata(O4))))
             end)},
    {"binary secondary index manipulation",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    O0 = riakc_obj:new(<<"b">>, <<"key1">>, <<"value1">>),
                    MD0 = riakc_obj:get_update_metadata(O0),
                    MD1 = riakc_obj:set_secondary_index(MD0, [{{binary_index, "idx"},[<<"aaa">>]}]),
                    O1 = riakc_obj:update_metadata(O0, MD1),
                    ?assertEqual(ok, riakc_pb_socket:put(Pid, O1)),
                    {ok, O2} = riakc_pb_socket:get(Pid, <<"b">>, <<"key1">>),
                    MD2 = riakc_obj:get_update_metadata(O2),
                    ?assertEqual([<<"aaa">>], lists:sort(riakc_obj:get_secondary_index(MD2,{binary_index,"idx"}))),
                    MD3 = riakc_obj:add_secondary_index(MD2, [{{binary_index, "idx"},[<<"bbb">>,<<"aaa">>,<<"ccc">>]}]),
                    O3 = riakc_obj:update_metadata(O2, MD3),
                    ?assertEqual(ok, riakc_pb_socket:put(Pid, O3)),
                    ?assertEqual({ok,?INDEX_RESULTS{keys=[<<"key1">>]}},
                                 riakc_pb_socket:get_index(Pid, <<"b">>, {binary_index, "idx"}, <<"bbb">>)),
                    {ok, O4} = riakc_pb_socket:get(Pid, <<"b">>, <<"key1">>),
                    MD4 = riakc_obj:get_update_metadata(O4),
                    ?assertEqual([<<"aaa">>,<<"bbb">>,<<"ccc">>], lists:sort(riakc_obj:get_secondary_index(MD4, {binary_index, "idx"}))),
                    MD5 = riakc_obj:delete_secondary_index(MD4,{binary_index,"idx"}),
                    O5 = riakc_obj:update_metadata(O4, MD5),
                    ?assertEqual(ok, riakc_pb_socket:put(Pid, O5))
             end)},
     {"integer secondary index manipulation",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    O0 = riakc_obj:new(<<"b">>, <<"key2">>, <<"value2">>),
                    MD0 = riakc_obj:get_update_metadata(O0),
                    MD1 = riakc_obj:set_secondary_index(MD0, [{{integer_index, "idx"},[67]}]),
                    O1 = riakc_obj:update_metadata(O0, MD1),
                    ?assertEqual(ok, riakc_pb_socket:put(Pid, O1)),
                    {ok, O2} = riakc_pb_socket:get(Pid, <<"b">>, <<"key2">>),
                    MD2 = riakc_obj:get_update_metadata(O2),
                    ?assertEqual([67], lists:sort(riakc_obj:get_secondary_index(MD2,{integer_index,"idx"}))),
                    MD3 = riakc_obj:add_secondary_index(MD2, [{{integer_index, "idx"},[56,10000,100]}]),
                    O3 = riakc_obj:update_metadata(O2, MD3),
                    ?assertEqual(ok, riakc_pb_socket:put(Pid, O3)),
                    ?assertEqual({ok,?INDEX_RESULTS{keys=[<<"key2">>]}},
                                 riakc_pb_socket:get_index(Pid, <<"b">>, {integer_index, "idx"}, 50, 60)),
                    {ok, O4} = riakc_pb_socket:get(Pid, <<"b">>, <<"key2">>),
                    MD4 = riakc_obj:get_update_metadata(O4),
                    ?assertEqual([56,67,100,10000], lists:sort(riakc_obj:get_secondary_index(MD4, {integer_index, "idx"}))),
                    MD5 = riakc_obj:delete_secondary_index(MD4,{integer_index,"idx"}),
                    O5 = riakc_obj:update_metadata(O4, MD5),
                    ?assertEqual(ok, riakc_pb_socket:put(Pid, O5))
             end)},
     {"counter increment / decrement / get value",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 unlink(Pid),
                 Bucket = <<"counter_test_bucket">>,
                 Key = <<"test_counter">>,
                 %% counters require allow_mult to be true
                 ok = riakc_pb_socket:set_bucket(Pid, Bucket, [{allow_mult, true}]),
                 ok = riakc_pb_socket:counter_incr(Pid, Bucket, Key, 10),
                 ?assertEqual({ok, 10}, riakc_pb_socket:counter_val(Pid, Bucket, Key)),
                 ok = riakc_pb_socket:counter_incr(Pid, Bucket, Key, -5, [{w, quorum}, {pw, one}, {dw, all}]),
                 ?assertEqual({ok, 5}, riakc_pb_socket:counter_val(Pid, Bucket, Key, [{pr, one}]))
             end)},
     {"create a search index / get / list / delete with default timeout",
     {timeout, 30, ?_test(begin
                riakc_test_utils:reset_riak(),
                {ok, Pid} = riakc_test_utils:start_link(),
                riakc_test_utils:reset_solr(Pid),
                Index = <<"indextest">>,
                SchemaName = <<"_yz_default">>,
                ?assertEqual(ok,
                    riakc_pb_socket:create_search_index(Pid,
                                                Index,
                                                SchemaName,
                                                [{n_val,2}])),
                    case riakc_pb_socket:get_search_index(Pid, Index) of
                        {ok, IndexData} ->
                            ?assertEqual(proplists:get_value(
                                         index, IndexData), Index),
                            ?assertEqual(proplists:get_value(
                                         schema, IndexData), SchemaName),
                            ?assertEqual(proplists:get_value(
                                         n_val, IndexData), 2);
                        {error, <<"notfound">>} ->
                            false
                    end,
                ?assertEqual({ok, [[{index,Index},
                                    {schema,SchemaName},
                                    {n_val,2}]]},
                             riakc_pb_socket:list_search_indexes(Pid)),
                ?assertEqual(ok, riakc_pb_socket:delete_search_index(Pid, Index))
             end)}},
     {"create a search index / get with user-set timeout",
     {timeout, 30, ?_test(begin
                riakc_test_utils:reset_riak(),
                {ok, Pid} = riakc_test_utils:start_link(),
                riakc_test_utils:reset_solr(Pid),
                Index = <<"indexwithintimeouttest">>,
                SchemaName = <<"_yz_default">>,
                ?assertEqual(ok,
                    riakc_pb_socket:create_search_index(Pid,
                                                Index,
                                                SchemaName,
                                                20000)),
                    case riakc_pb_socket:get_search_index(Pid, Index) of
                        {ok, IndexData} ->
                            ?assertEqual(proplists:get_value(
                                         index, IndexData), Index),
                            ?assertEqual(proplists:get_value(
                                         schema, IndexData), SchemaName);
                        {error, <<"notfound">>} ->
                            false
                    end
             end)}},
     {"create a search schema / get",
      {timeout, 30, ?_test(begin
                riakc_test_utils:reset_riak(),
                {ok, Pid} = riakc_test_utils:start_link(),
                riakc_test_utils:reset_solr(Pid),
                Schema = <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" stored=\"false\" multiValued=\"false\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" stored=\"false\" multiValued=\"false\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" stored=\"false\" multiValued=\"false\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"false\" multiValued=\"false\"/>
</fields>
<uniqueKey>_yz_id</uniqueKey>
<types>
    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
</types>
</schema>">>,
                Index = <<"schemaindex">>,
                SchemaName = <<"myschema">>,
                ?assertEqual(ok, riakc_pb_socket:create_search_schema(Pid, SchemaName, Schema)),
                ?assertEqual(ok, riakc_pb_socket:create_search_index(Pid, Index, SchemaName, [])),
                riakc_test_utils:wait_until( fun() ->
                    case riakc_pb_socket:list_search_indexes(Pid) of
                        {ok, []} ->
                            false;
                        {ok, [IndexData|_]} ->
                            proplists:get_value(index, IndexData) == Index andalso
                            proplists:get_value(schema, IndexData) == SchemaName andalso
                            proplists:get_value(n_val, IndexData) == 3
                    end
                end, 20, 1000 ),
                riakc_test_utils:wait_until( fun() ->
                    case riakc_pb_socket:get_search_schema(Pid, SchemaName) of
                        {ok, SchemaData} ->
                            proplists:get_value(name, SchemaData) == SchemaName andalso
                            proplists:get_value(content, SchemaData) == Schema;
                        {error, <<"notefound">>} ->
                            false
                    end
                end, 20, 1000 )
         end)}},
     {"create a search index and tie to a bucket",
     {timeout, 30, ?_test(begin
                riakc_test_utils:reset_riak(),
                {ok, Pid} = riakc_test_utils:start_link(),
                Index = <<"myindex">>,
                Bucket = <<"mybucket">>,
                ?assertEqual(ok, riakc_pb_socket:create_search_index(Pid, Index)),
                ok = riakc_pb_socket:set_search_index(Pid, Bucket, Index),
                PO = riakc_obj:new(Bucket, <<"fred">>, <<"{\"name_s\":\"Freddy\"}">>, "application/json"),
                {ok, _Obj} = riakc_pb_socket:put(Pid, PO, [return_head]),
                riakc_test_utils:wait_until( fun() ->
                    {ok, Result} = riakc_pb_socket:search(Pid, Index, <<"*:*">>),
                    1 == Result#search_results.num_found
                end, 20, 1000 )
         end)}},
     {"search utf8",
     {timeout, 30, ?_test(begin
                riakc_test_utils:reset_riak(),
                {ok, Pid} = riakc_test_utils:start_link(),
                riakc_test_utils:reset_solr(Pid),
                Index = <<"myindex">>,
                Bucket = <<"mybucket">>,
                ?assertEqual(ok, riakc_pb_socket:create_search_index(Pid, Index)),
                ok = riakc_pb_socket:set_search_index(Pid, Bucket, Index),
                PO = riakc_obj:new(Bucket, <<"fred">>, <<"{\"name_s\":\"×Ö¸Ö¼×¨Ö¸×\"}"/utf8>>, "application/json"),
                {ok, _Obj} = riakc_pb_socket:put(Pid, PO, [return_head]),
                riakc_test_utils:wait_until( fun() ->
                    {ok, Result} = riakc_pb_socket:search(Pid, Index, <<"name_s:×Ö¸Ö¼×¨Ö¸×"/utf8>>),
                    1 == Result#search_results.num_found
                end )
         end)}},
     {"trivial set delete",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:add_element(<<"X">>, riakc_set:new()))),
                    {ok, S0} = riakc_pb_socket:fetch_type(Pid, {<<"sets">>, <<"bucket">>}, <<"key">>),
                    ?assert(riakc_set:is_element(<<"X">>, S0)),
                    ?assertEqual(riakc_set:size(S0), 1),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:del_element(<<"X">>, S0))),
                    {ok, S1} = riakc_pb_socket:fetch_type(Pid, {<<"sets">>, <<"bucket">>}, <<"key">>),
                    ?assertNot(riakc_set:is_element(<<"X">>, S1)),
                    ?assertEqual(riakc_set:size(S1), 0)
             end)},
     {"add and remove items in nested set in map",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"maps">>, <<"bucket">>}, <<"key">>,
                                     riakc_map:to_op(riakc_map:update({<<"set">>, set},
                                                                      fun(S) ->
                                                                              riakc_set:add_element(<<"X">>,
                                                                                                    riakc_set:add_element(<<"Y">>, S))
                                                                      end, riakc_map:new()))),
                    {ok, M0} = riakc_pb_socket:fetch_type(Pid, {<<"maps">>, <<"bucket">>}, <<"key">>),
                    L0 = riakc_map:fetch({<<"set">>, set}, M0),
                    ?assert(lists:member(<<"X">>, L0)),
                    ?assert(lists:member(<<"Y">>, L0)),
                    ?assertEqual(length(L0), 2),

                    M1 = riakc_map:update({<<"set">>, set},
                                          fun(S) -> riakc_set:del_element(<<"X">>,
                                                                          riakc_set:add_element(<<"Z">>, S)) end,
                                          M0),

                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"maps">>, <<"bucket">>}, <<"key">>,
                                     riakc_map:to_op(M1)),
                    {ok, M2} = riakc_pb_socket:fetch_type(Pid, {<<"maps">>, <<"bucket">>}, <<"key">>),
                    L1 = riakc_map:fetch({<<"set">>, set}, M2),

                    ?assert(lists:member(<<"Y">>, L1)),
                    ?assert(lists:member(<<"Z">>, L1)),
                    ?assertEqual(length(L1), 2)
             end)},
     {"increment nested counter",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"maps">>, <<"bucket">>}, <<"key">>,
                                     riakc_map:to_op(riakc_map:update({<<"counter">>, counter},
                                                                      fun(C) ->
                                                                              riakc_counter:increment(5, C)
                                                                      end, riakc_map:new()))),
                    {ok, M0} = riakc_pb_socket:fetch_type(Pid, {<<"maps">>, <<"bucket">>}, <<"key">>),
                    C0 = riakc_map:fetch({<<"counter">>, counter}, M0),
                    ?assertEqual(C0, 5),

                    M1 = riakc_map:update({<<"counter">>, counter},
                                          fun(C) -> riakc_counter:increment(200, C) end,
                                          M0),
                    M2 = riakc_map:update({<<"counter">>, counter},
                                          fun(C) -> riakc_counter:decrement(117, C) end,
                                          M1),
                    M3 = riakc_map:update({<<"counter">>, counter},
                                          fun(C) -> riakc_counter:increment(256, C) end,
                                          M2),

                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"maps">>, <<"bucket">>}, <<"key">>,
                                     riakc_map:to_op(M3)),
                    {ok, M4} = riakc_pb_socket:fetch_type(Pid, {<<"maps">>, <<"bucket">>}, <<"key">>),
                    C1 = riakc_map:fetch({<<"counter">>, counter}, M4),
                    ?assertEqual(C1, 344)
             end)},
     {"updated nested lww register",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    %% The word "stone" translated into Russian and Thai
                    StoneInRussian = [1051,1102,1082,32,1082,1072,1084,1085,1077,1091,1083,1086,
                                      1074,1080,1090,1077,1083,1103],
                    StoneInThai = [3627,3636,3609],
                    {ok, Pid} = riakc_test_utils:start_link(),
                    ok = riakc_pb_socket:update_type(Pid,
                                                     {<<"maps">>, <<"bucket">>},
                                                     <<"key">>,
                                     riakc_map:to_op(
                                       riakc_map:update(
                                       {<<"register">>, register},
                                       fun(R) ->
                                               riakc_register:set(
                                                 term_to_binary({"barney", "rubble", StoneInRussian}),
                                                 R)
                                       end, riakc_map:new()))),
                    {ok, M0} = riakc_pb_socket:fetch_type(Pid, {<<"maps">>, <<"bucket">>}, <<"key">>),
                    R0 = riakc_map:fetch({<<"register">>, register}, M0),
                    ?assertEqual(binary_to_term(R0), {"barney", "rubble", StoneInRussian}),

                    ok = riakc_pb_socket:update_type(Pid,
                                                     {<<"maps">>, <<"bucket">>},
                                                     <<"key">>,
                                     riakc_map:to_op(
                                       riakc_map:update(
                                       {<<"register">>, register},
                                       fun(R) ->
                                               riakc_register:set(
                                                 term_to_binary({"barney", "rubble", StoneInThai}),
                                                 R)
                                       end, M0))),

                    {ok, M1} = riakc_pb_socket:fetch_type(Pid, {<<"maps">>, <<"bucket">>}, <<"key">>),
                    R1 = riakc_map:fetch({<<"register">>, register}, M1),
                    ?assertEqual(binary_to_term(R1), {"barney", "rubble", StoneInThai})
             end)},
     {"throw exception for undefined context for delete",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    ?assertThrow(context_required, riakc_set:del_element(<<"X">>,
                                                                         riakc_set:add_element(<<"X">>,
                                                                                               riakc_set:new()))),
                    ?assertThrow(context_required, riakc_map:erase({<<"counter">>, counter}, riakc_map:new())),
                    ?assertThrow(context_required, riakc_map:erase({<<"set">>, set}, riakc_map:new())),
                    ?assertThrow(context_required, riakc_map:erase({<<"map">>, map}, riakc_map:new())),
                    ?assertThrow(context_required, riakc_map:update({<<"set">>, set}, fun(S) -> riakc_set:del_element(<<"Y">>, S) end, riakc_map:new())),
                    ?assertThrow(context_required, riakc_flag:disable(riakc_flag:new()))
             end)},
     {"delete bogus item from set",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:add_element(<<"X">>, riakc_set:new()))),
                    {ok, S0} = riakc_pb_socket:fetch_type(Pid, {<<"sets">>, <<"bucket">>}, <<"key">>),
                    ?assert(riakc_set:is_element(<<"X">>, S0)),
                    ?assertEqual(riakc_set:size(S0), 1),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:del_element(<<"Y">>, S0))),
                    {ok, S1} = riakc_pb_socket:fetch_type(Pid, {<<"sets">>, <<"bucket">>}, <<"key">>),
                    ?assert(riakc_set:is_element(<<"X">>, S1)),
                    ?assertEqual(riakc_set:size(S1), 1)
             end)},
     {"add redundant item to set",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:add_element(<<"X">>, riakc_set:new()))),
                    {ok, S0} = riakc_pb_socket:fetch_type(Pid, {<<"sets">>, <<"bucket">>}, <<"key">>),
                    ?assert(riakc_set:is_element(<<"X">>, S0)),
                    ?assertEqual(riakc_set:size(S0), 1),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:add_element(<<"X">>, S0))),
                    {ok, S1} = riakc_pb_socket:fetch_type(Pid, {<<"sets">>, <<"bucket">>}, <<"key">>),
                    ?assert(riakc_set:is_element(<<"X">>, S1)),
                    ?assertEqual(riakc_set:size(S1), 1)
             end)},
     {"add and remove redundant item to/from set",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:add_element(<<"X">>,
                                                                           riakc_set:add_element(<<"Y">>, riakc_set:new())))),
                    {ok, S0} = riakc_pb_socket:fetch_type(Pid, {<<"sets">>, <<"bucket">>}, <<"key">>),
                    ?assert(riakc_set:is_element(<<"X">>, S0)),
                    ?assert(riakc_set:is_element(<<"Y">>, S0)),
                    ?assertEqual(riakc_set:size(S0), 2),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:del_element(<<"X">>, riakc_set:add_element(<<"X">>, S0)))),
                    {ok, S1} = riakc_pb_socket:fetch_type(Pid, {<<"sets">>, <<"bucket">>}, <<"key">>),
                    ?assert(riakc_set:is_element(<<"X">>, S1)),
                    ?assert(riakc_set:is_element(<<"Y">>, S1)),
                    ?assertEqual(riakc_set:size(S1), 2)
             end)},
     {"remove then add redundant item from/to set",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:add_element(<<"X">>,
                                                                           riakc_set:add_element(<<"Y">>, riakc_set:new())))),
                    {ok, S0} = riakc_pb_socket:fetch_type(Pid, {<<"sets">>, <<"bucket">>}, <<"key">>),
                    ?assert(riakc_set:is_element(<<"X">>, S0)),
                    ?assert(riakc_set:is_element(<<"Y">>, S0)),
                    ?assertEqual(riakc_set:size(S0), 2),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:add_element(<<"X">>, riakc_set:del_element(<<"X">>, S0)))),
                    {ok, S1} = riakc_pb_socket:fetch_type(Pid, {<<"sets">>, <<"bucket">>}, <<"key">>),
                    ?assert(riakc_set:is_element(<<"X">>, S1)),
                    ?assert(riakc_set:is_element(<<"Y">>, S1)),
                    ?assertEqual(riakc_set:size(S1), 2)
             end)},
     {"remove item from set with outdated context",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:add_element(<<"X">>,
                                                                           riakc_set:add_element(<<"Y">>, riakc_set:new())))),
                    {ok, S0} = riakc_pb_socket:fetch_type(Pid, {<<"sets">>, <<"bucket">>}, <<"key">>),
                    ?assert(riakc_set:is_element(<<"X">>, S0)),
                    ?assert(riakc_set:is_element(<<"Y">>, S0)),
                    ?assertEqual(riakc_set:size(S0), 2),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:add_element(<<"Z">>, riakc_set:new()))),

                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"sets">>, <<"bucket">>}, <<"key">>,
                                     riakc_set:to_op(riakc_set:del_element(<<"Z">>, S0))),
                    {ok, S1} = riakc_pb_socket:fetch_type(Pid, {<<"sets">>, <<"bucket">>}, <<"key">>),
                    ?assert(riakc_set:is_element(<<"X">>, S1)),
                    ?assert(riakc_set:is_element(<<"Y">>, S1)),
                    ?assert(riakc_set:is_element(<<"Z">>, S1)),
                    ?assertEqual(riakc_set:size(S1), 3)
             end)},
     {"add item to nested set in map while also removing set",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"maps">>, <<"bucket">>}, <<"key">>,
                                     riakc_map:to_op(riakc_map:update({<<"set">>, set},
                                                                      fun(S) ->
                                                                              riakc_set:add_element(<<"X">>,
                                                                                                    riakc_set:add_element(<<"Y">>, S))
                                                                      end, riakc_map:new()))),
                    {ok, M0} = riakc_pb_socket:fetch_type(Pid, {<<"maps">>, <<"bucket">>}, <<"key">>),
                    L0 = riakc_map:fetch({<<"set">>, set}, M0),
                    ?assert(lists:member(<<"X">>, L0)),
                    ?assert(lists:member(<<"Y">>, L0)),
                    ?assertEqual(length(L0), 2),

                    M1 = riakc_map:update({<<"set">>, set},
                                          fun(S) -> riakc_set:add_element(<<"Z">>, S) end,
                                          M0),
                    M2 = riakc_map:erase({<<"set">>, set}, M1),

                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"maps">>, <<"bucket">>}, <<"key">>,
                                     riakc_map:to_op(M2)),
                    {ok, M3} = riakc_pb_socket:fetch_type(Pid, {<<"maps">>, <<"bucket">>}, <<"key">>),
                    L1 = riakc_map:fetch({<<"set">>, set}, M3),

                    ?assert(lists:member(<<"Z">>, L1)),
                    ?assertEqual(length(L1), 1)
             end)},
     {"increment nested counter in map while also removing counter",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"maps">>, <<"bucket">>}, <<"key">>,
                                     riakc_map:to_op(riakc_map:update({<<"counter">>, counter},
                                                                      fun(C) ->
                                                                              riakc_counter:increment(5, C)
                                                                      end, riakc_map:new()))),
                    {ok, M0} = riakc_pb_socket:fetch_type(Pid, {<<"maps">>, <<"bucket">>}, <<"key">>),
                    C0 = riakc_map:fetch({<<"counter">>, counter}, M0),
                    ?assertEqual(C0, 5),

                    M1 = riakc_map:update({<<"counter">>, counter},
                                          fun(C) -> riakc_counter:increment(2, C) end,
                                          M0),
                    M2 = riakc_map:erase({<<"counter">>, counter}, M1),

                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"maps">>, <<"bucket">>}, <<"key">>,
                                     riakc_map:to_op(M2)),
                    {ok, M3} = riakc_pb_socket:fetch_type(Pid, {<<"maps">>, <<"bucket">>}, <<"key">>),
                    C1 = riakc_map:fetch({<<"counter">>, counter}, M3),

                    %% Expected result depends on combination of vnodes involved, so accept either answer
                    ?assert(C1 =:= 2 orelse C1 =:= 7)
             end)},
     {"add item to nested set in nested map in map while also removing nested map",
         ?_test(begin
                    riakc_test_utils:reset_riak(),
                    {ok, Pid} = riakc_test_utils:start_link(),
                    M0 = riakc_map:update({<<"map">>, map},
                                          fun(M) ->
                                                  riakc_map:update({<<"set">>, set},
                                                                   fun(S) ->
                                                                           riakc_set:add_element(<<"X">>,
                                                                                                 riakc_set:add_element(<<"Y">>, S))
                                                                   end,
                                                                   M)
                                          end,
                                          riakc_map:new()),
                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"maps">>, <<"bucket">>}, <<"key">>,
                                     riakc_map:to_op(M0)),

                    {ok, M1} = riakc_pb_socket:fetch_type(Pid, {<<"maps">>, <<"bucket">>}, <<"key">>),
                    L0 = orddict:fetch({<<"set">>, set}, riakc_map:fetch({<<"map">>, map}, M1)),

                    ?assert(lists:member(<<"X">>, L0)),
                    ?assert(lists:member(<<"Y">>, L0)),
                    ?assertEqual(length(L0), 2),

                    M2 = riakc_map:update({<<"map">>, map},
                                          fun(M) -> riakc_map:update({<<"set">>, set},
                                                                     fun(S) -> riakc_set:add_element(<<"Z">>, S) end,
                                                                     M)
                                          end,
                                          M1),
                    M3 = riakc_map:erase({<<"map">>, map}, M2),

                    ok = riakc_pb_socket:update_type(Pid,
                                     {<<"maps">>, <<"bucket">>}, <<"key">>,
                                     riakc_map:to_op(M3)),
                    {ok, M4} = riakc_pb_socket:fetch_type(Pid, {<<"maps">>, <<"bucket">>}, <<"key">>),
                    L1 = orddict:fetch({<<"set">>, set}, riakc_map:fetch({<<"map">>, map}, M4)),

                    ?assert(lists:member(<<"Z">>, L1)),
                    ?assertEqual(length(L1), 1)
                end)},
     {"get-preflist",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 Node = atom_to_binary(riakc_test_utils:test_riak_node(), latin1),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 {ok, ServerInfo} = riakc_pb_socket:get_server_info(Pid),
                 [{node, _}, {server_version, SV}] = lists:sort(ServerInfo),
                 Ver = binary_to_list(SV),
                 if Ver < "2.1" ->
                        ?debugFmt("preflists are not supported in version ~p", [Ver]);
                    true ->
                        {ok, Preflist} = riakc_pb_socket:get_preflist(Pid, <<"b">>, <<"f">>),
                        ?assertEqual([#preflist_item{partition = 52,
                                                    node = Node,
                                                    primary = true},
                                    #preflist_item{partition = 53,
                                                    node = Node,
                                                    primary = true},
                                    #preflist_item{partition = 54,
                                                    node = Node,
                                                    primary = true}],
                                    Preflist)
                 end
             end)},
     {"add redundant and multiple items to hll(set)",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 HB = {<<"hlls">>, <<"bucket">>},
                 HK = <<"key">>,
                 case riakc_pb_socket:get_bucket(Pid, HB) of
                     {ok, _} ->
                         Hll0 = riakc_hll:new(),
                         HllOp0 = riakc_hll:to_op(riakc_hll:add_elements([<<"X">>, <<"Y">>], Hll0)),
                         ok = riakc_pb_socket:update_type(Pid, HB, HK, HllOp0),
                         {ok, Hll1} = riakc_pb_socket:fetch_type(Pid, HB, HK),
                         HllOp1 = riakc_hll:to_op(riakc_hll:add_element(<<"X">>, Hll1)),
                         ok = riakc_pb_socket:update_type(Pid, HB, HK, HllOp1),
                         {ok, Hll2} = riakc_pb_socket:fetch_type(Pid, HB, HK),
                         ?assertEqual(riakc_hll:value(Hll1), 2),
                         ?assert(riakc_hll:is_type(Hll2)),
                         Value = riakc_hll:value(Hll2),
                         ?assertEqual(Value, 2),
                         %% Make sure card and value are the same
                         ?assertEqual(riakc_hll:card(Hll2), Value);
                     Rsp ->
                         ?debugFmt("hlls bucket is not present, skipping (~p)", [Rsp])
                 end
             end)},
     {"add item to gset, twice",
      ?_test(begin
                 riakc_test_utils:reset_riak(),
                 {ok, Pid} = riakc_test_utils:start_link(),
                 B = {<<"gsets">>, <<"bucket">>},
                 K = <<"key">>,
                 case riakc_pb_socket:get_bucket(Pid, B) of
                     {ok, _} ->
                        GSet0 = riakc_gset:new(),
                        GSetOp0 = riakc_gset:to_op(riakc_gset:add_element(<<"X">>, GSet0)),
                        ok = riakc_pb_socket:update_type(Pid, B, K, GSetOp0),
                        {ok, S0} = riakc_pb_socket:fetch_type(Pid, B, K),
                        ?assert(riakc_gset:is_element(<<"X">>, S0)),
                        ?assertEqual(riakc_gset:size(S0), 1),
                        GSetOp1 = riakc_gset:to_op(riakc_gset:add_element(<<"X">>, S0)),
                        ok = riakc_pb_socket:update_type(Pid, B, K, GSetOp1),
                        {ok, S1} = riakc_pb_socket:fetch_type(Pid, B, K),
                        ?assert(riakc_gset:is_element(<<"X">>, S1)),
                        ?assertEqual(riakc_gset:size(S1), 1);
                     Rsp ->
                         ?debugFmt("gsets bucket is not present, skipping (~p)", [Rsp])
                 end
             end)}
     ].

integration_test_() ->
    SetupFun = fun() ->
                   %% Grab the riakclient_pb.proto file
                   code:add_pathz("../ebin"),
                   ok = riakc_test_utils:maybe_start_network()
               end,
    CleanupFun = fun(_) -> net_kernel:stop() end,
    GenFun = fun() ->
                 case catch net_adm:ping(riakc_test_utils:test_riak_node()) of
                     pong -> integration_tests();
                     _ ->
                         ?debugMsg("Skipped - needs live server"),
                         []
                 end
             end,
    {setup, SetupFun, CleanupFun, {generator, GenFun}}.
-endif.
