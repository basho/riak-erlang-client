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

-module(riakc_test_utils).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

%% Get the test host - check env RIAK_TEST_PB_HOST then env 'RIAK_TEST_HOST_1'
%% falling back to 127.0.0.1
test_ip() ->
    case os:getenv("RIAK_TEST_PB_HOST") of
        false ->
            case os:getenv("RIAK_TEST_HOST_1") of
                false ->
                    "127.0.0.1";
                Host ->
                    Host
            end;
        Host ->
            Host
    end.

%% Test port - check env RIAK_TEST_PBC_1
test_port() ->
    case os:getenv("RIAK_TEST_PBC_1") of
        false ->
            8087;
        PortStr ->
            list_to_integer(PortStr)
    end.

%% Riak node under test - used to setup/configure/tweak it for tests
test_riak_node() ->
    case os:getenv("RIAK_TEST_NODE_1") of
        false ->
            'riak@127.0.0.1';
        NodeStr ->
            list_to_atom(NodeStr)
    end.

%% Node for the eunit node for distributed erlang
test_eunit_node() ->
    case os:getenv("RIAK_EUNIT_NODE") of
        false ->
            'eunit@127.0.0.1';
        EunitNodeStr ->
            list_to_atom(EunitNodeStr)
    end.

%% Cookie for distributed erlang
test_cookie() ->
    case os:getenv("RIAK_TEST_COOKIE") of
        false ->
            'riak';
        CookieStr ->
            list_to_atom(CookieStr)
    end.

start_link() ->
    riakc_pb_socket:start_link(test_ip(), test_port()).

start_link(Opts) ->
    riakc_pb_socket:start_link(test_ip(), test_port(), Opts).

%% Get the riak version from the init boot script, turn it into a list
%% of integers.
riak_version() ->
    StrVersion = element(2, rpc:call(test_riak_node(), init, script_id, [])),
    {match, [Major, Minor, Patch|_]} = re:run(StrVersion, "\\d+", [global, {capture, first, list}]),
    [ list_to_integer(V) || [V] <- [Major, Minor, Patch]].

%% Compare the first three part version array with the second.
%% returns `greater', `less', or `equal'.
compare_versions([M1,N1,P1], [M2,N2,P2]) ->
    V1 = (M1*1000000)+(N1*1000)+(P1),
    V2 = (M2*1000000)+(N2*1000)+(P2),
    case {V1 > V2, V1 == V2} of
        {true,_} ->
            greater;
        {false,false} ->
            less;
        {false,true} ->
            equal
    end.

%% Retry `Fun' until it returns `Retry' times, waiting `Delay'
%% milliseconds between retries. This is our eventual consistency bread
%% and butter
wait_until(Fun) when is_function(Fun) ->
    wait_until(Fun, 20, 500).
wait_until(_, 0, _) ->
    fail;
wait_until(Fun, Retry, Delay) when Retry > 0 ->
    Pass = Fun(),
    case Pass of
        true ->
            ok;
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Retry-1, Delay)
    end.

%% Resets the riak node
reset_riak() ->
    %% sleep because otherwise we're going to kill the vnodes too fast
    %% for the supervisor's maximum restart frequency, which will bring
    %% down the entire node
    ?assertEqual(ok, maybe_start_network()),
    case compare_versions(riak_version(), [1,2,0]) of
        less ->
            reset_riak_legacy();
        _ ->
            reset_riak_12()
    end.

reset_solr(Pid) ->
    %% clear indexes
    {ok, Indexes} = riakc_pb_socket:list_search_indexes(Pid),
    [ riakc_pb_socket:delete_search_index(Pid, proplists:get_value(index,Index)) || Index <- Indexes ],
    wait_until( fun() ->
        {ok, []} == riakc_pb_socket:list_search_indexes(Pid)
    end, 20, 1000),
    ok.

%% Resets a Riak 1.2+ node, which can run the memory backend in 'test'
%% mode.
reset_riak_12() ->
    set_test_backend(),
    ok = rpc:call(test_riak_node(), riak_kv_memory_backend, reset, []),
    reset_ring().

%% Sets up the memory/test backend, leaving it alone if already set properly.
set_test_backend() ->
    Env = rpc:call(test_riak_node(), application, get_all_env, [riak_kv]),
    Backend = proplists:get_value(storage_backend, Env),
    Test = proplists:get_value(test, Env),
    case {Backend, Test} of
        {riak_kv_memory_backend, true} ->
            ok;
        _ ->
            ok = rpc:call(test_riak_node(), application, set_env, [riak_kv, storage_backend, riak_kv_memory_backend]),
            ok = rpc:call(test_riak_node(), application, set_env, [riak_kv, test, true]),
            Vnodes = rpc:call(test_riak_node(), riak_core_vnode_manager, all_vnodes, [riak_kv_vnode]),
            [ ok = rpc:call(test_riak_node(), supervisor, terminate_child, [riak_core_vnode_sup, Pid]) ||
                {_, _, Pid} <- Vnodes ]
    end.

%% Resets a Riak 1.1 and earlier node.
reset_riak_legacy() ->
    timer:sleep(500),
    %% Until there is a good way to empty the vnodes, require the
    %% test to run with ETS and kill the vnode master/sup to empty all the ETS tables
    %% and the ring manager to remove any bucket properties
    ok = rpc:call(test_riak_node(), application, set_env, [riak_kv, storage_backend, riak_kv_memory_backend]),

    %% Restart the vnodes so they come up with ETS
    ok = supervisor:terminate_child({riak_kv_sup, test_riak_node()}, riak_kv_vnode_master),
    ok = supervisor:terminate_child({riak_core_sup, test_riak_node()}, riak_core_vnode_sup),
    {ok, _} = supervisor:restart_child({riak_core_sup, test_riak_node()}, riak_core_vnode_sup),
    {ok, _} = supervisor:restart_child({riak_kv_sup, test_riak_node()}, riak_kv_vnode_master),

    %% Clear the MapReduce cache
    ok = rpc:call(test_riak_node(), riak_kv_mapred_cache, clear, []),

    %% Now reset the ring so bucket properties are default
    reset_ring().

%% Resets the ring to a fresh one, effectively deleting any bucket properties.
reset_ring() ->
    Ring = rpc:call(test_riak_node(), riak_core_ring, fresh, []),
    ok = rpc:call(test_riak_node(), riak_core_ring_manager, set_my_ring, [Ring]).


%% Finds the pid of the PB listener process
riak_pb_listener_pid() ->
    {Children, Proc} = case compare_versions(riak_version(), [1,2,0]) of
                            less ->
                               {supervisor:which_children({riak_kv_sup, test_riak_node()}),
                                riak_kv_pb_listener};
                            _ ->
                               {supervisor:which_children({riak_api_sup, test_riak_node()}),
                                riak_api_pb_listener}
                        end,
    hd([Pid || {_,Pid,_,[Mod]} <- Children, Mod == Proc]).

pause_riak_pb_listener() ->
    Pid = riak_pb_listener_pid(),
    rpc:call(test_riak_node(), sys, suspend, [Pid]).

resume_riak_pb_listener() ->
    Pid = riak_pb_listener_pid(),
    rpc:call(test_riak_node(), sys, resume, [Pid]).

kill_riak_pb_sockets() ->
    Children = case compare_versions(riak_version(), [1,2,0]) of
                   less ->
                       supervisor:which_children({riak_kv_pb_socket_sup, test_riak_node()});
                   _ ->
                       supervisor:which_children({riak_api_pb_sup, test_riak_node()})
               end,
    case Children of
        [] ->
            ok;
        [_|_] ->
            Pids = [Pid || {_,Pid,_,_} <- Children],
            [rpc:call(test_riak_node(), erlang, exit, [Pid, kill]) || Pid <- Pids],
            erlang:yield(),
            kill_riak_pb_sockets()
    end.

maybe_start_network() ->
    %% Try to spin up net_kernel
    os:cmd("epmd -daemon"),
    case net_kernel:start([test_eunit_node(), longnames]) of
        {ok, _} ->
            erlang:set_cookie(test_riak_node(), test_cookie()),
            ok;
        {error, {already_started, _}} ->
            ok;
        X ->
            X
    end.

-endif.
