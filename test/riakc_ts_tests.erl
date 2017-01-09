%% -------------------------------------------------------------------
%%
%% riakc_ts_tests: timeseries client tests
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riakc_ts_tests).

-compile(export_all).

-include("riakc.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(TABLE, <<"GeoCheckin">>).
-define(TWENTY_MINS_AGO,  1443795700987).
-define(FIFTEEN_MINS_AGO, 1443796000987).
-define(TEN_MINS_AGO,     1443796300987).
-define(FIVE_MINS_AGO,    1443796600987).
-define(NOW,              1443796900987).

integration_tests({ok, _Props}) ->
    [{"ping",
      ?_test(begin
                 {ok, Pid} = riakc_test_utils:start_link(),
                 ?assertEqual(pong, riakc_pb_socket:ping(Pid)),
                 ?assertEqual(true, riakc_pb_socket:is_connected(Pid)),
                 riakc_pb_socket:stop(Pid)
             end)},
     {"put-get",
      ?_test(begin
                 {ok, Pid} = riakc_test_utils:start_link(),
                 Data = [{<<"hash1">>, <<"user2">>, ?TWENTY_MINS_AGO, <<"hurricane">>, 82.3},
                         {<<"hash1">>, <<"user2">>, ?FIFTEEN_MINS_AGO, <<"rain">>, 79.0},
                         {<<"hash1">>, <<"user2">>, ?FIVE_MINS_AGO, <<"wind">>, []},
                         {<<"hash1">>, <<"user2">>, ?NOW, <<"snow">>, 20.1}],
                 ok = riakc_ts:put(Pid, ?TABLE, Data),
                 Key = [<<"hash1">>, <<"user2">>, ?FIVE_MINS_AGO],
                 {ok, {_C, _R}} = riakc_ts:get(Pid, ?TABLE, Key, []),
                 riakc_pb_socket:stop(Pid)
             end)},
     {"query-describe",
      ?_test(begin
                 {ok, Pid} = riakc_test_utils:start_link(),
                 {ok, {ColumnNames, _Rows}} = riakc_ts:'query'(Pid, <<"DESCRIBE GeoCheckin">>),
                 ?assert(length(ColumnNames) >= 5),
                 riakc_pb_socket:stop(Pid)
             end)},
     {"list-keys",
      ?_test(begin
                 {ok, Pid} = riakc_test_utils:start_link(),
                 {ok, ReqId} = riakc_ts:stream_list_keys(Pid, ?TABLE),
                 {ok, Keys} = riakc_utils:wait_for_list(ReqId),
                 ?assert(length(Keys) > 0),
                 riakc_pb_socket:stop(Pid)
             end)}
     ];
integration_tests(Error) ->
    ?debugFmt("~s table does not exist: ~p", [?TABLE, Error]),
    [].

generate_integration_tests() ->
    {ok, Pid} = riakc_test_utils:start_link(),
    Tests = integration_tests(riakc_pb_socket:get_bucket_type(Pid, ?TABLE)),
    riakc_pb_socket:stop(Pid),
    Tests.

integration_test_() ->
    SetupFun = fun() ->
                   %% Grab the riakclient_pb.proto file
                   code:add_pathz("../ebin"),
                   ok = riakc_test_utils:maybe_start_network()
               end,
    CleanupFun = fun(_) -> net_kernel:stop() end,
    GenFun = fun() ->
                 case catch net_adm:ping(riakc_test_utils:test_riak_node()) of
                     pong -> generate_integration_tests();
                     _ -> []
                 end
             end,
    {setup, SetupFun, CleanupFun, {generator, GenFun}}.
-endif.
