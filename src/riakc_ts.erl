%% -------------------------------------------------------------------
%%
%% riakc_ts.erl: protobuf requests to Riak TS
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc protobuf requests to Riak TS

-module(riakc_ts).

-export([query/2,
         query/3,
         put/3,
         put/4,
         delete/4]).

-include_lib("riak_pb/include/riak_kv_pb.hrl").

-type table_name() :: binary().
-type ts_value() :: number() | binary().


-spec query(pid(), string()) ->
                   {[binary()], [tuple()]} | {error, term()}.
query(Pid, QueryText) ->
    query(Pid, QueryText, []).

-spec query(pid(), string(), [{binary(), binary()}]) ->
                   {[binary()], [tuple()]} | {error, term()}.
query(Pid, QueryText, Interpolations) ->
    Message = riakc_ts_query_operator:serialize(QueryText, Interpolations),
    Response = server_call(Pid, Message),
    riakc_ts_query_operator:deserialize(Response).


-spec put(pid(), table_name(), [{binary(), ts_value()}]) ->
                 ok | {error, term()}.
put(Pid, TableName, Measurements) ->
    put(Pid, TableName, [], Measurements).

-spec put(pid(), table_name(), [binary()], [{binary(), ts_value()}]) ->
                 ok | {error, term()}.
put(Pid, TableName, Columns, Measurements) ->
    Message = riakc_ts_put_operator:serialize(TableName, Columns, Measurements),
    Response = server_call(Pid, Message),
    riakc_ts_put_operator:deserialize(Response).


-spec delete(pid(), table_name(), [ts_value()], proplists:proplist()) ->
                    ok | {error, term()}.
delete(Pid, TableName, Key, Options)
  when is_list(Key) ->
    Message = riak_pb_ts_codec:encode_tsdelreq(TableName, Key, Options),
    _Response = server_call(Pid, Message).


%% --------------------------------------------
%% local functions

-spec server_call(pid(), tuple()) -> term().
server_call(Pid, Message) ->
    gen_server:call(Pid,
                    {req, Message, riakc_pb_socket:default_timeout(timeseries)},
                    infinity).
