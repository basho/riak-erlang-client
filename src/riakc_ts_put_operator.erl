%% -------------------------------------------------------------------
%%
%% riakc_ts_put_operator.erl: helper functions for put requests to Riak TS
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

%% @doc Helper functions for put requests to Riak TS

-module(riakc_ts_put_operator).

-include_lib("riak_pb/include/riak_pb.hrl").
-include_lib("riak_pb/include/riak_ts_pb.hrl").

-export([serialize/3,
         serialize_for_ttb/3,
         deserialize/1]).

serialize(TableName, ColumnNames, Measurements) ->
    ColumnDescs = riak_pb_ts_codec:encode_columnnames(ColumnNames),
    SerializedRows = riak_pb_ts_codec:encode_rows_non_strict(Measurements),
    #tsputreq{table   = TableName,
              columns = ColumnDescs,
              rows    = SerializedRows}.

serialize_for_ttb(TableName, ColumnNames, Measurements) ->
    
    ColumnDescs = riak_pb_ts_codec:encode_columnnames(ColumnNames),
    SerializedRows = riak_pb_ts_codec:encode_rows_for_ttb(Measurements),
        #tsttbputreq{table   = TableName,
                 columns = ColumnDescs,
                 rows    = SerializedRows}.

deserialize(Response) ->
    Response.
