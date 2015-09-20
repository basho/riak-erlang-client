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
-include_lib("riak_pb/include/riak_kv_pb.hrl").

-export([serialize/3,
         deserialize/1]).

-ifndef(SINT64_MIN).
-define(SINT64_MIN, -16#8000000000000000).
-endif.
-ifndef(SINT64_MAX).
-define(SINT64_MAX,  16#7FFFFFFFFFFFFFFF).
-endif.

serialize(TableName, Columns, Measurements) ->
    SerializedColumns = columns_for(Columns),
    SerializedRows = rows_for(Measurements),
    #tsputreq{table = TableName,
              columns = SerializedColumns,
              rows = SerializedRows}.

deserialize(Response) ->
    Response.

%% TODO: actually support column specifiers
columns_for(_Columns) ->
    undefined.

rows_for(Measurements) ->
    rows_for(Measurements, []).

rows_for([], SerializedMeasurements) ->
    SerializedMeasurements;
rows_for([MeasureRow|RemainingMeasures], SerializedMeasurements) ->
    SerializedRow = row_for(MeasureRow),
    rows_for(RemainingMeasures, [SerializedRow | SerializedMeasurements]).

row_for(MeasureRow) ->
    row_for(MeasureRow, []).

row_for([], SerializedCells) ->
    #tsrow{cells = lists:reverse(SerializedCells)};
row_for([Cell|RemainingCells], SerializedCells) ->
    row_for(RemainingCells,
            [cell_for(Cell) | SerializedCells]).

cell_for(Measure) when is_binary(Measure) ->
    #tscell{binary_value = Measure};
cell_for(Measure) when is_integer(Measure),
                       (?SINT64_MIN =< Measure),
                       (Measure =< ?SINT64_MAX)  ->
    #tscell{integer_value = Measure};
cell_for(Measure) when is_integer(Measure) ->
    #tscell{numeric_value = integer_to_list(Measure)};
cell_for(Measure) when is_float(Measure) ->
    #tscell{numeric_value = float_to_list(Measure)};
cell_for({time, Measure}) ->
    #tscell{timestamp_value = Measure};
cell_for(true) ->
    #tscell{boolean_value = true};
cell_for(false) ->
    #tscell{boolean_value = false}.
