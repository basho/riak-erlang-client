-module(riakc_ts_put_operator).

-include_lib("riak_pb/include/riak_pb.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").

-export([serialize/3]).

serialize(TableName, Columns, Measurements) ->
    SerializedColumns = columns_for(Columns),
    SerializedRows = rows_for(Measurements),
    #tsputreq{table = TableName,
              columns = SerializedColumns,
              rows = SerializedRows}.


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
            [cell_for(Cell), SerializedCells]).

cell_for(Measure) when is_binary(Measure) ->
    #tscell{binary_value = Measure};
cell_for(Measure) when is_integer(Measure) ->
    #tscell{integer_value = Measure};
cell_for(Measure) when is_float(Measure) ->
    #tscell{numeric_value = float_to_list(Measure)};
cell_for({time, Measure}) ->
    #tscell{timestamp_value = Measure};
cell_for(true) ->
    #tscell{boolean_value = true};
cell_for(false) ->
    #tscell{boolean_value = false}.
