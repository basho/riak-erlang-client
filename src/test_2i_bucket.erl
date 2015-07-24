-module(test_2i_bucket).
-export([read_bucket/4, total_value_size/3]).

read_bucket(Ip, Port, Bucket, Opts) ->
    {ok, Pid} = riakc_pb_socket:start_link(Ip, Port, [{auto_reconnect, true}]),
    {Time, {TotalCount, TotalSize}} = timer:tc(?MODULE, total_value_size, [Pid, Bucket, Opts]),
    Megs = TotalSize / (1024 * 1024),
    Secs = Time / 1000000,
    MbSecs = Megs / Secs,
    io:format("Read ~.2f Mb in ~p seconds (~.2f Mb/Sec, ~p objects)\n",
              [Megs, Secs, MbSecs, TotalCount]).

total_value_size(Pid, Bucket, Opts)->
    {ok, Ref} = riakc_pb_socket:cs_bucket_fold(Pid, Bucket, Opts),
    read_stream(Ref, {0, 0}).

read_stream(Ref, {Count, Size}) ->
    receive
        {Ref, {ok, Objects}} ->
            {Count1, Size1} = count_bytes(Objects, {Count, Size}),
            case (Size div 10000000) /= (Size1 div 10000000) of
                true ->
                    io:format("~.2f Mb (~p objects)\n",
                              [Size1 / (1024 * 1024), Count1]);
                false ->
                    ok
            end,
            read_stream(Ref, {Count1, Size1});
        {Ref, {done, _}} ->
            {Count, Size}
    end.

count_bytes([], Count) ->
    Count;
count_bytes([Obj|Rest], {Count, TSize}) ->
    Size = erlang:byte_size(riakc_obj:get_value(Obj)),
    count_bytes(Rest, {Count+1, TSize + Size}).
