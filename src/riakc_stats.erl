-module(riakc_stats).
-export([stats_format/1,
         init_stats/1,
         record_stat/3,
         record_cntr/2,
         merge_stats/2,
         print/1]).

-export([time/1,
         bytes/1]).

-record(stats, {timestamp, level, dict}).

record_cntr(_Key, #stats{level = 0} = Stats) ->
    Stats;
record_cntr(Key, #stats{dict = D} = Stats) ->
    Stats#stats{dict = dict:update_counter({count, Key}, 1, D)}.

record_stat(_Key, _Val, #stats{level = 0} = Stats) ->
    Stats;
record_stat(Key, Val, #stats{dict = Dict0, level = 1} = Stats) ->
    Dict1 = dict:update_counter({count, Key}, 1, Dict0),
    Stats#stats{dict = dict:update_counter({total, Key}, Val, Dict1)};
record_stat(Key, Val, #stats{dict = Dict0, level = 2} = Stats) ->
    Dict1 = dict:update_counter({count, Key}, 1, Dict0),
    Dict2 = dict:update_counter({total, Key}, Val, Dict1),
    Stats#stats{dict = dict:update_counter({histogram, granulate(Val), Key}, 1, Dict2)}.

init_stats(#stats{level = Level}) ->
    init_stats(Level);
init_stats(Level) ->
    #stats{timestamp = os:timestamp(), level = Level, dict = dict:new()}.

stats_format(#stats{timestamp = TS0, level = Level, dict = Dict}) ->
    TS1 = os:timestamp(),
    TDiff = timer:now_diff(TS1, TS0),
    {Cntrs, Hists} = stats_format(
                       Level, lists:sort(dict:to_list(Dict)), [],
                       [{key, count, total, lists:reverse(steps(Level))}]),
    {{TDiff, 1}, Cntrs, Hists}.

stats_format(_Level, [], CAcc, HAcc) -> {CAcc, HAcc};
stats_format(Level, [{{count, Key}, CVal} | List], CAcc, HAcc) ->
    case lists:keytake({total, Key}, 1, List) of
        false -> stats_format(Level, List, [{Key, CVal} | CAcc], HAcc);
        {value, {{total, Key}, TVal}, NewList} ->
            {OutList, Histogram} =
                lists:foldl(fun(I, {LAcc, XAcc}) ->
                                    case lists:keytake({histogram, I, Key}, 1, LAcc) of
                                        false -> {LAcc, [0 | XAcc]};
                                        {value, {{_, _, Key}, HVal}, LAcc2} -> {LAcc2, [HVal | XAcc]}
                                    end
                            end, {NewList, []}, steps(Level)),
            stats_format(Level, OutList, CAcc, [{Key, CVal, TVal, Histogram} | HAcc])
    end.

granulate(0)                     -> 0;
granulate(1)                     -> 1;
granulate(2)                     -> 2;
granulate(N) when N =< 4         -> 4;
granulate(N) when N =< 7         -> 7;
granulate(N) when N =< 10        -> 10;
granulate(N) when N =< 20        -> 20;
granulate(N) when N =< 40        -> 40;
granulate(N) when N =< 70        -> 70;
granulate(N) when N =< 100       -> 100;
granulate(N) when N =< 200       -> 200;
granulate(N) when N =< 400       -> 400;
granulate(N) when N =< 700       -> 700;
granulate(N) when N =< 1000      -> 1000;
granulate(N) when N =< 2000      -> 2000;
granulate(N) when N =< 4000      -> 4000;
granulate(N) when N =< 7000      -> 7000;
granulate(N) when N =< 10000     -> 10000;
granulate(N) when N =< 20000     -> 20000;
granulate(N) when N =< 40000     -> 40000;
granulate(N) when N =< 70000     -> 70000;
granulate(N) when N =< 100000    -> 100000;
granulate(N) when N =< 200000    -> 200000;
granulate(N) when N =< 400000    -> 400000;
granulate(N) when N =< 700000    -> 700000;
granulate(N) when N =< 1000000   -> 1000000;
granulate(N) when N =< 2000000   -> 2000000;
granulate(N) when N =< 4000000   -> 4000000;
granulate(N) when N =< 7000000   -> 7000000;
granulate(N) when N =< 10000000  -> 10000000;
granulate(N) when N =< 20000000  -> 20000000;
granulate(N) when N =< 40000000  -> 40000000;
granulate(N) when N =< 70000000  -> 70000000;
granulate(N) when N =< 100000000 -> 100000000;
granulate(N) when N =< 200000000 -> 200000000;
granulate(N) when N =< 400000000 -> 400000000;
granulate(N) when N =< 700000000 -> 700000000;
granulate(_)                     -> 1000000000.

steps(2) ->
    [1000000000,
     700000000, 400000000, 200000000, 100000000,
     70000000, 40000000, 20000000, 10000000,
     7000000, 4000000, 2000000, 1000000,
     700000, 400000, 200000, 100000,
     70000, 40000, 20000, 10000,
     7000, 4000, 2000, 1000,
     700, 400, 200, 100,
     70, 40, 20, 10,
     7, 4, 2, 1,
     0];
steps(_) -> [].

merge_stats({{TAcc1, TCnt1}, Cntrs1, HistG1}, {{TAcc2, TCnt2}, Cntrs2, HistG2}) ->
    {{TAcc1 + TAcc2, TCnt1 + TCnt2}, add_cntrs(Cntrs1, Cntrs2, []), add_hists(HistG1, HistG2, [])}.

add_cntrs([], [], Acc) -> Acc;
add_cntrs([], Cntrs2, Acc) -> lists:append([Cntrs2, Acc]);
add_cntrs(Cntrs1, [], Acc) -> lists:append([Cntrs1, Acc]);
add_cntrs([{Cntr, Val1} = Cntr1 | Cntrs1], Cntrs2, Acc) ->
    case lists:keytake(Cntr, 1, Cntrs2) of
        false ->
            add_cntrs(Cntrs1, Cntrs2, [Cntr1 | Acc]);
        {value, {Cntr, Val2}, NewCntrs2} ->
            add_cntrs(Cntrs1, NewCntrs2, [{Cntr, Val1 + Val2} | Acc])
    end.

add_hists([], [], Acc) -> Acc;
add_hists([], Hists2, Acc) -> lists:append([Hists2, Acc]);
add_hists(Hists1, [], Acc) -> lists:append([Hists1, Acc]);
add_hists([{key, _, _, _} = HistRec | Hists1], Hists2, Acc) ->
    add_hists(Hists1, lists:keydelete(key, 1, Hists2), [HistRec | Acc]);
add_hists([{Key, Cntr1, Ttl1, Hist1} = HistRec1 | Hists1], Hists2, Acc) ->
    case lists:keytake(Key, 1, Hists2) of
        false ->
            add_hists(Hists1, Hists2, [HistRec1 | Acc]);
        {value, {Key, Cntr2, Ttl2, Hist2}, NewHists2} ->
            add_hists(Hists1, NewHists2, [{Key, Cntr1 + Cntr2, Ttl1 + Ttl2, add_hist(Hist1, Hist2, [])} | Acc])
    end.

add_hist([], [], Acc) -> lists:reverse(Acc);
add_hist([H1 | Hist1], [H2 | Hist2], Acc) ->
    add_hist(Hist1, Hist2, [H1 + H2 | Acc]).

print({{Time, Conns}, Cntrs, Hists}) ->
    io:format(user,
              "~n~nRiak Connection Stats"
              "~n connections established: ~p"
              "~n stat monitoring period: " ++ time(Time div Conns) ++ "~n",
              [Conns]),
    lists:foreach(fun({Key, Cnt}) ->
                          print_cntr_key(Key, Cnt)
                  end, Cntrs),
    {value,{key,_,_,KeyDivs}, RestHists} =
        lists:keytake(key, 1, Hists),
    lists:foreach(fun({Key, Cnt, Ttl, Divs}) ->
                          TypeFunc = print_hist_key(Key, Cnt, Ttl),
                          print_hist(TypeFunc, 0, Divs, KeyDivs)
                  end, RestHists),
    io:format(user, "~n~n", []).

print_hist_key(send, Cnt, Ttl) ->
    io:format(user,
              "~n~n data sent: " ++ bytes(Ttl) ++
              "~n packets: ~p"
              "~n average packet size: " ++ bytes(Ttl div Cnt),
              [Cnt]),
    bytes;
print_hist_key(recv, Cnt, Ttl) ->
    io:format(user,
              "~n~n data received: " ++ bytes(Ttl) ++
              "~n packets: ~p"
              "~n average packet size: " ++ bytes(Ttl div Cnt),
              [Cnt]),
    bytes;
print_hist_key(connect, Cnt, Ttl) ->
    io:format(user,
              "~n~n reconnections: ~p"
              "~n average reconnect time: " ++ time(Ttl div Cnt),
              [Cnt]),
    time;
print_hist_key({service_time, OpTimeout, OpType, BucketName}, Cnt, Ttl) ->
    io:format(user,
              "~n~n ~p ~p operations on bucket: ~p"
              "~n with timeout: ~p"
              "~n average service time: " ++ time(Ttl div Cnt),
              [Cnt, OpType, BucketName, OpTimeout]),
    time;
print_hist_key({queue_time, QTimeout}, Cnt, Ttl) ->
    io:format(user,
              "~n~n ~p requests got queued with timeout: ~p"
              "~n average queueing time: " ++ time(Ttl div Cnt),
              [Cnt, QTimeout]),
    time.

time(Val) when Val < 1000 ->
    integer_to_list(Val) ++ " us";
time(Val) when Val < 1000000 ->
    integer_to_list(Val div 1000) ++ " ms";
time(Val) ->
    integer_to_list(Val div 1000000) ++ " s".

bytes(Val) when Val < 1000 ->
    integer_to_list(Val) ++ " B";
bytes(Val) when Val < 1000000 ->
    integer_to_list(Val div 1000) ++ " KB";
bytes(Val) when Val < 1000000000 ->
    integer_to_list(Val div 1000000) ++ " MB";
bytes(Val) ->
    integer_to_list(Val div 1000000000) ++ " GB".


print_hist(_TypeFunc, _PredDiv, [], []) -> ok;
print_hist(TypeFunc, _PredDiv, [0|Divs], [D|KeyDivs]) ->
    print_hist(TypeFunc, D, Divs, KeyDivs);
print_hist(TypeFunc, PredDiv, [N|Divs], [D|KeyDivs]) ->
    io:format(user,
              "~n  ~p between " ++ apply(?MODULE, TypeFunc, [PredDiv])
              ++ " and " ++ apply(?MODULE, TypeFunc, [D]), [N]),
    print_hist(TypeFunc, D, Divs, KeyDivs).

print_cntr_key({TimeoutTag, Timeout, OPType, BucketName}, Cnt)
  when TimeoutTag == op_timeout; TimeoutTag == req_timeout ->
    io:format(user, "~n ~p ~p requests on bucket: ~p timed out while being serviced, timeout: ~p.~n",
              [Cnt, OPType, BucketName, Timeout]);
print_cntr_key({TimeoutTag, Timeout, BucketName}, Cnt)
  when TimeoutTag == q_timeout; TimeoutTag == req_timeout ->
    io:format(user,
              "~n ~p requests on bucket: ~p timed out while in the queue, timeout: ~p.~n",
              [Cnt, BucketName, Timeout]);
print_cntr_key({queue_len, QLen}, Cnt) ->
    io:format(user,
              "~n on ~p occassions the queue was ~p requests long when new requests were added.~n",
              [Cnt, QLen]).
