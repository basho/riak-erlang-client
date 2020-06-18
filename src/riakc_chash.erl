%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Dec 2019 14:25
%%%-------------------------------------------------------------------
-module(riakc_chash).
-author("paulhunt").

%% API
-export([
    chash_key/1,
    chashring_to_chashbin/1,
    iterator/2,
    iterator_pop/2,
    iterator_value/1,
    iterator_next/1
]).

-record(chashbin, {
    size    :: pos_integer(),
    owners  :: binary(),
    nodes   :: tuple()
}).

-record(iterator, {
    pos     :: non_neg_integer(),
    start   :: non_neg_integer(),
    chbin   :: chashbin()
}).

-type chashbin() :: #chashbin{}.
-type iterator() :: #iterator{}.

-define(ENTRY, binary-unit:176).
-define(RINGTOP, erlang:trunc(math:pow(2, 160) - 1)).

-define(is_bucket(Bucket), is_binary(Bucket) orelse (is_tuple(Bucket) andalso is_binary(element(1, Bucket)) andalso is_binary(element(2, Bucket)))).
-define(is_chashbin(Record), erlang:is_record(Record, chashbin)).

%% ====================================================================
%% API functions
%% ====================================================================
-spec chash_key({Bucket :: binary(), Key :: binary()}) ->
    binary().
chash_key({Bucket, Key}) when ?is_bucket(Bucket) andalso erlang:is_binary(Key) ->
    BinaryBucketKey = erlang:term_to_binary({Bucket, Key}),
    crypto:hash(sha, BinaryBucketKey).

-spec chashring_to_chashbin({Size :: integer(), Owners :: list({non_neg_integer(), atom()})}) ->
    chashbin().
chashring_to_chashbin({Size, Owners}) when erlang:is_integer(Size) andalso erlang:is_list(Owners) ->
    Nodes1 = [Node || {_, Node} <- Owners],
    Nodes2 = lists:usort(Nodes1),
    Nodes3 = lists:zip(Nodes2, lists:seq(1, erlang:length(Nodes2))),
    Bin = create_bin(Owners, Nodes3, <<>>),
    #chashbin{size = Size, owners = Bin, nodes = erlang:list_to_tuple(Nodes2)}.

-spec iterator(HashKey :: integer() | first, ChBin :: chashbin()) ->
    iterator().
iterator(first, ChBin) when ?is_chashbin(ChBin) ->
    #iterator{pos = 0, start = 0, chbin = ChBin};
iterator(<<HashKey:160/integer>>, ChBin) when ?is_chashbin(ChBin) ->
    iterator(HashKey, ChBin);
iterator(HashKey, ChBin) when erlang:is_integer(HashKey) andalso ?is_chashbin(ChBin) ->
    Pos = responsible_position(HashKey, ChBin),
    #iterator{pos = Pos, start = Pos, chbin = ChBin}.

-spec iterator_pop(Nval :: pos_integer(), iterator()) ->
    {[{integer(), node()}], iterator()}.
iterator_pop(Nval, Iterator = #iterator{pos = Pos, chbin = ChBin}) ->
    #chashbin{size = Size, owners = Bin, nodes = Nodes} = ChBin,
    L = case Bin of
            <<_:Pos/?ENTRY, Bin2:Nval/?ENTRY, _/binary>> ->
                [{Index, erlang:element(Id, Nodes)} || <<Index:160/integer, Id:16/integer>> <= Bin2];
            _ ->
                Left = (Nval + Pos) - Size,
                Skip = Pos - Left,
                <<Bin3:Left/?ENTRY, _:Skip/?ENTRY, Bin2/binary>> = Bin,
                L1 = [{Index, erlang:element(Id, Nodes)} || <<Index:160/integer, Id:16/integer>> <= Bin2],
                L2 = [{Index, erlang:element(Id, Nodes)} || <<Index:160/integer, Id:16/integer>> <= Bin3],
                L1 ++ L2
        end,
    Pos2 = (Pos + Nval) rem Size,
    Iterator2 = Iterator#iterator{pos = Pos2},
    {L, Iterator2}.

-spec iterator_value(iterator()) ->
    {chash:index_as_int(), node()}.
iterator_value(#iterator{pos = Pos, chbin = #chashbin{owners = Owners, nodes = Nodes}}) ->
    <<_:Pos/?ENTRY, Index:160/integer, Id:16/integer, _/binary>> = Owners,
    Owner = erlang:element(Id, Nodes),
    {Index, Owner}.

-spec iterator_next(iterator()) ->
    iterator() | done.
iterator_next(Iterator = #iterator{pos = Position, start = Start, chbin = ChBin}) ->
    NewPosition = (Position + 1) rem ChBin#chashbin.size,
    case NewPosition of
        Start ->
            done;
        _ ->
            Iterator#iterator{pos = NewPosition}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
create_bin([], _, Bin) ->
    Bin;
create_bin([{Idx, Owner}|Owners], Nodes, Bin) ->
    {Owner, Id} = lists:keyfind(Owner, 1, Nodes),
    Bin2 = <<Bin/binary, Idx:160/integer, Id:16/integer>>,
    create_bin(Owners, Nodes, Bin2).

responsible_position(HashKey, #chashbin{size = Size}) ->
    Inc = ?RINGTOP div Size,
    ((HashKey div Inc) + 1) rem Size.

%% ====================================================================
%% Eunit tests
%% ====================================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

chashring() ->
    {8, [
        {0,'node1@127.0.0.1'},{10,'node1@127.0.0.1'},{20,'node2@127.0.0.1'},{30,'node2@127.0.0.1'},
        {40,'node3@127.0.0.1'},{50,'node3@127.0.0.1'},{60,'node4@127.0.0.1'},{70,'node4@127.0.0.1'}
    ]}.

big_chashring() ->
    Node1 = 'node1@127.0.0.1',
    Node2 = 'node2@127.0.0.1',
    Node3 = 'node3@127.0.0.1',
    Node4 = 'node4@127.0.0.1',
    {64, [
        {0, Node1}, {10, Node2}, {20, Node3}, {30, Node4}, {40, Node1}, {50, Node2}, {60, Node3}, {70, Node4},
        {80, Node1}, {90, Node2}, {100, Node3}, {110, Node4}, {120, Node1}, {130, Node2}, {140, Node3}, {150, Node4},
        {160, Node1}, {170, Node2}, {180, Node3}, {190, Node4}, {200, Node1}, {210, Node2}, {220, Node3}, {230, Node4},
        {240, Node1}, {250, Node2}, {260, Node3}, {270, Node4}, {280, Node1}, {290, Node2}, {300, Node3}, {310, Node4},
        {320, Node1}, {330, Node2}, {340, Node3}, {350, Node4}, {360, Node1}, {370, Node2}, {380, Node3}, {390, Node4},
        {400, Node1}, {410, Node2}, {420, Node3}, {430, Node4}, {440, Node1}, {450, Node2}, {460, Node3}, {470, Node4},
        {480, Node1}, {490, Node2}, {500, Node3}, {510, Node4}, {520, Node1}, {530, Node2}, {540, Node3}, {550, Node4},
        {560, Node1}, {570, Node2}, {580, Node3}, {590, Node4}, {600, Node1}, {610, Node2}, {620, Node3}, {630, Node4}
    ]}.

chashbin() ->
    #chashbin{size = 8, owners = <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        10,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,20,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,30,0,2,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,40,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,50,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,60,0,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,70,0,4>>, nodes = {'node1@127.0.0.1', 'node2@127.0.0.1',
        'node3@127.0.0.1', 'node4@127.0.0.1'}}.

iterator() ->
    #iterator{pos = 7, start = 7, chbin = chashbin()}.

%% chash_key/1 tests
chash_key_test() ->
    BucketKey = {<<"test_bucket">>, <<"test_key">>},
    ExpectedChashKey = <<218,213,43,122,122,34,72,213,82,88,61,244,236,21,55,30,4,245,249,100>>,
    ActualChashKey = chash_key(BucketKey),
    ?assertEqual(ExpectedChashKey, ActualChashKey),
    ok.

%% chashring_to_chashbin/1 tests
chashring_to_chashbin_test() ->
    ChashRing = chashring(),
    ExpectedChashBin = chashbin(),
    ActualChashBin = chashring_to_chashbin(ChashRing),
    ?assertEqual(ExpectedChashBin, ActualChashBin),
    ok.

%% iterator/2 test
iterator_with_first_hash_key_test() ->
    ChashBin = chashbin(),
    ExpectedIterator = #iterator{pos = 0, start = 0, chbin = ChashBin},
    ActualIterator = iterator(first, ChashBin),
    ?assertEqual(ExpectedIterator, ActualIterator),
    ok.

iterator_with_binary_hash_key_test() ->
    ChashBin = chashbin(),
    BinHashKey = chash_key({<<"test_bucket">>, <<"test_key">>}),
    ExpectedIterator = #iterator{pos = 7, start = 7, chbin = ChashBin},
    ActualIterator = iterator(BinHashKey, ChashBin),
    ?assertEqual(ExpectedIterator, ActualIterator),
    ok.

iterator_with_integer_hash_key_test() ->
    ChashBin = chashbin(),
    <<IntChashKey:160/integer>> = chash_key({<<"test_bucket">>, <<"test_key">>}),
    ExpectedIterator = #iterator{pos = 7, start = 7, chbin = ChashBin},
    ActualIterator = iterator(IntChashKey, ChashBin),
    ?assertEqual(ExpectedIterator, ActualIterator),
    ok.

%% iterator_pop/2 tests
iterator_pop_with_big_chashbin_test() ->
    BigChashRing = big_chashring(),
    BigChashBin = chashring_to_chashbin(BigChashRing),
    BinHashKey = chash_key({<<"test_bucket">>, <<"test_key">>}),
    Nval = 3,
    ExpectedNodeList = [{550, 'node4@127.0.0.1'}, {560, 'node1@127.0.0.1'}, {570, 'node2@127.0.0.1'}],
    ExpectedIterator = #iterator{pos = 58, start = 55, chbin = BigChashBin},
    ExpectedResult = {ExpectedNodeList, ExpectedIterator},
    Iterator = iterator(BinHashKey, BigChashBin),
    ActualResult = iterator_pop(Nval, Iterator),
    ?assertEqual(ExpectedResult, ActualResult),
    ok.

iterator_pop_with_small_chashbin_test() ->
    Iterator = iterator(),
    Nval = 3,
    ExpectedNodeList = [{70, 'node4@127.0.0.1'}, {0, 'node1@127.0.0.1'}, {10, 'node1@127.0.0.1'}],
    ExpectedIterator = #iterator{pos = 2, start = 7, chbin = chashbin()},
    ExpectedResult = {ExpectedNodeList, ExpectedIterator},
    ActualResult = iterator_pop(Nval, Iterator),
    ?assertEqual(ExpectedResult, ActualResult),
    ok.

%% iterator_value/1 tests
iterator_value_test() ->
    Iterator = iterator(),
    ExpectedResult = {70, 'node4@127.0.0.1'},
    ActualResult = iterator_value(Iterator),
    ?assertEqual(ExpectedResult, ActualResult),
    ok.

%% TODO - Complete eunit tests for iterator_next/1 function.
%% iterator_next/1 tests

-endif.
