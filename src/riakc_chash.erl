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
    iterator_pop/2
]).

-record(chashbin, {
    size :: pos_integer(),
    owners :: binary(),
    nodes :: tuple()
}).

-record(iterator, {
    pos     :: non_neg_integer(),
    start   :: non_neg_integer(),
    chbin   :: chashbin()
}).

-type chashbin() :: #chashbin{}.
-type iterator() :: #iterator{}.

-define(UNIT, 176).
-define(ENTRY, binary-unit:?UNIT).
-define(RINGTOP, erlang:trunc(math:pow(2, 160) - 1)).

-define(is_chashbin(Record), erlang:is_record(Record, chashbin)).

%% ====================================================================
%% API functions
%% ====================================================================
-spec chash_key({Bucket :: binary(), Key :: binary()}) ->
    ok.
chash_key({Bucket, Key}) when erlang:is_binary(Bucket) andalso erlang:is_binary(Key) ->
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
