%% -------------------------------------------------------------------
%%
%% riakc_map: Eventually-consistent map/dict type
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc <p>Encapsulates a map data-type. Maps differ from Erlang's dict
%% types in several ways:</p>
%% <ul>
%% <li>Maps are special key-value structures where the key is a pair
%% of a binary name and data type, and the value is a container of the
%% specified type, e.g. `{<<"friends">>, set} -> riakc_set:set()'.
%% Regular Erlang terms may not be values in the map, but could be
%% serialized into `register' fields if last-write-wins semantics are
%% sufficient.</li>
%% <li>Like the other eventually-consistent types, updates are not
%% applied to local state. Instead, removals, and
%% modifications are captured for later application by Riak.</li>
%% <li>You may not "store" values in a map.
%%  Existing or non-existing entries can be modified
%% using `update/3', which is analogous to `dict:update/3'. If the
%% entry is not present, it will be populated with a new value before
%% the update function is applied. The update function will receive
%% the appropriate container type as its sole argument.</li>
%% <li>Like sets, removals will be processed before additions and
%% updates in Riak. Removals performed without a context may result in
%% failure.</li>
%% <li> Updating an entry followed by removing that same
%% entry will result in no operation being recorded. Likewise,
%% removing an entry followed by updating that entry will
%% cancel the removal operation.</li>
%% </ul>
%% @end
-module(riakc_map).
-behaviour(riakc_datatype).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

%% Callbacks
-export([new/0, new/1, new/2,
         value/1,
         to_op/1,
         is_type/1,
         type/0]).

%% Operations
-export([erase/2,
         update/3]).

%% Queries
-export([size/1,
         fetch/2,
         find/2,
         is_key/2,
         fetch_keys/1,
         fold/3]).

-record(map, {value = [] :: [raw_entry()], %% orddict
              updates = [] :: [entry()], %% orddict
              removes = [] :: ordsets:ordset(key()),
              context = undefined :: riakc_datatype:context() }).

-type datatype() :: counter | flag | register | set | map.
-type key() :: {binary(), datatype()}.
-type entry() :: {key(), riakc_datatype:datatype()}.
-type raw_entry() :: {key(), term()}.
-type update_fun() :: fun((riakc_datatype:datatype()) -> riakc_datatype:datatype()).
-type embedded_type_op() :: riakc_counter:counter_op() |
                            riakc_set:set_op() |
                            riakc_flag:flag_op() |
                            riakc_register:register_op() |
                            map_op().
-type field_update() :: {update, key(), embedded_type_op()}.
-type simple_map_op() :: {remove, key()} | field_update().
-type map_op() :: {update, [simple_map_op()]}.

-export_type([crdt_map/0]).
-type crdt_map() :: #map{}.

%% @doc Creates a new, empty map container type.
-spec new() -> crdt_map().
new() ->
    #map{}.

%% @doc Creates a new map with the specified context.
-spec new(riakc_datatype:context()) -> crdt_map().
new(Context) ->
    #map{context=Context}.

%% @doc Creates a new map with the specified key-value pairs and context.
-spec new([raw_entry()], riakc_datatype:context()) -> crdt_map().
new(Values, Context) when is_list(Values) ->
    #map{value=orddict:from_list(Values), context=Context}.

%% @doc Gets the original value of the map.
-spec value(crdt_map()) -> [raw_entry()].
value(#map{value=V}) -> V.

%% @doc Extracts an operation from the map that can be encoded into an
%% update request.
-spec to_op(crdt_map()) -> riakc_datatype:update(map_op()).
to_op(#map{updates=U, removes=R, context=C}) ->
    Updates = [ {remove, Key} || Key <- R ] ++
        orddict:fold(fun fold_extract_op/3, [], U),
    case Updates of
        [] -> undefined;
        _ ->
            {type(), {update, Updates}, C}
    end.

%% @doc Determines whether the passed term is a map container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, map).

%% @doc Returns the symbolic name of this container.
-spec type() -> atom().
type() -> map.


%% ==== Operations ====

%% @doc Removes a key and its value from the map. Removing a key that
%% does not exist simply records a remove operation.
%% @throws context_required
-spec erase(key(), crdt_map()) -> crdt_map().
erase(_Key, #map{context=undefined}) ->
    throw(context_required);
erase(Key, #map{removes=R}=M) ->
    M#map{removes=ordsets:add_element(Key, R)}.


%% @doc Updates the value stored at the key by calling the passed
%% function to get the new value. If the key did not previously exist,
%% it will be initialized to the empty value for its type before being
%% passed to the function.
-spec update(key(), update_fun(), crdt_map()) -> crdt_map().
update(Key, Fun, #map{updates=U}=M) ->
    %% In order, search for key in 1) batched updates, then 2) values
    %% taken from Riak, and otherwise 3) create a new, empty data type
    %% for the update
    O = update_value_or_new(orddict:find(Key, U), Key, M),
    M#map{updates=orddict:store(Key, Fun(O), U)}.

%% ==== Queries ====

%% @doc Returns the number of entries in the map.
-spec size(crdt_map()) -> pos_integer().
size(#map{value=Entries}) ->
    orddict:size(Entries).

%% @doc Returns the "unwrapped" value associated with the key in the
%% map. If the key is not present, an exception is generated.
-spec fetch(key(), crdt_map()) -> term().
fetch(Key, #map{value=Entries}) ->
    orddict:fetch(Key, Entries).

%% @doc Searches for a key in the map. Returns `{ok, UnwrappedValue}'
%% when the key is present, or `error' if the key is not present in
%% the map.
-spec find(key(), crdt_map()) -> {ok, term()} | error.
find(Key, #map{value=Entries}) ->
    orddict:find(Key, Entries).

%% @doc Test if the key is contained in the map.
-spec is_key(key(), crdt_map()) -> boolean().
is_key(Key, #map{value=Entries}) ->
    orddict:is_key(Key, Entries).

%% @doc Returns a list of all keys in the map.
-spec fetch_keys(crdt_map()) -> [key()].
fetch_keys(#map{value=Entries}) ->
    orddict:fetch_keys(Entries).

%% @doc Folds over the entries in the map. This yields raw values,
%% not container types.
-spec fold(fun((key(), term(), term()) -> term()), term(), crdt_map()) -> term().
fold(Fun, Acc0, #map{value=Entries}) ->
    orddict:fold(Fun, Acc0, Entries).

%% ==== Internal functions ====

%% @doc Determines the module for the container type of the value
%% pointed to by the given key.
%% @private
-spec type_module(key()) -> module().
type_module({_, T}) ->
    riakc_datatype:module_for_type(T).

%% @doc Folder for extracting operations from embedded types.
-spec fold_extract_op(key(), riakc_datatype:datatype(), [field_update()]) -> [field_update()].
fold_extract_op(Key, Value, Acc0) ->
    Mod = type_module(Key),
    fold_ignore_noop(Mod:to_op(Value), Key, Acc0).

%% @doc Assist `fold_extract_op/3'
fold_ignore_noop(undefined, _Key, Acc0) ->
    Acc0;
fold_ignore_noop({_Type, Op, _Context}, Key, Acc0) ->
    [{update, Key, Op} | Acc0].

%% @doc Helper function for `update/3`. Look for a key in this map's
%% updates, and if not found, invoke `value_or_new/3'.
-spec update_value_or_new({'ok', riakc_datatype:datatype()} | error,
                          key(), crdt_map()) ->
                                 riakc_datatype:datatype().
update_value_or_new({ok, O}, _Key, _Map) ->
    O;
update_value_or_new(error, Key, #map{value=V, context=C}) ->
    Mod = type_module(Key),
    value_or_new(orddict:find(Key, V), Mod, C).

%% @doc Helper function for `update/3`. Look for a key in the map's
%% value, and if not found, instantiate a new data type record.
-spec value_or_new({'ok', term()} | error,
                   module(), riakc_datatype:context()) ->
                          riakc_datatype:datatype().
value_or_new({ok, Val}, Mod, Context) ->
    Mod:new(Val, Context);
value_or_new(error, Mod, Context) ->
    Mod:new(Context).

-ifdef(EQC).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

gen_type() ->
    ?SIZED(S, gen_type(S)).

gen_type(S) ->
    ?LET({Entries, Ctx},
         {list(gen_entry(S)), binary()},
         new(Entries, Ctx)).

gen_entry(S) ->
    frequency([
           {Fr, ?LAZY({{binary(), Mod:type()}, gen_value(Mod, S-1)})}
           || {Fr, Mod} <- [
                            {5, riakc_flag},
                            {5, riakc_register},
                            {5, riakc_counter},
                            {5, riakc_set},
                            {1, riakc_map}
                           ],
              Fr >= 5 orelse S >= 5]).

gen_value(riakc_map, S) ->
    ?LET(T, gen_type(S), value(T));
gen_value(Mod, _S) ->
    ?LET(T, Mod:gen_type(), Mod:value(T)).

gen_key() ->
    {binary(), elements([flag, register, counter, set, map])}.

gen_op() ->
    oneof([
           {erase, [gen_key()]},
           ?LAZY({update, ?LET({_,T}=Key, gen_key(), [Key,
                                                      gen_update_fun(T)])})
          ]).

gen_update_fun(Type) ->
    frequency([{5, gen_update_fun1(Type)},
               {1, gen_update_fun2(Type)}]).

gen_update_fun1(Type) ->
    Mod = riakc_datatype:module_for_type(Type),
    ?LET({Op, Args}, Mod:gen_op(),
         fun(R) ->
                 erlang:apply(Mod, Op, Args ++ [R])
         end).

gen_update_fun2(Type) ->
    Mod = riakc_datatype:module_for_type(Type),
    ?LET(OpList, non_empty(list(Mod:gen_op())),
         fun(R) ->
                 lists:foldl(fun({Op, Args}, Acc) ->
                                     erlang:apply(Mod, Op, Args ++ [Acc])
                             end, R, OpList)
         end).

prop_nested_defaults() ->
    %% A map with default-initialized nested objects should
    %% effectively be a no-op
    ?FORALL(Nops, non_empty(list(gen_key())),
            begin
                Map = lists:foldl(fun(K,M) -> riakc_map:update(K, fun(V) -> V end, M) end,
                                  riakc_map:new(), Nops),
                undefined == riakc_map:to_op(Map)
            end).

prop_nested_defaults_test() ->
    ?assert(eqc:quickcheck(?QC_OUT(prop_nested_defaults()))).

-endif.
