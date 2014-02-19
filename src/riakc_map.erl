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
%% applied to local state. Instead, additions, removals, and
%% modifications are captured for later application by Riak. Use
%% `dirty_value/1' to access a local "view" of the updates.</li>
%% <li>You may not "store" values in a map, but you may create new
%% entries using `add/2', which inserts an empty value of the
%% specified type. Existing or non-existing entries can be modified
%% using `update/3', which is analogous to `dict:update/3'. If the
%% entry is not present, it will be populated with a new value before
%% the update function is applied. The update function will receive
%% the appropriate container type as its sole argument.</li>
%% <li>Like sets, removals will be processed before additions and
%% updates in Riak. Removals performed without a context may result in
%% failure.</li>
%% <li>Adding or updating an entry followed by removing that same
%% entry will result in no operation being recorded. Likewise,
%% removing an entry followed by adding or updating that entry will
%% cancel the removal operation.</li>
%% </ul>
%% @end
-module(riakc_map).
-behaviour(riakc_datatype).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-compile(export_all).
-endif.

%% Callbacks
-export([new/0, new/2,
         value/1,
         dirty_value/1,
         to_op/1,
         is_type/1,
         type/0]).

%% Operations
-export([add/2,
         erase/2,
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
              adds = [] :: ordsets:ordset(key()),
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
-type simple_map_op() :: {add, key()} | {remove, key()} | field_update().
-type map_op() :: {update, [simple_map_op()]}.

-export_type([map/0]).
-type map() :: #map{}.

%% @doc Creates a new, empty map container type.
-spec new() -> map().
new() ->
    #map{}.

%% @doc Creates a new map with the specified key-value pairs and context.
-spec new([raw_entry()], riakc_datatype:context()) -> map().
new(Values, Context) when is_list(Values) ->
    #map{value=orddict:from_list(Values), context=Context}.

%% @doc Gets the original value of the map.
-spec value(map()) -> [raw_entry()].
value(#map{value=V}) -> V.

%% @doc Gets the value of the map after local updates are applied.
-spec dirty_value(map()) -> [raw_entry()].
dirty_value(#map{value=V, updates=U, removes=R}) ->
    Merged = orddict:merge(fun(K, _Value, Update) ->
                                   Mod = type_module(K),
                                   Mod:dirty_value(Update)
                           end, V, U),
    [ Pair || {Key, _}=Pair <- Merged,
              not ordsets:is_element(Key, R) ].

%% @doc Extracts an operation from the map that can be encoded into an
%% update request.
-spec to_op(map()) -> riakc_datatype:update(map_op()).
to_op(#map{updates=U, adds=A, removes=R, context=C}) ->
    Updates = [ {add, Key} || Key <- A ] ++
        [ {remove, Key} || Key <- R ] ++
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

%% @doc Adds a key to the map, inserting the empty value for its type.
%% Adding a key that already exists in the map has no effect. If the
%% key has been previously removed from the map, the removal will be
%% discarded, but no explicit add will be recorded.
-spec add(key(), map()) -> map().
add(Key, #map{value=V, updates=U, adds=A, removes=R}=M) ->
    case {orddict:is_key(Key, U), ordsets:is_element(Key, R)} of
        %% It is already in the updates, do nothing.
        {true, _} ->
            M;
        %% It was previously removed, clear that removal.
        {false, true} ->
            M#map{removes=ordsets:del_element(Key, R)};
        %% It is brand new, record the add and store a value in the
        %% updates. If it was in the original value, initialize it
        %% with that.
        {false, false} ->
            Value = find_or_new(Key, V),
            M#map{adds=ordsets:add_element(Key, A),
                  updates=orddict:store(Key, Value, U)}
    end.

%% @doc Removes a key and its value from the map. Removing a key that
%% does not exist simply records a remove operation. Removing a key
%% whose value has been added via `add/2' or locally modified via
%% `update/3' nullifies any of those modifications, without recording
%% a remove operation.
-spec erase(key(), map()) -> map().
erase(Key, #map{updates=U, adds=A, removes=R}=M) ->
    case orddict:is_key(Key, U) of
        true ->
            M#map{updates=orddict:erase(Key, U),
                  adds=ordsets:del_element(Key, A)};
        false ->
            M#map{removes=ordsets:add_element(Key, R)}
    end.

%% @doc Updates the value stored at the key by calling the passed
%% function to get the new value. If the key did not previously exist,
%% it will be initialized to the empty value for its type before being
%% passed to the function. If the key was previously removed with
%% `erase/2', the remove operation will be nullified.
-spec update(key(), update_fun(), map()) -> map().
update(Key, Fun, #map{value=V, updates=U0, removes=R0}=M) ->
    R = ordsets:del_element(Key, R0),
    U = case orddict:is_key(Key, U0) of
            true ->
                orddict:update(Key, Fun, U0);
            false ->
                orddict:store(Key, Fun(find_or_new(Key, V)), U0)
        end,
    M#map{removes=R, updates=U}.

%% ==== Queries ====

%% @doc Returns the number of entries in the map.
-spec size(map()) -> pos_integer().
size(#map{value=Entries}) ->
    orddict:size(Entries).

%% @doc Returns the "unwrapped" value associated with the key in the
%% map. If the key is not present, an exception is generated.
-spec fetch(key(), map()) -> term().
fetch(Key, #map{value=Entries}) ->
    orddict:fetch(Key, Entries).

%% @doc Searches for a key in the map. Returns `{ok, UnwrappedValue}'
%% when the key is present, or `error' if the key is not present in
%% the map.
-spec find(key(), map()) -> {ok, term()} | error.
find(Key, #map{value=Entries}) ->
    orddict:find(Key, Entries).

%% @doc Test if the key is contained in the map.
-spec is_key(key(), map()) -> boolean().
is_key(Key, #map{value=Entries}) ->
    orddict:is_key(Key, Entries).

%% @doc Returns a list of all keys in the map.
-spec fetch_keys(map()) -> [key()].
fetch_keys(#map{value=Entries}) ->
    orddict:fetch_keys(Entries).

%% @doc Folds over the entries in the map. This yields raw values,
%% not container types.
-spec fold(fun((key(), term(), term()) -> term()), term(), map()) -> term().
fold(Fun, Acc0, #map{value=Entries}) ->
    orddict:fold(Fun, Acc0, Entries).

%% ==== Internal functions ====

find_or_new(Key, Values) ->
    Mod = type_module(Key),
    case orddict:find(Key, Values) of
        {ok, Found} ->
            Mod:new(Found, undefined);
        error ->
            Mod:new()
    end.

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
    case Mod:to_op(Value) of
        undefined ->
            [{add, Key}|Acc0];
        {_Type, Op, _Context} ->
            [{update, Key, Op} | Acc0]
    end.

-ifdef(EQC).
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
           {add, [gen_key()]},
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
-endif.
