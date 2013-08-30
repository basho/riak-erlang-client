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

%% @doc Encapsulates a map data-type. Maps are special key-value data
%% structures where the key is a pair of a binary name and data type,
%% and the value is a container of the specified type, e.g.
%% `{<<"friends">>, set} -> riakc_set:set()'.
-module(riakc_map).
-behaviour(riakc_datatype).

%% Callbacks
-export([new/0, new/1, new/2,
         value/1,
         to_op/1,
         context/1,
         is_type/1]).

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

-record(map, {value = [] :: orddict:orddict(entry()),
              adds = [] :: [key()],
              removes = [] :: [key()],
              context = undefined :: riakc_datatype:context() }).

-type datatype() :: counter | flag | register | set.
-type key() :: {binary(), datatype()}.
-type entry() :: {key(), riakc_datatype:datatype()}.
-type raw_entry() :: {key(), term()}.
-type update_fun() :: fun((riakc_datatype:dataype()) -> riakc_datatype:datatype()).
-type embedded_type_op() :: riakc_counter:counter_op() |
                            riakc_set:set_op() |
                            riakc_flag:flag_op() |
                            riakc_register:register_op().
-type field_update() :: {update, key(), embedded_type_op()}.
-type simple_map_op() :: {add, key()} | {remove, key()} | field_update().
-type map_op() :: {update, [simple_map_op()]}.

-export_type([map/0]).
-type map() :: #map{}.

%% @doc Creates a new, empty map container type.
-spec new() -> map().
new() ->
    #map{}.

%% @doc Creates a new map with the specified key-value pairs.
-spec new([raw_entry()]) -> map().
new(Values) when is_list(Values) ->
    #map{value = orddict:from_list([ lift_entry(Pair) || Pair <- Values ])}.

%% @doc Creates a new map with the specified key-value pairs and context.
-spec new([raw_entry()], riakc_datatype:context()) -> map().
new(Values, Context) when is_list(Values) ->
    M = new(Values),
    M#map{context=Context}.

%% @doc Extracts the value of the map, including all the contained values.
-spec value(map()) -> [raw_entry()].
value(#map{value=V}) ->
    [ begin
          M = type_module(Key),
          {Key, M:value(Value)}
      end || {Key, Value} <- orddict:to_list(V) ].

%% @doc Extracts an operation from the map that can be encoded into an
%% update request.
-spec to_op(map()) -> map_op().
to_op(#map{value=V, adds=A, removes=R}) ->
    {update,
     [ {add, Key} || Key <- A ] ++
         [ {remove, Key} || Key <- R ] ++
         orddict:fold(fun fold_extract_op/3, [], V)}.

%% @doc Extracts the update context from the map.
-spec context(map()) -> riakc_datatype:context().
context(#map{context=C}) -> C.

%% @doc Determines whether the passed term is a set container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, map).


%% ==== Operations ====

%% @doc Adds a key to the map, inserting the empty value for its type.
%% Adding a key that already exists in the map has no effect. If the
%% key has been previously removed from the map, an exception will be
%% raised.
-spec add(key(), map()) -> map().
add(Key, #map{value=Entries, adds=A, removes=R}=M) ->
    case {is_key(Key, M), lists:member(Key, R)} of
        %% The key is already in the map, do nothing.
        {true, _} -> M;
        %% The key has already been removed from the map, re-adding
        %% makes no sense.
        {false, true} ->
            erlang:error(badarg, [Key, M]);
        %% The key does not exist yet, initialize it with an empty
        %% value.
        {false, false} ->
            Mod = type_module(Key),
            NewValue = Mod:new(),
            M#map{value=orddict:store(Key, NewValue, Entries),
                  adds=[Key|A]}
    end.

%% @doc Removes a key and its value from the map. Removing a key that
%% does not exist has no effect. Removing a key whose value has been
%% locally modified via `update/3` nullifies any of those
%% modifications.
-spec erase(key(), map()) -> map().
erase(Key, #map{value=Entries, adds=A, removes=R}=M) ->
    case is_key(Key, M) of
        false -> M;
        true ->
            M#map{value=orddict:erase(Key, Entries),
                  adds=lists:delete(Key, A),
                  removes=[Key|R]}
    end.

%% @doc Updates the value stored at the key by calling the passed
%% function to get the new value. If the key did not previously exist,
%% it will be initialized to the empty value for its type. If the key
%% has been previously removed from the map, an exception will be
%% raised.
-spec update(key(), update_fun(), map()) -> map().
update(Key, Fun, #map{value=Entries, adds=A, removes=R}=M) ->
    case lists:member(Key, R) of
        true -> erlang:error(badarg, [Key, Fun, M]);
        false ->
            Mod = type_module(Key),
            M#map{value=orddict:update(Key, Fun, Mod:new(), Entries),
                  adds=lists:delete(Key, A)}
    end.

%% ==== Queries ====

%% @doc Returns the number of entries in the map.
-spec size(map()) -> pos_integer().
size(#map{value=Entries}) ->
    orddict:size(Entries).

%% @doc Returns the "unwrapped" value associated with the key in the
%% map. If the key is not present, an exception is generated.
-spec fetch(key(), map()) -> term().
fetch(Key, #map{value=Entries}=M) ->
    try orddict:fetch(Key, Entries) of
        Value ->
            entry_value(Key, Value)
    catch
        error:function_clause ->
            erlang:error(badarg, [Key, M])
    end.

%% @doc Searches for a key in the map. Returns `{ok, UnwrappedValue}'
%% when the key is present, or `error' if the key is not present in
%% the map.
-spec find(key(), map()) -> {ok, term()} | error.
find(Key, #map{value=Entries}) ->
    case orddict:find(Key, Entries) of
        {ok, Value} ->
            {ok, entry_value(Key, Value)};
        error -> error
    end.

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
    orddict:fold(fun(Key, Value, Acc) ->
                         Fun(Key, entry_value(Key,Value), Acc)
                 end, Acc0, Entries).

%% ==== Internal functions ====

%% @doc Extracts the raw value from a wrapped value for the given key.
%% @private
-spec entry_value(key(), riakc_datatype:datatype()) -> term().
entry_value(Key, Value) ->
    Mod = type_module(Key),
    Mod:value(Value).

%% @doc Takes a raw entry (key and raw value) and wraps the value in
%% the appropriate container type.
%% @private
-spec lift_entry(raw_entry()) -> entry().
lift_entry({Key, Value}) ->
    M = type_module(Key),
    {Key, M:new(Value)}.

%% @doc Determines the module for the container type of the value
%% pointed to by the given key.
%% @private
-spec type_module(key()) -> module().
type_module({_, T}) ->
    riakc_datatype:module(T).

%% @doc Folder for extracting operations from embedded types.
-spec fold_extract_op(key(), riakc_datatype:datatype(), [field_update()]) -> [field_update()].
fold_extract_op(Key, Value, Acc0) ->
    Mod = type_module(Key),
    case Mod:to_op(Value) of
        undefined ->
            Acc0;
        Op ->
            {update, Key, Op}
    end.
