%% -------------------------------------------------------------------
%%
%% riakc_datatype: Behaviour for eventually-consistent data-types
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

%% @doc When used with riakc_pb_socket:fetch_type() and
%% riakc_pb_socket:update_type(), modules implementing this behaviour
%% provide an internally consistent local view of the data while
%% capturing update operations for shipping back to the server.
-module(riakc_datatype).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(QC_OUT(P), eqc:on_output(fun(Fmt, Args) -> io:format(user, Fmt, Args) end, P)).
-compile(export_all).
-endif.


-define(MODULES, [riakc_set, riakc_counter, riakc_flag, riakc_register, riakc_map]).

-export([module_for_type/1,
         module_for_term/1]).

-export_type([datatype/0, update/1, context/0]).

-type maybe(T) :: T | undefined.
-type datatype() :: term().
-type typename() :: atom().
-type context() :: maybe(binary()).
-type update(T) :: maybe({typename(), T, context()}).

%% Constructs a new, empty container for the type. Use this when
%% creating a new key.
-callback new() -> datatype().

%% Constructs a new, empty container with context for the type. Use
%% this when creating a new key inside a map.
-callback new(context()) -> datatype().

%% Constructs a new container for the type with the specified
%% value and opaque server-side context. This should only be used
%% internally by the client code.
-callback new(Value::term(), context()) -> datatype().

%% Returns the original, unmodified value of the type. This does
%% not include the application of any locally-queued operations.
-callback value(datatype()) -> term().

%% Extracts an operation from the container that can be encoded
%% into an update request. 'undefined' should be returned if the type
%% is unmodified. This should be passed to
%% riakc_pb_socket:update_type() to submit modifications.
-callback to_op(datatype()) -> update(term()).

%% Determines whether the given term is the type managed by the
%% container module.
-callback is_type(datatype()) -> boolean().

%% Determines the symbolic name of the container's type, e.g.
%% set, map, counter.
-callback type() -> typename().

%% Returns the module that is a container for the given abstract
%% type.
-spec module_for_type(Type::atom()) -> module().
module_for_type(set)      -> riakc_set;
module_for_type(counter)  -> riakc_counter;
module_for_type(flag)     -> riakc_flag;
module_for_type(register) -> riakc_register;
module_for_type(map)      -> riakc_map.

%% @doc Returns the appropriate container module for the given term,
%% if possible.
-spec module_for_term(datatype()) -> maybe(module()).
module_for_term(T) ->
    lists:foldl(fun(Mod, undefined) ->
                        case Mod:is_type(T) of
                            true -> Mod;
                            false -> undefined
                        end;
                   (_, Mod) ->
                        Mod
                end, undefined, ?MODULES).

-ifdef(EQC).
-define(MODPROPS, [prop_value_immutable,
                   prop_unmodified,
                   prop_modified,
                   prop_is_type,
                   prop_module_for_term]).
-define(F(Fmt, Args), lists:flatten(io_lib:format(Fmt, Args))).
datatypes_test_() ->
     [{" prop_module_type() ",
       ?_assertEqual(true, quickcheck(?QC_OUT(prop_module_type())))}] ++
     [ {?F(" ~s(~s) ", [Prop, Mod]),
        ?_assertEqual(true, quickcheck(?QC_OUT(eqc:testing_time(2, ?MODULE:Prop(Mod)))))} ||
         Prop <- ?MODPROPS,
         Mod <- ?MODULES ].

run_props() ->
    run_props(500).

run_props(Count) ->
    run_props(?MODULES, Count, true).

run_props(_Mods, _Count, false) -> false;
run_props([], _Count, Res) -> Res;
run_props([Mod|Rest], Count, true) ->
    run_props(Rest, Count, run_mod_props(Mod, Count)).

run_mod_props(Mod, Count) ->
    run_mod_props(Mod, Count, ?MODPROPS, true).

%% Let's write the Either monad for the Nth time.
run_mod_props(_Mod, _Count, _Props, false) -> false;
run_mod_props(_Mode, _Count, [], Res) -> Res;
run_mod_props(Mod, Count, [Prop|Rest], _) ->
    io:format("~n~s(~s): ", [Prop, Mod]),
    run_mod_props(Mod, Count, Rest,
                  eqc:quickcheck(eqc:numtests(Count, ?MODULE:Prop(Mod)))).

prop_value_immutable(Mod) ->
    %% Modifications of the type don't change the original value.
    ?FORALL({Type, {Op,Args}},
            {Mod:gen_type(), Mod:gen_op()},
            begin
                OriginalValue = Mod:value(Type),
                NewType = erlang:apply(Mod, Op, Args ++ [Type]),
                OriginalValue == Mod:value(NewType)
            end).

prop_unmodified(Mod) ->
    %% An unmodified type returns 'undefined' for the op.
    ?FORALL(Type, Mod:gen_type(),
            Mod:to_op(Type) == undefined).

prop_modified(Mod) ->
    %% A modified type does not return 'undefined' for the op.
    ?FORALL({Type, {Op,Args}},
            {Mod:gen_type(), Mod:gen_op()},
            begin
                NewType = erlang:apply(Mod, Op, Args ++ [Type]),
                Mod:to_op(NewType) /= undefined
            end).

prop_is_type(Mod) ->
    %% is_type/1 callback always returns true for wrappers handled by
    %% Mod.
    ?FORALL(Type, Mod:gen_type(),
            Mod:is_type(Type)).

prop_module_for_term(Mod) ->
    %% module_for_term/1 returns the correct wrapper module.
    ?FORALL(Type, Mod:gen_type(),
            module_for_term(Type) == Mod).

prop_module_type() ->
    %% module/1 returns the correct wrapper module for the type.
    ?FORALL(Mod, elements(?MODULES),
            module_for_type(Mod:type()) == Mod).
-endif.
