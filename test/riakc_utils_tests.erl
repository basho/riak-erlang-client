%% -------------------------------------------------------------------
%%
%% riakc_utils_tests
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
-ifdef(TEST).

-module(riakc_utils_tests).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

bad_unicode_binary_test() ->
    S = <<"\xa0\xa1">>,
    ?assertThrow({unicode_error, _Msg}, riakc_utils:characters_to_unicode_binary(S)).

-endif.
