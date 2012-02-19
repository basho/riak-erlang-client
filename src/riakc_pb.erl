%% -------------------------------------------------------------------
%%
%% riakc_pb: protocol buffer utility functions
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Utility functions for protocol buffers. These are used inside
%% the client code and do not normally need to be used in application
%% code.

-module(riakc_pb).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).
-endif.
-include("riakclient_pb.hrl").
-include("riakc_obj.hrl").

%% Names of PB fields in bucket properties
-define(PB_PROPS,   <<"props">>).
-define(PB_KEYS,    <<"keys">>).
-define(PB_LINKFUN, <<"linkfun">>).
-define(PB_MOD,     <<"mod">>).
-define(PB_FUN,     <<"fun">>).
-define(PB_CHASH,   <<"chash_keyfun">>).
-define(PB_JSFUN,    <<"jsfun">>).
-define(PB_JSANON,   <<"jsanon">>).
-define(PB_JSBUCKET, <<"bucket">>).
-define(PB_JSKEY,    <<"key">>).
-define(PB_ALLOW_MULT, <<"allow_mult">>).

-export([encode/1, decode/2, msg_type/1, msg_code/1,
         pbify_rpbcontents/2, pbify_rpbcontent/1,
         pbify_rpbcontent_entry/3, erlify_rpbcontents/1,
         erlify_rpbcontent/1, pbify_rpbpair/1, any_to_list/1,
         erlify_rpbpair/1, pbify_rpblink/1, erlify_rpblink/1,
         erlify_rpbbucketprops/1, pbify_rpbbucketprops/1,
         pbify_rpbbucketprops/2, pbify_bool/1, erlify_bool/1,
         to_binary/1]).

%% @doc Create an iolist of msg code and protocol buffer message
-spec encode(atom() | tuple()) -> iolist().
encode(Msg) when is_atom(Msg) ->
    [msg_code(Msg)];
encode(Msg) when is_tuple(Msg) ->
    MsgType = element(1, Msg),
    [msg_code(MsgType) | riakclient_pb:iolist(MsgType, Msg)].
 
%% @doc Decode a protocol buffer message given its type - if no bytes
%% return the atom for the message code
-spec decode(integer(), binary()) -> atom() | tuple().
decode(MsgCode, <<>>) ->
    msg_type(MsgCode);
decode(MsgCode, MsgData) ->
    riakclient_pb:decode(msg_type(MsgCode), MsgData).

%% @doc Converts a message code into the symbolic message name.
-spec msg_type(integer()) -> atom().
msg_type(0) -> rpberrorresp;
msg_type(1) -> rpbpingreq;
msg_type(2) -> rpbpingresp;
msg_type(3) -> rpbgetclientidreq;
msg_type(4) -> rpbgetclientidresp;
msg_type(5) -> rpbsetclientidreq;
msg_type(6) -> rpbsetclientidresp;
msg_type(7) -> rpbgetserverinforeq;
msg_type(8) -> rpbgetserverinforesp;
msg_type(9) -> rpbgetreq;
msg_type(10) -> rpbgetresp;
msg_type(11) -> rpbputreq;
msg_type(12) -> rpbputresp;
msg_type(13) -> rpbdelreq;
msg_type(14) -> rpbdelresp;
msg_type(15) -> rpblistbucketsreq;
msg_type(16) -> rpblistbucketsresp;
msg_type(17) -> rpblistkeysreq;
msg_type(18) -> rpblistkeysresp;
msg_type(19) -> rpbgetbucketreq;
msg_type(20) -> rpbgetbucketresp;
msg_type(21) -> rpbsetbucketreq;
msg_type(22) -> rpbsetbucketresp;
msg_type(23) -> rpbmapredreq;
msg_type(24) -> rpbmapredresp;
msg_type(_) -> undefined.

%% @doc Converts a symbolic message name into a message code.
-spec msg_code(atom()) -> integer().
msg_code(rpberrorresp)           -> 0;
msg_code(rpbpingreq)             -> 1;
msg_code(rpbpingresp)            -> 2;
msg_code(rpbgetclientidreq)      -> 3;
msg_code(rpbgetclientidresp)     -> 4;
msg_code(rpbsetclientidreq)      -> 5;
msg_code(rpbsetclientidresp)     -> 6;
msg_code(rpbgetserverinforeq)    -> 7;
msg_code(rpbgetserverinforesp)   -> 8;
msg_code(rpbgetreq)              -> 9;
msg_code(rpbgetresp)             -> 10;
msg_code(rpbputreq)              -> 11;
msg_code(rpbputresp)             -> 12;
msg_code(rpbdelreq)              -> 13;
msg_code(rpbdelresp)             -> 14;
msg_code(rpblistbucketsreq)      -> 15;
msg_code(rpblistbucketsresp)     -> 16;
msg_code(rpblistkeysreq)         -> 17;
msg_code(rpblistkeysresp)        -> 18;
msg_code(rpbgetbucketreq)        -> 19;
msg_code(rpbgetbucketresp)       -> 20;
msg_code(rpbsetbucketreq)        -> 21;
msg_code(rpbsetbucketresp)       -> 22;
msg_code(rpbmapredreq)           -> 23;
msg_code(rpbmapredresp)          -> 24.

%% ===================================================================
%% Encoding/Decoding
%% ===================================================================
    
%% @doc Convert a list of {MetaData,Value} pairs to protocol buffers
-spec pbify_rpbcontents(riakc_obj:contents(), list()) -> list(tuple()).
pbify_rpbcontents([], Acc) ->
    lists:reverse(Acc);
pbify_rpbcontents([Content | Rest], Acc) ->
    pbify_rpbcontents(Rest, [pbify_rpbcontent(Content) | Acc]).

%% @doc Convert a metadata/value pair into an #rpbcontent{} record
-spec pbify_rpbcontent({riakc_obj:metadata(), riakc_obj:value()}) -> tuple().
pbify_rpbcontent({MetadataIn, ValueIn}=C) ->
    {Metadata, Value} = 
        case is_binary(ValueIn) of
            true ->
                C;
            false ->
                %% If the riak object was created using
                %% the native erlang interface, it is possible
                %% for the value to consist of arbitrary terms.  
                %% PBC needs to send a binary, so replace the content type
                %% to mark it as an erlang binary and encode
                %% the term as a binary.
                {dict:store(?MD_CTYPE, ?CTYPE_ERLANG_BINARY, MetadataIn),
             term_to_binary(ValueIn)}
        end,
    dict:fold(fun pbify_rpbcontent_entry/3, #rpbcontent{value = Value}, Metadata).

%% @doc Convert the metadata dictionary entries to protocol buffers
-spec pbify_rpbcontent_entry(MetadataKey::string(), any(), tuple()) -> tuple().
pbify_rpbcontent_entry(?MD_CTYPE, ContentType, PbContent) when is_list(ContentType) -> 
    PbContent#rpbcontent{content_type = ContentType};
pbify_rpbcontent_entry(?MD_CHARSET, Charset, PbContent) when is_list(Charset) ->
    PbContent#rpbcontent{charset = Charset};
pbify_rpbcontent_entry(?MD_ENCODING, Encoding, PbContent) when is_list(Encoding) ->
    PbContent#rpbcontent{content_encoding = Encoding};
pbify_rpbcontent_entry(?MD_VTAG, Vtag, PbContent) when is_list(Vtag) ->
    PbContent#rpbcontent{vtag = Vtag};
pbify_rpbcontent_entry(?MD_LINKS, Links, PbContent) when is_list(Links) ->
    PbContent#rpbcontent{links = [pbify_rpblink(E) || E <- Links]};
pbify_rpbcontent_entry(?MD_LASTMOD, {MS,S,US}, PbContent) -> 
    PbContent#rpbcontent{last_mod = 1000000*MS+S, last_mod_usecs = US};
pbify_rpbcontent_entry(?MD_USERMETA, UserMeta, PbContent) when is_list(UserMeta) ->
    PbContent#rpbcontent{usermeta = [pbify_rpbpair(E) || E <- UserMeta]};
pbify_rpbcontent_entry(?MD_INDEX, Indexes, PbContent) when is_list(Indexes) ->
    PbContent#rpbcontent{indexes = [pbify_rpbpair(E) || E <- Indexes]};
pbify_rpbcontent_entry(_Key, _Value, PbContent) ->
    %% Ignore unknown metadata - need to add to RpbContent if it needs to make it
    %% to/from the client
    PbContent.

%% @doc Convert a list of rpbcontent pb messages to a list of [{MetaData,Value}] tuples
-spec erlify_rpbcontents(PBContents::[tuple()]) -> riakc_obj:contents().
erlify_rpbcontents(RpbContents) ->
    [erlify_rpbcontent(RpbContent) || RpbContent <- RpbContents].

%% @doc Convert an rpccontent pb message to an erlang {MetaData,Value} tuple
-spec erlify_rpbcontent(PBContent::tuple()) -> {riakc_obj:metadata(), riakc_obj:value()}.
erlify_rpbcontent(PbC) ->
    ErlMd0 = orddict:new(),
    case PbC#rpbcontent.content_type of
        undefined ->
            ErlMd1 = ErlMd0;
        ContentType ->
            ErlMd1 = orddict:store(?MD_CTYPE, binary_to_list(ContentType), ErlMd0)
    end,
    case PbC#rpbcontent.charset of
        undefined ->
            ErlMd2 = ErlMd1;
        Charset ->
            ErlMd2 = orddict:store(?MD_CHARSET, binary_to_list(Charset), ErlMd1)
    end,
    case PbC#rpbcontent.content_encoding of
        undefined ->
            ErlMd3 = ErlMd2;
        Encoding ->
            ErlMd3 = orddict:store(?MD_ENCODING, binary_to_list(Encoding), ErlMd2)
    end,
    case PbC#rpbcontent.vtag of
        undefined ->
            ErlMd4 = ErlMd3;
        Vtag ->
            ErlMd4 = orddict:store(?MD_VTAG, binary_to_list(Vtag), ErlMd3)
    end,
    case PbC#rpbcontent.links of
        undefined ->
            ErlMd5 = ErlMd4;
        PbLinks ->
            Links = [erlify_rpblink(E) || E <- PbLinks],
            ErlMd5 = orddict:store(?MD_LINKS, Links, ErlMd4)
    end,
    case PbC#rpbcontent.last_mod of
        undefined ->
            ErlMd6 = ErlMd5;
        LastMod ->
            case PbC#rpbcontent.last_mod_usecs of
                undefined ->
                    Usec = 0;
                Usec ->
                    Usec
            end,
            Msec = LastMod div 1000000,
            Sec = LastMod rem 1000000,
            ErlMd6 = orddict:store(?MD_LASTMOD, {Msec,Sec,Usec}, ErlMd5)
    end,
    case PbC#rpbcontent.usermeta of
        undefined ->
            ErlMd7 = ErlMd6;
        PbUserMeta ->
            UserMeta = [erlify_rpbpair(E) || E <- PbUserMeta],
            ErlMd7 = orddict:store(?MD_USERMETA, UserMeta, ErlMd6)
    end,
    case PbC#rpbcontent.indexes of
        undefined ->
            ErlMd = ErlMd7;
        PbIndexes ->
            Indexes = [erlify_rpbpair(E) || E <- PbIndexes],
            ErlMd = orddict:store(?MD_INDEX, Indexes, ErlMd7)
    end,

    {dict:from_list(orddict:to_list(ErlMd)), PbC#rpbcontent.value}.


%% @doc Convert {K,V} tuple to protocol buffers
-spec pbify_rpbpair({Key::binary(), Value::any()}) -> tuple().
pbify_rpbpair({K,V}) ->
    #rpbpair{key = K, value = any_to_list(V)}.

%% @doc Converts an arbitrary type to a list for sending in a PB.
-spec any_to_list(list() | atom() | binary() | integer()) -> list().
any_to_list(V) when is_list(V) ->
    V;
any_to_list(V) when is_atom(V) ->
    atom_to_list(V);
any_to_list(V) when is_binary(V) ->
    binary_to_list(V);
any_to_list(V) when is_integer(V) ->
    integer_to_list(V).

%% @doc Convert RpbPair PB message to erlang {K,V} tuple
-spec erlify_rpbpair(PBPair::tuple()) -> {string(), string()}.
erlify_rpbpair(#rpbpair{key = K, value = V}) ->
    {binary_to_list(K), binary_to_list(V)}.

%% @doc Convert erlang link tuple to RpbLink PB message
-spec pbify_rpblink({{riakc_obj:bucket(), riakc_obj:key()}, binary() | string()}) -> tuple().
pbify_rpblink({{B,K},T}) ->
    #rpblink{bucket = B, key = K, tag = T}.

%% @doc Convert RpbLink PB message to erlang link tuple
-spec erlify_rpblink(PBLink::tuple()) -> {{riakc_obj:bucket(), riakc_obj:key()}, binary() | string()}.
erlify_rpblink(#rpblink{bucket = B, key = K, tag = T}) ->
    {{B,K},T}.

%% @doc Convert an RpbBucketProps message to a property list
-spec erlify_rpbbucketprops(PBProps::tuple() | undefined) -> [proplists:property()].
erlify_rpbbucketprops(undefined) ->
    [];
erlify_rpbbucketprops(Pb) ->
    lists:flatten(
      [case Pb#rpbbucketprops.n_val of
           undefined ->
               [];
           Nval ->
               {n_val, Nval}
       end,
       case Pb#rpbbucketprops.allow_mult of
           undefined ->
               [];
           Flag when is_boolean(Flag) ->
               {allow_mult, Flag};
           Flag ->
               {allow_mult, erlify_bool(Flag)}
       end]).

%% @doc Convert a property list to an RpbBucketProps message
-spec pbify_rpbbucketprops([proplists:property()]) -> PBProps::tuple().
pbify_rpbbucketprops(Props) ->
    pbify_rpbbucketprops(Props, #rpbbucketprops{}).

%% @doc Convert a property list to an RpbBucketProps message
-spec pbify_rpbbucketprops([proplists:property()], PBPropsIn::tuple()) -> PBPropsOut::tuple().
pbify_rpbbucketprops([], Pb) ->
    Pb;
pbify_rpbbucketprops([{n_val, Nval} | Rest], Pb) ->
    pbify_rpbbucketprops(Rest, Pb#rpbbucketprops{n_val = Nval});
pbify_rpbbucketprops([{allow_mult, Flag} | Rest], Pb) ->
    pbify_rpbbucketprops(Rest, Pb#rpbbucketprops{allow_mult = pbify_bool(Flag)});
pbify_rpbbucketprops([_Ignore|Rest], Pb) ->
    %% Ignore any properties not explicitly part of the PB message
    pbify_rpbbucketprops(Rest, Pb).
    
%% @doc Convert a true/false, 1/0 etc to a 1/0 for protocol buffers bool
-spec pbify_bool(boolean() | integer()) -> boolean().
pbify_bool(true) ->
    true;
pbify_bool(false) ->
    false;
pbify_bool(N) when is_integer(N) ->
    case N =:= 0 of
        true ->
            true;
        false ->
            false
    end.

%% @doc Convert a protocol buffers boolean to an Erlang boolean
-spec erlify_bool(integer()) -> boolean().
erlify_bool(0) ->
    false;
erlify_bool(1) ->
    true.

%% @doc Make sure an atom/string/binary is definitely a binary
-spec to_binary(atom() | list() | binary()) -> binary().
to_binary(A) when is_atom(A) ->
    atom_to_binary(A, latin1);
to_binary(L) when is_list(L) ->
    list_to_binary(L);
to_binary(B) when is_binary(B) ->
    B.

%% ===================================================================
%% Unit Tests
%% ===================================================================
-ifdef(TEST).

pb_test_() ->
    {setup, fun() ->
                    code:add_pathz("../ebin")
            end,

     [{"content encode decode", 
       ?_test(begin
                  MetaData = dict:from_list(
                               [{?MD_CTYPE, "ctype"},
                                {?MD_CHARSET, "charset"},
                                {?MD_ENCODING, "encoding"},
                                {?MD_VTAG, "vtag"},
                                {?MD_LINKS, [{{<<"b1">>, <<"k1">>}, <<"v1">>},
                                             {{<<"b2">>, <<"k2">>}, <<"v2">>}
                                            ]},
                                {?MD_LASTMOD, {1, 2, 3}},
                                {?MD_USERMETA, [{"X-Riak-Meta-MyMetaData1","here it is"},
                                                {"X-Riak-Meta-MoreMd", "have some more"}
                                               ]}
                               ]),
                  Value = <<"test value">>,
                  {MetaData2, Value2} = erlify_rpbcontent(
                                          riakclient_pb:decode_rpbcontent(
                                            riakclient_pb:encode_rpbcontent(
                                              pbify_rpbcontent({MetaData, Value})))),
                  MdSame = (lists:sort(dict:to_list(MetaData)) =:= 
                                lists:sort(dict:to_list(MetaData2))),
                  ?assertEqual(true, MdSame),
                  Value = Value2
              end)},
      {"empty content encode decode", 
       ?_test(begin
                  MetaData = dict:new(),
                  Value = <<"test value">>,
                  {MetaData2, Value2} = erlify_rpbcontent(
                                          riakclient_pb:decode_rpbcontent(
                                            riakclient_pb:encode_rpbcontent(
                                              pbify_rpbcontent({MetaData, Value})))),
                  MdSame = (lists:sort(dict:to_list(MetaData)) =:= 
                                lists:sort(dict:to_list(MetaData2))),
                  ?assertEqual(true, MdSame),
                  Value = Value2
              end)},
      {"msg code encode decode",
       ?_test(begin
                  msg_code_encode_decode(0)
              end)},
      {"bucket props encode decode",
       ?_test(begin
                  Props = [{n_val, 99},
                           {allow_mult, true}],
                  Props2 = erlify_rpbbucketprops(
                             riakclient_pb:decode_rpbbucketprops(
                               riakclient_pb:encode_rpbbucketprops(
                                 pbify_rpbbucketprops(Props)))),
                  MdSame = (lists:sort(Props) =:= 
                                lists:sort(Props2)),
                  ?assertEqual(true, MdSame)
              end)},
      {"bucket props encode decode 2",
       ?_test(begin
                  Props = [{n_val, 33},
                           {allow_mult, false}],
                  Props2 = erlify_rpbbucketprops(
                             riakclient_pb:decode_rpbbucketprops(
                               riakclient_pb:encode_rpbbucketprops(
                                 pbify_rpbbucketprops(Props)))),
                  MdSame = (lists:sort(Props) =:= 
                                lists:sort(Props2)),
                  ?assertEqual(true, MdSame)
              end)}
     ]
    }.


msg_code_encode_decode(N) ->
    case msg_type(N) of
        undefined ->
            ok;
        MsgType ->
            ?assertEqual(N, msg_code(MsgType)),
            msg_code_encode_decode(N+1)
    end.

-endif.
  

