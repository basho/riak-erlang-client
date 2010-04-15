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

%% @doc protocol buffer utilities

-module(riakc_pb).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include("riakclient_pb.hrl").
-include("riakc_obj.hrl").
-compile([export_all]).

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

%% Create an iolist of msg code and protocol buffer message
encode(Msg) when is_atom(Msg) ->
    [msg_code(Msg)];
encode(Msg) when is_tuple(Msg) ->
    MsgType = element(1, Msg),
    [msg_code(MsgType) | riakclient_pb:iolist(MsgType, Msg)].
 
%% Decode a protocol buffer message given its type - if no bytes
%% return the atom for the message code
decode(MsgCode, <<>>) ->
    msg_type(MsgCode);
decode(MsgCode, MsgData) ->
    riakclient_pb:decode(msg_type(MsgCode), MsgData).

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
msg_type(_) -> undefined.
    
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
msg_code(rpblistkeysresp)        -> 18.

%% ===================================================================
%% Encoding/Decoding
%% ===================================================================
    
%% Convert a list of {MetaData,Value} pairs to protocol buffers
pbify_rpbcontents([], Acc) ->
    lists:reverse(Acc);
pbify_rpbcontents([Content | Rest], Acc) ->
    pbify_rpbcontents(Rest, [pbify_rpbcontent(Content) | Acc]).

%% Convert a metadata/value pair into an #rpbcontent{} record    
pbify_rpbcontent({Metadata, Value}) ->
    dict:fold(fun pbify_rpbcontent_entry/3, #rpbcontent{value = Value}, Metadata).

%% Convert the metadata dictionary entries to protocol buffers
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
pbify_rpbcontent_entry(_Key, _Value, PbContent) ->
    %% Ignore unknown metadata - need to add to RpbContent if it needs to make it
    %% to/from the client
    PbContent.

%% Convert a list of rpbcontent pb messages to a list of [{MetaData,Value}] tuples
erlify_rpbcontents(RpbContents) ->
    [erlify_rpbcontent(RpbContent) || RpbContent <- RpbContents].

%% Convert an rpccontent pb message to an erlang {MetaData,Value} tuple
erlify_rpbcontent(PbC) ->
    ErlMd0 = dict:new(),
    case PbC#rpbcontent.content_type of
        undefined ->
            ErlMd1 = ErlMd0;
        ContentType ->
            ErlMd1 = dict:store(?MD_CTYPE, binary_to_list(ContentType), ErlMd0)
    end,
    case PbC#rpbcontent.charset of
        undefined ->
            ErlMd2 = ErlMd1;
        Charset ->
            ErlMd2 = dict:store(?MD_CHARSET, binary_to_list(Charset), ErlMd1)
    end,
    case PbC#rpbcontent.content_encoding of
        undefined ->
            ErlMd3 = ErlMd2;
        Encoding ->
            ErlMd3 = dict:store(?MD_ENCODING, binary_to_list(Encoding), ErlMd2)
    end,
    case PbC#rpbcontent.vtag of
        undefined ->
            ErlMd4 = ErlMd3;
        Vtag ->
            ErlMd4 = dict:store(?MD_VTAG, binary_to_list(Vtag), ErlMd3)
    end,
    case PbC#rpbcontent.links of
        undefined ->
            ErlMd5 = ErlMd4;
        PbLinks ->
            Links = [erlify_rpblink(E) || E <- PbLinks],
            ErlMd5 = dict:store(?MD_LINKS, Links, ErlMd4)
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
            ErlMd6 = dict:store(?MD_LASTMOD, {Msec,Sec,Usec}, ErlMd5)
    end,
    case PbC#rpbcontent.usermeta of
        undefined ->
            ErlMd = ErlMd6;
        PbUserMeta ->
            UserMeta = [erlify_rpbpair(E) || E <- PbUserMeta],
            ErlMd = dict:store(?MD_USERMETA, UserMeta, ErlMd6)
    end,

    {ErlMd, PbC#rpbcontent.value}.
    

%% Convert {K,V} tuple to protocol buffers
pbify_rpbpair({K,V}) ->
    #rpbpair{key = K, value = V}.

%% Convert RpbPair PB message to erlang {K,V} tuple
erlify_rpbpair(#rpbpair{key = K, value = V}) ->
    {binary_to_list(K), binary_to_list(V)}.
    
%% Covnert erlang link tuple to RpbLink PB message
pbify_rpblink({{B,K},T}) ->
    #rpblink{bucket = B, key = K, tag = T}.

%% Convert RpbLink PB message to erlang link tuple
erlify_rpblink(#rpblink{bucket = B, key = K, tag = T}) ->
    {{B,K},T}.

%% Convert a true/false, 1/0 etc to a 1/0 for protocol buffers bool
pbify_bool(true) ->
    1;
pbify_bool(false) ->
    0;
pbify_bool(N) when is_integer(N) ->
    case N =:= 0 of
        true ->
            1;
        false ->
            0
    end.

%% Make sure an atom/string/binary is definitely a binary
to_binary(A) when is_atom(A) ->
    list_to_binary(atom_to_list(A));
to_binary(L) when is_list(L) ->
    list_to_binary(L);
to_binary(B) when is_binary(B) ->
    B.


%% Convert <<"_">> to '_', otherwise leaves binary alone
maybe_underscore_atom(<<"_">>) ->
    '_';
maybe_underscore_atom(Bin) ->
    Bin.

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
                  MdSame = (dict:to_list(MetaData) =:= dict:to_list(MetaData2)),
                  MdSame = true,
                  Value = Value2
              end)}
     ]}.

-endif.
  

