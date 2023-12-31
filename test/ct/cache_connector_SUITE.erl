%%%-------------------------------------------------------------------
%%% @author Cam

%%% Copyright (c) 2021 by yangcancai(yangcancai0112@gmail.com), All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%       https://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

%%% @doc
%%%
%%% @end
%%% Created : 2023-12-06T09:18:34+00:00
%%%-------------------------------------------------------------------

-module(cache_connector_SUITE).

-author("Cam").

-include("cache_connector_ct.hrl").

-compile(export_all).

-define(KEY, <<"cache-connector-test-key">>).

all() ->
    [
     raw,
     config,
     lock,
     lua_get,
     lua_delete,
     weak_fetch,
     weak_fetch_error,
     strongfetch,
     strongfetch_error,
     fetch_disable_cache ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(cache_connector),
    cool_tools_logger:set_global_loglevel(debug),
    cool_tools_logger:start_default_log(true),
    start_redis(),
    new_meck(),
    Config.

end_per_suite(Config) ->
    del_meck(),
    application:stop(cache_connector),
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

new_meck() ->
    ok.

expect() ->
    ok.

del_meck() ->
    meck:unload().

handle(_Config) ->
    ok.
raw(_) ->
    Key = <<"raw">>,
    V1 = <<"v1">>,
    V2 = <<"v2">>,
    CmdFn = cmd_fn(),
    delete(CmdFn, Key),
  {ok, V1} =  cache_connector:fetch(#{type => weak,
                                                cmd_fn => CmdFn,
                                                key => Key,
                                                ttl => 60000,
                                                fn => gen_data_func(V1, 200)}),
  {ok, V1} = cache_connector:raw_get(CmdFn, Key),
  ok  = cache_connector:raw_set(CmdFn, Key, V2, 60000),
  {ok, V2} = cache_connector:raw_get(CmdFn, Key),
  ok.
config(_) ->
    ?assertEqual(cache_connector:default_config(), cache_connector:get_config()),
    {error, _} = cache_connector:set_config(error_key, 1),
    100 = cache_connector:get_config(locked_sleep),
    ok = cache_connector:set_config(locked_sleep, 1),
    1 = cache_connector:get_config(locked_sleep),
    ok.
fetch_disable_cache(_) ->
  {ok, <<"v">>} = cache_connector:fetch(#{fn => gen_data_func(<<"v">>, 200)}),
  {ok, <<"v1">>} = cache_connector:fetch(#{fn => gen_data_func(<<"v1">>, 200)}),
  ok.
lock(_) ->
    CmdFn = cmd_fn(),
    Key = <<"cache-connector-lock">>,
    Owner = <<"test_owner">>,
    delete(CmdFn, Key),
    {ok, <<"LOCKED">>} = cache_connector:lock_for_update(CmdFn, Key, Owner),
    %% lock by test_owner
    {ok, <<"test_owner">>} = cache_connector:lock_for_update(CmdFn, Key, <<"other">>),
    ok = cache_connector:unlock_for_update(CmdFn, Key, <<"other">>),
    ok.

lua_get(_) ->
    CmdFn = cmd_fn(),
    delete(CmdFn, ?KEY),
    {ok, [undefined, <<"LOCKED">>]} = cache_connector:lua_get(CmdFn, ?KEY, <<"me">>),
    ok = cache_connector:lua_set(CmdFn, ?KEY, <<"123">>, 1000, <<"me">>),
    {ok, [<<"123">>, undefined]} = cache_connector:lua_get(CmdFn, ?KEY, <<"123">>),
    timer:sleep(1200),
    {ok, [undefined, <<"LOCKED">>]} = cache_connector:lua_get(CmdFn, ?KEY, <<"me">>),
    ok.

lua_delete(_) ->
    CmdFn = cmd_fn(),
    delete(CmdFn, ?KEY),
    ok = cache_connector:lua_delete(CmdFn, ?KEY),
    ok.

weak_fetch(_) ->
    Key = <<"weak_fetch1">>,
    V1 = <<"v1">>,
    V2 = <<"v2">>,
    CmdFn = cmd_fn(),
    delete(CmdFn, Key),
    CmdFn1 = cmd_fn1(),
    Begin = erlang:system_time(1000),
    erlang:spawn(fun() ->
                    {ok, V1} =
                        cache_connector:fetch(#{type => weak,
                                                cmd_fn => CmdFn,
                                                key => Key,
                                                ttl => 60000,
                                                fn => gen_data_func(V1, 200)})
                 end),
    timer:sleep(20),
    {ok, V1} =
        cache_connector:fetch(#{type => weak,
                                cmd_fn => CmdFn1,
                                key => Key,
                                ttl => 60000,
                                fn => gen_data_func(V1, 201)}),
    ?assertEqual(time_since(Begin) > 150, true),
    ok = cache_connector:tag_deleted(CmdFn, Key),
    {ok, V1} =
        cache_connector:fetch(#{type => weak,
                                cmd_fn => CmdFn1,
                                key => Key,
                                ttl => 60000,
                                fn => gen_data_func(V2, 200)}),
    timer:sleep(300),
    {ok, V2} =
        cache_connector:fetch(#{type => weak,
                                cmd_fn => CmdFn1,
                                key => Key,
                                ttl => 60000,
                                fn => gen_data_func(<<"ignore">>, 200)}),
    ok.

weak_fetch_error(_) ->
    Key = <<"weak_fetch">>,
    V1 = <<"v1">>,
    CmdFn = cmd_fn(),
    delete(CmdFn, Key),
    Begin = erlang:system_time(1000),
    {error, not_found} =
        cache_connector:fetch(#{type => weak,
                                cmd_fn => CmdFn,
                                key => Key,
                                ttl => 60000,
                                fn => fun() -> {error, not_found} end}),
    {ok, V1} =
        cache_connector:fetch(#{type => weak,
                                cmd_fn => CmdFn,
                                key => Key,
                                ttl => 60000,
                                fn => fun() -> {ok, V1} end}),
    ?assertEqual(time_since(Begin) < 150, true),
    ok.

strongfetch(_) ->
    Key = ?KEY,
    V1 = <<"v1">>,
    V2 = <<"v2">>,
    CmdFn = cmd_fn(),
    delete(CmdFn, ?KEY),
    CmdFn1 = cmd_fn1(),
    Begin = erlang:system_time(1000),
    erlang:spawn(fun() ->
                    {ok, V1} =
                        cache_connector:fetch(#{type => strong,
                                                cmd_fn => CmdFn,
                                                key => Key,
                                                ttl => 60000,
                                                fn => gen_data_func(V1, 200)})
                 end),
    timer:sleep(20),
    {ok, V1} =
        cache_connector:fetch(#{type => strong,
                                cmd_fn => CmdFn1,
                                key => Key,
                                ttl => 60000,
                                fn => gen_data_func(V1, 200)}),
    ?assertEqual(true, time_since(Begin) > 150),
    Begin1 = erlang:system_time(1000),
    ok = cache_connector:tag_deleted(cmd_fn(), Key),
    {ok, V2} =
        cache_connector:fetch(#{type => strong,
                                cmd_fn => CmdFn1,
                                key => Key,
                                ttl => 60000,
                                fn => gen_data_func(V2, 200)}),
    ?assertEqual(true, time_since(Begin1) > 150),
    {ok, V2} =
        cache_connector:fetch(#{type => strong,
                                cmd_fn => CmdFn1,
                                key => Key,
                                ttl => 60000,
                                fn => gen_data_func(<<"ignore">>, 200)}),
    ok.

strongfetch_error(_) ->
    Key = ?KEY,
    Begin = erlang:system_time(1000),
    CmdFn = cmd_fn(),
    delete(CmdFn, ?KEY),
    {error, not_found} =
        cache_connector:fetch(#{type => strong,
                                cmd_fn => CmdFn,
                                key => Key,
                                ttl => 60000,
                                fn => fun() -> {error, not_found} end}),
    {ok, <<"v1">>} =
        cache_connector:fetch(#{type => strong,
                                cmd_fn => CmdFn,
                                key => Key,
                                ttl => 60000,
                                fn => fun() -> {ok, <<"v1">>} end}),
    ?assertEqual(time_since(Begin) < 150, true),
    ok.

start_redis() ->
    {ok, _} = application:ensure_all_started(eredis),
    ok.

cmd_fn() ->
    {ok, P} = eredis:start_link("127.0.0.1", 6380, 0, "123456"),
    fun(L) -> eredis:q(P, L) end.

cmd_fn1() ->
    {ok, P1} = eredis:start_link("127.0.0.1", 6380, 0, "123456"),
    fun(L) -> eredis:q(P1, L) end.

gen_data_func(Value, Sleep) ->
    fun() ->
       timer:sleep(Sleep),
       {ok, Value}
    end.

time_since(Begin) ->
    erlang:system_time(1000) - Begin.

delete(CmdFn, Key) ->
    CmdFn([del, Key]).
