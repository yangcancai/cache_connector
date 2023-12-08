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
    [lua_get, lua_delete, weak_fetch, weak_fetch_error, strongfetch, strongfetch_error, fetch_batch].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(cache_connector),
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
   Key = <<"weak_fetch">>,
   V1 = <<"v1">>,
   V2 = <<"v2">>,
   CmdFn = cmd_fn(),
   delete(CmdFn, Key),
   CmdFn1 = cmd_fn1(),
   Begin = erlang:system_time(1000),
   erlang:spawn(fun() ->
        {ok, V1} = cache_connector:fetch(#{type => weak,
         cmd_fn => CmdFn,
         key => Key,
         ttl => 60000,
         fn => gen_data_func(V1, 200)})
        end),
    timer:sleep(20),
      {ok, V1} = cache_connector:fetch(#{type => weak,
         cmd_fn => CmdFn1,
         key => Key,
         ttl => 60000,
         fn => gen_data_func(V1, 201)}),
     ?assertEqual(time_since(Begin) > 150, true),
     ok = cache_connector:tag_deleted(CmdFn, Key),
   {ok, V2} = cache_connector:fetch(#{type => weak,
         cmd_fn => CmdFn1,
         key => Key,
         ttl => 60000,
         fn => gen_data_func(V2, 200)}),
     timer:sleep(300),
     {ok, V2} = cache_connector:fetch(#{type => weak,
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
   {error, not_found} = cache_connector:fetch(#{type => weak,
         cmd_fn => CmdFn,
         key => Key,
         ttl => 60000,
         fn => fun() -> {error, not_found} end}),
   {ok, V1} = cache_connector:fetch(#{type => weak,
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
        {ok, V1} = cache_connector:fetch(#{type => strong,
         cmd_fn => CmdFn,
         key => Key,
         ttl => 60000,
         fn => gen_data_func(V1, 200)})
        end),
    timer:sleep(20),
      {ok, V1} = cache_connector:fetch(#{type => strong,
         cmd_fn => CmdFn1,
         key => Key,
         ttl => 60000,
         fn => gen_data_func(V1, 200)}),
     ?assertEqual(true, time_since(Begin) > 150),
     Begin1 = erlang:system_time(1000),
     ok = cache_connector:tag_deleted(cmd_fn(), Key),
     {ok, V2} = cache_connector:fetch(#{type => strong,
         cmd_fn => CmdFn1,
         key => Key,
         ttl => 60000,
         fn => gen_data_func(V2, 200)}),
    ?assertEqual(true, time_since(Begin1) > 150),
    {ok, V2} = cache_connector:fetch(#{type => strong,
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
  {error, not_found} = cache_connector:fetch(#{
       type => strong,
       cmd_fn => CmdFn,
       key => Key,
       ttl => 60000,
       fn => fun() -> {error, not_found} end}),
  {ok, <<"v1">>} = cache_connector:fetch(#{
       type => strong,
       cmd_fn => CmdFn,
       key => Key,
       ttl => 60000,
       fn => fun() -> {ok, <<"v1">>} end}),
    ?assertEqual(time_since(Begin) < 150, true),
  ok.
fetch_batch(_) ->
    ok.

start_redis() ->
    {ok, _} = application:ensure_all_started(eredis),
    ok.
cmd_fn() ->
    {ok, P} = eredis:start_link("127.0.0.1", 6380, 0, "123456"),
    fun(L) ->
        eredis:q(P, L)
    end.
cmd_fn1() ->
  {ok, P1} = eredis:start_link("127.0.0.1", 6380, 0, "123456"),
    fun(L) ->
        eredis:q(P1, L)
    end.
gen_data_func(Value, Sleep) ->
    fun() ->
     timer:sleep(Sleep),
        {ok, Value}
    end.

time_since(Begin) ->
  erlang:system_time(1000) - Begin.

delete(CmdFn, Key) ->
  CmdFn([del, Key]).