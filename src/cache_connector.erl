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

-module(cache_connector).
%% LockSleep is the sleep interval time if try lock failed. default is 100ms
-define(LOCKED_SLEEP, 100).
%% LockExpire is the expire time for the lock which is allocated when updating cache. default is 3s
%% should be set to the max of the underling data calculating time.
-define(LOCKED_EXPIRE, 3000).
%%EmptyExpire is the expire time for empty result. default is 60s
-define(EMPTY_EXPIRE, application:get_env(cache_connector, empty_expire, 60000)).
%% Delay is the delay delete time for keys that are tag deleted. default is 10s
-define(DELAY_DELETE, 10000).
%% WaitReplicas is the number of replicas to wait for. default is 0
%% if WaitReplicas is > 0, it will use redis WAIT command to wait for TagAsDeleted synchronized.
-define(WAIT_REPLICAS, 0).
%% WaitReplicasTimeout is the number of replicas to wait for. default is 3000ms
%% if WaitReplicas is > 0, WaitReplicasTimeout is the timeout for WAIT command.
-define(WAIT_REPLICAS_TIMEOUT, 3000).
%% RandomExpireAdjustment is the random adjustment for the expire time. default 0.1
%% if the expire time is set to 600s, and this value is set to 0.1, then the actual expire time will be 540s - 600s
%% solve the problem of cache avalanche.
-define(RandomExpireAdjustment, 0.1).
-include_lib("kernel/include/logger.hrl").
-author("Cam").

-export([
  fetch/1,
  tag_deleted/2,
  raw_get/2,
  raw_set/4,
  lua_get/3,
  lua_set/5,
  lock_for_update/3,
  unlock_for_update/3,
  fetch_batch/1,
  tag_deleted_batch/2]).

%% [undefined, <<"LOCKED">>]
%% [<<Value>>, undefined]
-type lua_get_res()  :: [undefined | binary()].
fetch(#{type := strong,cmd_fn := CmdFn, key := Key, ttl := TTL, fn := Fn}) ->
  strong_fetch(CmdFn, Key, TTL, Fn);
%% disable cache read
fetch(#{type := weak,cmd_fn := CmdFn, key := Key, ttl := TTL, fn := Fn}) ->
  weak_fetch(CmdFn, Key, TTL, Fn);
fetch(#{fn := Fn}) ->
  Fn().
fetch_batch(#{type := strong, cmd_fn := CmdFn, keys := Keys, ttl := TTL, fn := Fn}) ->
  strong_fetch_batch(CmdFn, Keys, TTL, Fn);
fetch_batch(#{type := weak, cmd_fn := CmdFn, keys := Keys, ttl := TTL, fn := Fn}) ->
  weak_fetch_batch(CmdFn, Keys, TTL, Fn);
fetch_batch(#{keys := Keys, fn := Fn}) ->
  Fn(Keys).
tag_deleted(CmdFn, Key) ->
  case ?WAIT_REPLICAS > 0 of
      true ->
        Res = lua_delete(CmdFn, Key),
        case Res of
            ok ->
               CmdFn([wait, ?WAIT_REPLICAS, ?WAIT_REPLICAS_TIMEOUT]);
            E ->
              E
        end;
      _->
        lua_delete(CmdFn, Key)
  end.

tag_deleted_batch(CmdFn, Keys) ->
  case ?WAIT_REPLICAS > 0 of
      true ->
         case lua_delete_batch(CmdFn, Keys) of
             ok ->
              CmdFn([wait, ?WAIT_REPLICAS, to_seconds(?WAIT_REPLICAS_TIMEOUT)]);
             E ->
              E
        end;
     false ->
      lua_delete_batch(CmdFn, Keys)
  end.

raw_get(CmdFn, Key) ->
  CmdFn([hget, Key, value]).

raw_set(CmdFn, Key, Value, TTL) ->
  case CmdFn([hset, Key, value, Value]) of
       ok ->
         CmdFn([expire, Key, to_seconds(TTL)]);
       E -> E
  end.
-spec lua_get(CmdFn :: fun((Elem :: L) -> Return),
    Key :: binary(),
    Owner :: binary()) -> Res when
    Return :: {ok, lua_get_res()} | {error, term()},
    Res :: {ok, lua_get_res()}  | {error, term()},
    L :: list().
lua_get(CmdFn, Key, Owner) ->
  Script = <<" -- luaGet
	local v = redis.call('HGET', KEYS[1], 'value')
	local lu = redis.call('HGET', KEYS[1], 'lockUntil')
	if lu ~= false and tonumber(lu) < tonumber(ARGV[1]) or lu == false and v == false then
		redis.call('HSET', KEYS[1], 'lockUntil', ARGV[2])
		redis.call('HSET', KEYS[1], 'lockOwner', ARGV[3])
		return { v, 'LOCKED' }
	end
	return {v, lu}">>,
  Now = erlang:system_time(1000),
  CmdFn([eval, Script, 1, Key, Now, erlang:integer_to_binary(Now + to_seconds(?LOCKED_EXPIRE)), Owner]).

lua_get_batch(CmdFn, Keys, Owner) ->
  Script = <<"-- luaGetBatch
    local rets = {}
    for i, key in ipairs(KEYS)
    do
        local v = redis.call('HGET', key, 'value')
        local lu = redis.call('HGET', key, 'lockUntil')
        if lu ~= false and tonumber(lu) < tonumber(ARGV[1]) or lu == false and v == false then
            redis.call('HSET', key, 'lockUntil', ARGV[2])
            redis.call('HSET', key, 'lockOwner', ARGV[3])
            table.insert(rets, { v, 'LOCKED' })
        else
            table.insert(rets, {v, lu})
        end
    end
    return rets">>,
  CmdFn([eval, Script, erlang:length(Keys) | Keys]  ++ [erlang:system_time(1), erlang:system_time(1) + to_seconds(?LOCKED_EXPIRE), Owner]).

-spec lua_set(fun((L:: T) -> Return),
    binary(),
    binary(),
    pos_integer(),
    binary()) -> Res when
    T :: list(),
    Return :: {ok, term()} | {error, term()},
    Res :: ok | {error, term()}.
lua_set(CmdFn, Key, Value, TTL, Owner) ->
  Script = <<"-- luaSet
	local o = redis.call('HGET', KEYS[1], 'lockOwner')
	if o ~= ARGV[2] then
			return
	end
	redis.call('HSET', KEYS[1], 'value', ARGV[1])
	redis.call('HDEL', KEYS[1], 'lockUntil')
	redis.call('HDEL', KEYS[1], 'lockOwner')
	redis.call('EXPIRE', KEYS[1], ARGV[3])
  ">>,
  case CmdFn([eval, Script, 1, Key, Value, Owner, erlang:integer_to_binary(to_seconds(TTL))]) of
    {ok, _}  -> ok;
    E -> E
  end.

lua_set_batch(CmdFn, Keys, Values, TTLS, Owner) ->
  Script = <<"-- luaSetBatch
    local n = #KEYS
    for i, key in ipairs(KEYS)
    do
        local o = redis.call('HGET', key, 'lockOwner')
        if o ~= ARGV[1] then
                return
        end
        redis.call('HSET', key, 'value', ARGV[i+1])
        redis.call('HDEL', key, 'lockUntil')
        redis.call('HDEL', key, 'lockOwner')
        redis.call('EXPIRE', key, ARGV[i+1+n])
    end">>,
  CmdFn([eval, Script, erlang:length(Keys), Keys, Owner] ++  Values ++ TTLS).
-spec lock_for_update(fun((L) -> (Res)),Key, Owner) ->
  {ok, binary()} | {error, term()} when
  L :: list(),
  Key :: binary(),
  Owner :: binary(),
  Res :: {ok, binary()} | {error, term()}.
lock_for_update(CmdFn, Key, Owner) ->
  LockUntil = math:pow(10,10),
  Script = <<"-- luaLock
	local lu = redis.call('HGET', KEYS[1], 'lockUntil')
	local lo = redis.call('HGET', KEYS[1], 'lockOwner')
	if lu == false or tonumber(lu) < tonumber(ARGV[2]) or lo == ARGV[1] then
		redis.call('HSET', KEYS[1], 'lockUntil', ARGV[2])
		redis.call('HSET', KEYS[1], 'lockOwner', ARGV[1])
		return 'LOCKED'
	end
	return lo">>,
  CmdFn([eval, Script, 1, Key, Owner, erlang:trunc(LockUntil)]).

-spec unlock_for_update(CmdFn :: fun((L) -> (Res)),
  Key :: binary(),
  Owner :: binary()) -> ok | {error, term()} when
  L :: list(),
  Res :: {ok, undefined} | {error, term()}.
unlock_for_update(CmdFn, Key, Owner) ->
  Script = <<" -- luaUnlock
	local lo = redis.call('HGET', KEYS[1], 'lockOwner')
	if lo == ARGV[1] then
		redis.call('HSET', KEYS[1], 'lockUntil', 0)
		redis.call('HDEL', KEYS[1], 'lockOwner')
		redis.call('EXPIRE', KEYS[1], ARGV[2])
	end
">>,
  case CmdFn([eval, Script, 1, Key, Owner, to_seconds(?LOCKED_EXPIRE)]) of
    {ok, _}  -> ok;
    E -> E
  end.

-spec lua_delete(fun((L) -> (Res)),
    Key) -> ok | {error, term()} when
  L :: list(),
  Key :: binary(),
  Res :: {ok, term()} | {error, term()}.
lua_delete(CmdFn, Key) ->
  Script = <<" --  delete
		redis.call('HSET', KEYS[1], 'lockUntil', 0)
		redis.call('HDEL', KEYS[1], 'lockOwner')
		redis.call('EXPIRE', KEYS[1], ARGV[1])">>,
  case CmdFn([eval, Script, 1, Key, to_seconds(?DELAY_DELETE)]) of
    {ok, _}  -> ok;
    E -> E
  end.

lua_delete_batch(CmdFn, Keys) ->
  Script = <<" -- luaDeleteBatch
		for i, key in ipairs(KEYS) do
			redis.call('HSET', key, 'lockUntil', 0)
			redis.call('HDEL', key, 'lockOwner')
			redis.call('EXPIRE', key, ARGV[1])
		end">>,
  CmdFn([eval, Script, Keys, [to_seconds(?DELAY_DELETE)]]).

strong_fetch(CmdFn, Key, TTL, FetchDbFn) ->
  strong_fetch(CmdFn, Key, TTL, FetchDbFn, cool_tools:to_binary(cool_tools:uuid_v1_string())).
strong_fetch(CmdFn, Key, TTL, FetchDbFn, Owner) ->
  case lua_get(CmdFn, Key, Owner) of
    {ok, [_,Row |_]}  when Row /= undefined, Row /= <<"LOCKED">> ->
      %% sleep
       timer:sleep(?LOCKED_SLEEP),
       strong_fetch(CmdFn, Key, TTL, FetchDbFn, Owner);
    {ok, [V, Row |_]}  when Row /= <<"LOCKED">> ->
       {ok, V};
    {ok, _} ->
       %% fetch_new
       fetch_new(CmdFn, Key, TTL, Owner, FetchDbFn);
    Error -> Error
  end.

strong_fetch_batch(CmdFn, Keys, TTL, FetchDbFn) ->
  Owner = cool_tools:to_binary(cool_tools:uuid_v1_string()),
  case lua_get_batch(CmdFn, Keys, Owner) of
    {ok, Res}  ->
      {ToGet, ToFetch, Result, _} = lists:foldl(fun do_fetch_batch/2,{[], [], #{}, 1} ,Res),
      case do_to_fetch(CmdFn, Keys, ToFetch, TTL, Owner, FetchDbFn) of
        {ok, Fetched} ->
           Result1 = lists:foldl(cool_tools_pa:bind(fun do_result/3, Fetched), Result, ToFetch),
           do_to_get(CmdFn, Keys, ToGet, TTL, Owner, Result1, FetchDbFn);
        FetchErr ->
          FetchErr
      end;
    E ->
       E
  end.
do_result(Fetched, I, Result) ->
  Result#{I => proplists:get_value(I, Fetched)}.

do_to_get(CmdFn, Keys, ToGet, TTL, Owner, Result, FetchDbFn) ->
  List = cool_tools:pmap(cool_tools_pa:bind(fun do_to_get1/4, CmdFn, Keys, Owner), ToGet),
  {NewResult, NeedFetch, Err} = lists:foldl(fun to_get_res/2, {Result, [], ok}, List),
  case Err of
       error ->
         {error, lua_get_error};
       ok ->
         case do_to_fetch(CmdFn, Keys, NeedFetch, TTL, Owner, FetchDbFn) of
           {ok, Fetched}  ->
             {ok, lists:foldl(cool_tools_pa:bind(fun do_result/3, Fetched), NewResult, NeedFetch)};
           E -> E
         end
  end.

to_get_res({ok, I, V}, {Result, NeedFetch, Ok}) ->
  {Result#{I => V}, NeedFetch, Ok};
to_get_res({ok, I, need_fetch}, {Result, NeedFetch, Ok}) ->
  {Result, [I|NeedFetch], Ok};
to_get_res(_, {Result, NeedFetch, _Ok}) ->
  {Result, NeedFetch, error}.

do_to_get1(CmdFn, Keys, Owner, I) ->
  case wait_to_lua_get(CmdFn, maps:get(I, Keys), Owner) of
    %% normal value
    {ok, [A, B]} when B /= <<"LOCKED">> ->
      {ok, I, A};
    %% locked for update, need to fetch
    {ok, [_, _B]} ->
      {ok, I, need_fetch};
    E ->
      E
  end.

wait_to_lua_get(CmdFn, Key, Owner) ->
  case lua_get(CmdFn, Key, Owner) of
     {ok, [A, B]} when A /= nil , B /= <<"LOCKED">> ->
       timer:sleep(?LOCKED_SLEEP),
       wait_to_lua_get(CmdFn, Key, Owner);
    {ok, R} ->
      {ok, R};
     E ->
      E
  end.

do_to_fetch(_CmdFn, _Keys, [], _TTL, _Owner, _FetchDbFn) ->
  {ok, []};
do_to_fetch(CmdFn, Keys, ToFetch, TTL, Owner, FetchDbFn) ->
  case FetchDbFn(ToFetch) of
    {ok, Data}  ->
      {BatchKeys, BatchValues, BatchExpires} = lists:foldl(
        cool_tools_pa:bind(fun do_to_fetch1/6, CmdFn, erlang:list_to_tuple(Data), erlang:list_to_tuple(Keys), TTL),
        {[], [], []}, ToFetch),
      lua_set_batch(CmdFn, BatchKeys, BatchValues, BatchExpires, Owner),
      {ok, Data};
    E ->
      Keys1 = erlang:list_to_tuple(Keys),
      lists:foldl(fun(I, Acc) ->
        unlock_for_update(CmdFn, erlang:element(I, Keys1), Owner),
        Acc end, ok, ToFetch),
      E
  end.
do_to_fetch1(CmdFn, Data, Keys, TTL, I, {BatchKeys, BatchValues, BatchExpires})  ->
  Ex = to_seconds(TTL - ?DELAY_DELETE - erlang:trunc(rand:uniform() * ?RandomExpireAdjustment * TTL)),
  case erlang:element(I, Data) of
    <<>> ->
      case ?EMPTY_EXPIRE of
          0 ->
            CmdFn([del, maps:get(I, Keys)]),
            {BatchKeys, BatchValues, BatchExpires};
          _->
            {[erlang:element(I, Keys) | BatchKeys], [ <<>> | BatchValues], [ to_seconds(?EMPTY_EXPIRE) | BatchExpires]}
      end;
    V ->
      {[element(I, Keys) | BatchKeys], [ V | BatchValues], [ Ex | BatchExpires]}
  end.
do_fetch_batch([A, undefined], {ToGet, ToFetch, Result, I}) ->
  {ToGet, ToFetch, Result#{I => A}, I + 1};
do_fetch_batch([_A, <<"LOCKED">>], {ToGet, ToFetch, Result, I}) ->
  {ToGet, [ I | ToFetch],  Result, I + 1};
%% locked by other
do_fetch_batch([_A, _], {ToGet, ToFetch, Result, I}) ->
  {[I | ToGet], ToFetch,Result, I + 1}.

weak_fetch(CmdFn, Key, TTL, FetchDbFn) ->
  weak_fetch(CmdFn, Key, TTL, FetchDbFn, cool_tools:to_binary(cool_tools:uuid_v1_string())).
weak_fetch(CmdFn, Key, TTL, FetchDbFn, Owner) ->
  case lua_get(CmdFn, Key, Owner) of
    {ok, [A, B| _]}  when A == nil , B /= <<"LOCKED">> ->
      timer:sleep(?LOCKED_SLEEP),
      weak_fetch(CmdFn, Key, TTL, FetchDbFn);
    {ok, [A, B | _]} when B /= <<"LOCKED">> ->
      {ok, A};
    {ok, [A, _B | _]} when A /= nil ->
      fetch_new(CmdFn, Key, TTL, Owner, FetchDbFn);
    E ->
      E
  end.

weak_fetch_batch(CmdFn, Keys, TTL, FetchDbFn) ->
  Owner = cool_tools:to_binary(cool_tools:uuid_v1_string()),
  case lua_get_batch(CmdFn, Keys, Owner) of
    {ok, Res}  ->
      {Result, ToFetchAsync, ToFetch1, ToGet, _} = lists:foldl(fun do_weak_fetch_batch/2, {#{}, [], [], [], 1}, Res),
      to_fetch_async(CmdFn, Keys, lists:reverse(ToFetchAsync), TTL, Owner, FetchDbFn),
      ToFetch = lists:reverse(ToFetch1),
      case do_to_fetch(CmdFn, Keys, ToFetch, TTL, Owner, FetchDbFn) of
        {ok, Fetched} ->
           L = lists:zip(ToFetch, Fetched),
           Result1 = lists:foldl(cool_tools_pa:bind(fun do_result/3, L), Result, ToFetch),
           do_weak_to_get(CmdFn, Keys, lists:reverse(ToGet), TTL, Owner, Result1, FetchDbFn);
        {error, FetchErr} ->
          {error, FetchErr}
      end;
    {error, E} ->
      {error, E}
  end.

to_fetch_async(CmdFn, Keys, ToFetchAsync, TTL, Owner, FetchDbFn) ->
  erlang:spawn(fun() ->
     do_to_fetch(CmdFn, Keys, ToFetchAsync, TTL, Owner, FetchDbFn)
  end).

do_weak_fetch_batch([undefined, <<"LOCKED">>], {Result, ToFetchAsync, ToFetch, ToGet, I}) ->
  {Result, ToFetchAsync, [I | ToFetch], ToGet, I + 1};
do_weak_fetch_batch([undefined, _B], {Result, ToFetchAsync, ToFetch, ToGet, I}) ->
  {Result, ToFetchAsync, ToFetch, [I | ToGet], I + 1};
do_weak_fetch_batch([A, <<"LOCKED">>], {Result, ToFetchAsync, ToFetch, ToGet, I}) ->
  {Result#{I => A}, [ I | ToFetchAsync], ToFetch, ToGet, I + 1};
do_weak_fetch_batch([A, _], {Result, ToFetchAsync, ToFetch, ToGet, I}) ->
  {Result#{I => A}, ToFetchAsync, ToFetch, ToGet, I + 1}.

fetch_new(CmdFn, Key, TTL, Owner, FetchDbFn) ->
  case FetchDbFn() of
    {ok, <<>>} ->
       case ?EMPTY_EXPIRE of
            0 ->
              ok = CmdFn([del, Key]),
              {ok, ""};
           _->
              lua_set(CmdFn, Key, <<>>, ?EMPTY_EXPIRE, Owner),
              {ok, ""}
      end;
    {ok, Result} ->
      lua_set(CmdFn, Key, Result, TTL, Owner),
      {ok, Result};
    Error ->
        %% unlockForUpdate
      unlock_for_update(CmdFn, Key, Owner),
      Error
  end.

to_seconds(Time) when is_integer(Time)->
  Time div 1000.

do_weak_to_get(CmdFn, Keys, ToGet, TTL, Owner, Result, FetchDbFn) ->
  List = cool_tools:pmap(cool_tools_pa:bind(fun do_weak_to_get1/4, CmdFn, Keys, Owner), ToGet),
  {NewResult, NeedFetch, NeedFetchAsync, Err} = lists:foldl(fun to_weak_get_res/2, {Result, [], [], ok}, List),
  case Err of
       error ->
         {error, lua_get_error};
       ok ->
         to_fetch_async(CmdFn, Keys, NeedFetchAsync, TTL, Owner, FetchDbFn),
         case do_to_fetch(CmdFn, Keys, NeedFetch, TTL, Owner, FetchDbFn) of
           {ok, NeedFetchDataList}  ->
              L  = lists:zip(NeedFetch, NeedFetchDataList),
             {ok, maps:values(lists:foldl(cool_tools_pa:bind(fun do_result/3, L), NewResult, NeedFetch))};
           {error, E} -> {error, E}
         end
  end.
to_weak_get_res({ok, I, need_fetch}, {Result, NeedFetch, NeedFetchAsync, Ok}) ->
  {Result, [I|NeedFetch], NeedFetchAsync, Ok};
to_weak_get_res({ok, I, need_fetch_async}, {Result, NeedFetch, NeedFetchAsync, Ok}) ->
  {Result, NeedFetch, [I | NeedFetchAsync], Ok};
to_weak_get_res({ok, I, V}, {Result, NeedFetch, NeedFetchAsync, Ok}) ->
  {Result#{I => V}, NeedFetch, NeedFetchAsync, Ok};
to_weak_get_res(_, {Result, NeedFetch, NeedFetchAsync, _Ok}) ->
  {Result, NeedFetch, NeedFetchAsync, error}.

do_weak_to_get1(CmdFn, Keys, Owner, I) ->
  case weak_wait_to_lua_get(CmdFn, maps:get(I, Keys), Owner) of
    %% normal value
    {ok, [A, B]} when B /= <<"LOCKED">> ->
      {ok, I, A};
    %% locked for update, need to fetch
    {ok, [A, _B]} when A == nil->
      {ok, I, need_fetch};
    {ok, _} ->
      {ok, I, need_fetch_async};
    E ->
      E
  end.
weak_wait_to_lua_get(CmdFn, Key, Owner) ->
  case lua_get(CmdFn, Key, Owner) of
     {ok, [A, B]} when A == nil , B /= <<"LOCKED">> ->
       timer:sleep(?LOCKED_SLEEP),
       weak_wait_to_lua_get(CmdFn, Key, Owner);
    {ok, R} ->
      {ok, R};
     E ->
      E
  end.
