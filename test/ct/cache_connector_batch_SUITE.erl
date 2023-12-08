%%%-------------------------------------------------------------------
%%% @author cam
%%% @copyright (C) 2023, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Dec 2023 5:49 PM
%%%-------------------------------------------------------------------
-module(cache_connector_batch_SUITE).
-author("cam").

-include("cache_connector_ct.hrl").

-compile(export_all).
-define(KEY, <<"cache-connector-test-key">>).

all() ->
    [lua_get_batch,weak_fetch_batch, strong_fetch_batch].

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
lua_get_batch(_) ->
  CmdFn = cmd_fn(),
  {Keys, _, _, _} = get_batch_data(10),
  delete_batch(CmdFn, Keys),
  Owner = <<"test-owner">>,
  {ok,[[undefined,<<"LOCKED">>],
    [undefined,<<"LOCKED">>],
    [undefined,<<"LOCKED">>],
    [undefined,<<"LOCKED">>],
    [undefined,<<"LOCKED">>],
    [undefined,<<"LOCKED">>],
    [undefined,<<"LOCKED">>],
    [undefined,<<"LOCKED">>],
    [undefined,<<"LOCKED">>],
    [undefined,<<"LOCKED">>]]} = cache_connector:lua_get_batch(CmdFn, Keys, Owner),
  ok.
weak_fetch_batch(_) ->
   CmdFn = cmd_fn(),
   N = 10 + rand:uniform(20),
   {Keys, Values1, _Values2, _Values3} = get_batch_data(N),
   delete_batch(CmdFn, Keys),
%%   erlang:spawn(fun() ->
     {ok, Values1} = cache_connector:fetch_batch(#{
                 type => weak,
                 cmd_fn => CmdFn,
                 keys => Keys,
                 ttl => 60000,
                 fn =>  gen_batch_data_func(N, Values1, 200)
               }),
%%                end),
   timer:sleep(1000),
   ok.
strong_fetch_batch(_) ->
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

get_batch_data(N) ->
  {Keys, Values1, Values2, Values3} = lists:foldl(fun get_keys/2, {[], [], [], []}, lists:seq(1, N)),
  {lists:reverse(Keys),
    lists:reverse(Values1),
    lists:reverse(Values2),
    lists:reverse(Values3)}.
get_keys(I, {Keys, Values1, Values2, Values3}) ->
  I1 = erlang:integer_to_binary(I),
  {[<<"cache-connector-batch-key-", I1/binary>>| Keys],
    [<<"v1-", I1/binary>> | Values1],
    [<<"v2-", I1/binary>> | Values2],
    [<<"v3-", I1/binary>> | Values3]}.

gen_batch_data_func(_N, Values, Sleep) ->
  fun(Idxs) ->
    Values1 = erlang:list_to_tuple(Values),
    timer:sleep(Sleep),
    Res = lists:foldl(fun(I, Acc) ->
      [erlang:element(I, Values1)| Acc] end,
      [], Idxs),
    {ok, lists:reverse(Res)}
  end.
delete_batch(CmdFn, Keys) ->
  lists:foldl(cool_tools_pa:bind(fun do_delete/3, CmdFn), ok, Keys).

do_delete(CmdFn, Key, Acc) ->
  {ok, _} = CmdFn([del, Key]),
  Acc.