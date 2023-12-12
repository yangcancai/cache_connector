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
    [lua_get_batch,
     weak_fetch_batch,
     weak_fetch_batch_overlap,
     strong_fetch_batch,
     strong_fetch_batch_overlap,
     strong_fetch_batch_overlap_expire,
     strong_fetch_batch_error,
     strong_fetch_batch_some_value_empty].

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

lua_get_batch(_) ->
    CmdFn = cmd_fn(),
    {Keys, _, _, _} = get_batch_data(10),
    delete_batch(CmdFn, Keys),
    Owner = <<"test-owner">>,
    {ok,
     [[undefined, <<"LOCKED">>],
      [undefined, <<"LOCKED">>],
      [undefined, <<"LOCKED">>],
      [undefined, <<"LOCKED">>],
      [undefined, <<"LOCKED">>],
      [undefined, <<"LOCKED">>],
      [undefined, <<"LOCKED">>],
      [undefined, <<"LOCKED">>],
      [undefined, <<"LOCKED">>],
      [undefined, <<"LOCKED">>]]} =
        cache_connector:lua_get_batch(CmdFn, Keys, Owner),
    ok.

weak_fetch_batch(_) ->
    CmdFn = cmd_fn(),
    N = 10 + rand:uniform(20),
    Begin = erlang:system_time(1000),
    {Keys, Values1, Values2, Values3} = get_batch_data(N),
    delete_batch(CmdFn, Keys),
    erlang:spawn(fun() ->
                    {ok, Values1} =
                        cache_connector:fetch_batch(#{type => weak,
                                                      cmd_fn => CmdFn,
                                                      keys => Keys,
                                                      ttl => 60000,
                                                      fn => gen_batch_data_func(N, Values1, 200)})
                 end),
    timer:sleep(20),
    {ok, Values1} =
        cache_connector:fetch_batch(#{type => weak,
                                      cmd_fn => CmdFn,
                                      keys => Keys,
                                      ttl => 60000,
                                      fn => gen_batch_data_func(N, Values2, 200)}),
    ?assertEqual(time_since(Begin) > 150, true),
    ok = cache_connector:tag_deleted_batch(CmdFn, Keys),
    Begin1 = erlang:system_time(1000),
    {ok, Values1} =
        cache_connector:fetch_batch(#{type => weak,
                                      cmd_fn => CmdFn,
                                      keys => Keys,
                                      ttl => 60000,
                                      fn => gen_batch_data_func(N, Values3, 200)}),
    ?assertEqual(time_since(Begin1) < 200, true),
    timer:sleep(300),
    {ok, Values3} =
        cache_connector:fetch_batch(#{type => weak,
                                      cmd_fn => CmdFn,
                                      keys => Keys,
                                      ttl => 60000,
                                      fn => gen_batch_data_func(N, Values3, 200)}),
    ok.

weak_fetch_batch_overlap(_) ->
    CmdFn = cmd_fn(),
    N = 100,
    First = 41,
    Second = 60,
    Begin = erlang:system_time(1000),
    Keys = gen_keys(N),
    {Keys1, Values1} = get_batch_data(<<"v1-">>, Keys, 1, Second),
    ?assertEqual(erlang:length(Keys1), erlang:length(Values1)),
    {Keys2, Values2} = get_batch_data(<<"v2-">>, Keys, First, N),
    ?assertEqual(erlang:length(Keys2), erlang:length(Values2)),
    Keys1T = erlang:list_to_tuple(Keys1),
    Keys2T = erlang:list_to_tuple(Keys2),
    lists:foldl(fun(I, Acc) ->
                   ?assertEqual(erlang:element(I - (First - 1), Keys2T), element(I, Keys1T)),
                   Acc
                end,
                ok,
                lists:seq(First, Second)),
    delete_batch(CmdFn, Keys),
    delete_batch(CmdFn, Keys1),
    delete_batch(CmdFn, Keys2),
    erlang:spawn(fun() ->
                    {ok, Values1} =
                        cache_connector:fetch_batch(#{type => weak,
                                                      cmd_fn => CmdFn,
                                                      keys => Keys1,
                                                      ttl => 60000,
                                                      fn => gen_batch_data_func(Values1, 200)})
                 end),
    timer:sleep(20),
    {ok, Values} =
        cache_connector:fetch_batch(#{type => weak,
                                      cmd_fn => CmdFn,
                                      keys => Keys2,
                                      ttl => 60000,
                                      fn => gen_batch_data_func(Values2, 200)}),
    ?assertEqual(time_since(Begin) > 150, true),

    Values1T = erlang:list_to_tuple(Values1),
    Values2T = erlang:list_to_tuple(Values2),
    ValuesT = erlang:list_to_tuple(Values),
    lists:foldl(fun(I, Acc) ->
                   ?assertEqual({I, erlang:element(I - First + 1, Keys2T), element(I, Values1T)},
                                {I, element(I, Keys1T), element(I - First + 1, ValuesT)}),
                   Acc
                end,
                ok,
                lists:seq(First, Second)),
    lists:foldl(fun(I, Acc) ->
                   ?assertEqual(element(I - First + 1, Values2T), element(I - First + 1, ValuesT)),
                   Acc
                end,
                ok,
                lists:seq(Second + 1, N)),

    {KeysDele, _} = get_batch_data(<<>>, Keys, First, Second),
    ok = cache_connector:tag_deleted_batch(CmdFn, KeysDele),
    Begin1 = erlang:system_time(1000),
    {ok, V} =
        cache_connector:fetch_batch(#{type => weak,
                                      cmd_fn => CmdFn,
                                      keys => Keys2,
                                      ttl => 60000,
                                      fn => gen_batch_data_func(Values2, 200)}),
    ?assertEqual(time_since(Begin1) < 200, true),
    VT = erlang:list_to_tuple(V),
    lists:foldl(fun(I, Acc) ->
                   ?assertEqual({I, erlang:element(I - First + 1, Keys2T), element(I, Values1T)},
                                {I, element(I, Keys1T), element(I - First + 1, VT)}),
                   Acc
                end,
                ok,
                lists:seq(First, Second)),
    lists:foldl(fun(I, Acc) ->
                   ?assertEqual(element(I - First + 1, Values2T), element(I - First + 1, VT)),
                   Acc
                end,
                ok,
                lists:seq(Second + 1, N)),

    timer:sleep(300),
    {ok, V1} =
        cache_connector:fetch_batch(#{type => weak,
                                      cmd_fn => CmdFn,
                                      keys => Keys2,
                                      ttl => 60000,
                                      fn => gen_batch_data_func(Values2, 200)}),
    ?assertEqual(Values2, V1),
    ok.

strong_fetch_batch_some_value_empty(_) ->
    CmdFn = cmd_fn(),
    CmdFn1 = cmd_fn1(),
    N = 10 + rand:uniform(20),
    Begin = erlang:system_time(1000),
    {Keys, Values1, Values2, Values3} = get_batch_data(N),
    Values4 = get_value(<<"v4_">>, Keys),
    delete_batch(CmdFn, Keys),
    erlang:spawn(fun() ->
                    {ok, Values1} =
                        cache_connector:fetch_batch(#{type => strong,
                                                      cmd_fn => CmdFn1,
                                                      keys => Keys,
                                                      ttl => 60000,
                                                      fn => gen_batch_data_func(N, Values1, 200)})
                 end),
    timer:sleep(20),
    {ok, Values1} =
        cache_connector:fetch_batch(#{type => strong,
                                      cmd_fn => CmdFn,
                                      keys => Keys,
                                      ttl => 60000,
                                      fn => gen_batch_data_func(N, Values2, 200)}),
    ?assertEqual(time_since(Begin) > 150, true),
    ok = cache_connector:tag_deleted_batch(CmdFn, Keys),
    Begin1 = erlang:system_time(1000),
    {ok, Values3} =
        cache_connector:fetch_batch(#{type => strong,
                                      cmd_fn => CmdFn,
                                      keys => Keys,
                                      ttl => 60000,
                                      fn => gen_batch_data_func(N, Values3, 200)}),
    ?assertEqual(time_since(Begin1) > 150, true),
    {ok, Values3} =
        cache_connector:fetch_batch(#{type => strong,
                                      cmd_fn => CmdFn,
                                      keys => Keys,
                                      ttl => 60000,
                                      fn => gen_batch_data_func(N, Values4, 200)}),
    ok.

strong_fetch_batch(_) ->
    CmdFn = cmd_fn(),
    CmdFn1 = cmd_fn1(),
    N = 10 + rand:uniform(20),
    Begin = erlang:system_time(1000),
    {Keys, Values1, Values2, Values3} = get_batch_data(N),
    Values4 = get_value(<<"v4_">>, Keys),
    delete_batch(CmdFn, Keys),
    erlang:spawn(fun() ->
                    {ok, Values1} =
                        cache_connector:fetch_batch(#{type => strong,
                                                      cmd_fn => CmdFn1,
                                                      keys => Keys,
                                                      ttl => 60000,
                                                      fn => gen_batch_data_func(N, Values1, 200)})
                 end),
    timer:sleep(20),
    {ok, Values1} =
        cache_connector:fetch_batch(#{type => strong,
                                      cmd_fn => CmdFn,
                                      keys => Keys,
                                      ttl => 60000,
                                      fn => gen_batch_data_func(N, Values2, 200)}),
    ?assertEqual(time_since(Begin) > 150, true),
    ok = cache_connector:tag_deleted_batch(CmdFn, Keys),
    Begin1 = erlang:system_time(1000),
    {ok, Values3} =
        cache_connector:fetch_batch(#{type => strong,
                                      cmd_fn => CmdFn,
                                      keys => Keys,
                                      ttl => 60000,
                                      fn => gen_batch_data_func(N, Values3, 200)}),
    ?assertEqual(time_since(Begin1) > 150, true),
    {ok, Values3} =
        cache_connector:fetch_batch(#{type => strong,
                                      cmd_fn => CmdFn,
                                      keys => Keys,
                                      ttl => 60000,
                                      fn => gen_batch_data_func(N, Values4, 200)}),
    ok.

strong_fetch_batch_overlap(_) ->
    CmdFn = cmd_fn(),
    CmdFn1 = cmd_fn1(),
    N = 100,
    First = 41,
    Second = 60,
    Begin = erlang:system_time(1000),
    Keys = gen_keys(N),
    {Keys1, Values1} = get_batch_data(<<"v1-">>, Keys, 1, Second),
    ?assertEqual(erlang:length(Keys1), erlang:length(Values1)),
    {Keys2, Values2} = get_batch_data(<<"v2-">>, Keys, First, N),
    ?assertEqual(erlang:length(Keys2), erlang:length(Values2)),
    Keys1T = erlang:list_to_tuple(Keys1),
    Keys2T = erlang:list_to_tuple(Keys2),
    erlang:spawn(fun() ->
                    {ok, Values1} =
                        cache_connector:fetch_batch(#{type => strong,
                                                      cmd_fn => CmdFn1,
                                                      ttl => 20000,
                                                      keys => Keys1,
                                                      fn => gen_batch_data_func(Values1, 200)})
                 end),
    timer:sleep(20),
    {ok, Values} =
        cache_connector:fetch_batch(#{type => strong,
                                      cmd_fn => CmdFn,
                                      ttl => 20000,
                                      keys => Keys2,
                                      fn => gen_batch_data_func(Values2, 200)}),

    ?assertEqual(time_since(Begin) > 150, true),
    Values1T = erlang:list_to_tuple(Values1),
    Values2T = erlang:list_to_tuple(Values2),
    ValuesT = erlang:list_to_tuple(Values),
    lists:foldl(fun(I, Acc) ->
                   ?assertEqual({I, erlang:element(I - First + 1, Keys2T), element(I, Values1T)},
                                {I, element(I, Keys1T), element(I - First + 1, ValuesT)}),
                   Acc
                end,
                ok,
                lists:seq(First, Second)),
    lists:foldl(fun(I, Acc) ->
                   ?assertEqual(element(I - First + 1, Values2T), element(I - First + 1, ValuesT)),
                   Acc
                end,
                ok,
                lists:seq(Second + 1, N)),

    ok.

strong_fetch_batch_overlap_expire(_) ->
    CmdFn = cmd_fn(),
    N = 100,
    First = 41,
    Second = 60,
    Begin = erlang:system_time(1000),
    Keys = gen_keys(N),
    ok = cache_connector:set_config(delay_delete, 10),
    {Keys1, Values1} = get_batch_data(<<"v1-">>, Keys, 1, Second),
    ?assertEqual(erlang:length(Keys1), erlang:length(Values1)),
    {Keys2, Values2} = get_batch_data(<<"v2-">>, Keys, First, N),
    ?assertEqual(erlang:length(Keys2), erlang:length(Values2)),
    Keys1T = erlang:list_to_tuple(Keys1),
    Keys2T = erlang:list_to_tuple(Keys2),
    {ok, Values1} =
        cache_connector:fetch_batch(#{type => strong,
                                      cmd_fn => CmdFn,
                                      ttl => 2000,
                                      keys => Keys1,
                                      fn => gen_batch_data_func(Values1, 200)}),

    {ok, Values} =
        cache_connector:fetch_batch(#{type => strong,
                                      cmd_fn => CmdFn,
                                      ttl => 2000,
                                      keys => Keys2,
                                      fn => gen_batch_data_func(Values2, 200)}),
    ?assertEqual(time_since(Begin) > 150, true),
    Values1T = erlang:list_to_tuple(Values1),
    Values2T = erlang:list_to_tuple(Values2),
    ValuesT = erlang:list_to_tuple(Values),
    lists:foldl(fun(I, Acc) ->
                   ?assertEqual({I, erlang:element(I - First + 1, Keys2T), element(I, Values1T)},
                                {I, element(I, Keys1T), element(I - First + 1, ValuesT)}),
                   Acc
                end,
                ok,
                lists:seq(First, Second)),
    lists:foldl(fun(I, Acc) ->
                   ?assertEqual(element(I - First + 1, Values2T), element(I - First + 1, ValuesT)),
                   Acc
                end,
                ok,
                lists:seq(Second + 1, N)),
    timer:sleep(1000),
    {ok, Values2} =
        cache_connector:fetch_batch(#{type => strong,
                                      cmd_fn => CmdFn,
                                      ttl => 2000,
                                      keys => Keys2,
                                      fn => gen_batch_data_func(Values2, 200)}),
    ok.

strong_fetch_batch_error(_) ->
    CmdFn = cmd_fn(),
    N = 100,
    Begin = erlang:system_time(1000),
    {Keys, Values1, _Values2, _Values3} = get_batch_data(N),
    {error, not_found} =
        cache_connector:fetch_batch(#{type => strong,
                                      cmd_fn => CmdFn,
                                      ttl => 60000,
                                      keys => Keys,
                                      fn => gen_batch_error_func(200)}),
    {ok, Values1} =
        cache_connector:fetch_batch(#{type => strong,
                                      cmd_fn => CmdFn,
                                      ttl => 60000,
                                      keys => Keys,
                                      fn => gen_batch_data_func(Values1, 200)}),
    ?assertEqual(time_since(Begin) > 150, true),
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

gen_keys(N) ->
    erlang:element(1, get_batch_data(N)).

get_batch_data(Prefix, Keys, Start, End) when is_binary(Prefix) ->
    KeysTuple = erlang:list_to_tuple(Keys),
    {[element(I, KeysTuple) || I <- lists:seq(Start, End)],
     [<<Prefix/binary, (erlang:integer_to_binary(I))/binary>> || I <- lists:seq(Start, End)]}.

get_value(Prefix, Keys) ->
    [<<Prefix/binary, (erlang:integer_to_binary(I))/binary>>
     || I <- lists:seq(1, erlang:length(Keys))].

get_batch_data(N) ->
    {Keys, Values1, Values2, Values3} =
        lists:foldl(fun get_keys/2, {[], [], [], []}, lists:seq(1, N)),
    {lists:reverse(Keys),
     lists:reverse(Values1),
     lists:reverse(Values2),
     lists:reverse(Values3)}.

get_keys(I, {Keys, Values1, Values2, Values3}) ->
    I1 = erlang:integer_to_binary(I),
    KeyPrefix =
        erlang:iolist_to_binary(
            cool_tools:uuid_v1_string()),
    {[<<"cache-connector-batch-key-", KeyPrefix/binary, I1/binary>> | Keys],
     [rand_empty(<<"v1-", I1/binary>>) | Values1],
     [rand_empty(<<"v2-", I1/binary>>) | Values2],
     [rand_empty(<<"v3-", I1/binary>>) | Values3]}.

rand_empty(V) ->
    case rand:uniform(10) < 5 of
        true ->
            <<>>;
        _ ->
            V
    end.

gen_batch_data_func(Values, Sleep) ->
    fun(Idxs) ->
       Values1 = erlang:list_to_tuple(Values),
       timer:sleep(Sleep),
       Res = lists:foldl(fun(I, Acc) -> [erlang:element(I, Values1) | Acc] end, [], Idxs),
       {ok, lists:reverse(Res)}
    end.

gen_batch_error_func(Sleep) ->
    fun(_Idxs) ->
       timer:sleep(Sleep),
       %%    Res = lists:foldl(fun(_I, Acc) ->
       %%    [{error, not_found} | Acc] end,
       %%    [], Idxs),
       {error, not_found}
    end.

gen_batch_data_func(_N, Values, Sleep) ->
    gen_batch_data_func(Values, Sleep).

delete_batch(CmdFn, Keys) ->
    lists:foldl(
        cool_tools_pa:bind(fun do_delete/3, CmdFn), ok, Keys).

do_delete(CmdFn, Key, Acc) ->
    {ok, _} = CmdFn([del, Key]),
    Acc.
