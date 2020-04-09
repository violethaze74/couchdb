% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(aegis_basic_test).

-include_lib("eunit/include/eunit.hrl").

-define(DB, #{uuid => <<"foo">>}).

get_set_db_test() ->
    Db = erlfdb_util:get_test_db([empty]),
    Key = <<"foo">>,
    Value = <<"bar">>,
    ?assertEqual(ok, aegis:set(?DB, Db, Key, Value)),
    ?assertNotEqual(Value, erlfdb:get(Db, Key)),
    ?assertEqual(Value, aegis:get(?DB, Db, Key)).

get_set_tx_test() ->
    Db = erlfdb_util:get_test_db([empty]),
    Key = <<"foo">>,
    Value = <<"bar">>,
    ?assertEqual(ok, aegis:set(?DB, Db, Key, Value)),
    Tx = erlfdb:create_transaction(Db),
    Future = aegis:get(?DB, Tx, Key),
    ?assertEqual(Value, aegis:wait(Future)).

get_range_test() ->
    Db = erlfdb_util:get_test_db([empty]),
    Rows = [{<<"foo1">>, <<"bar1">>},
            {<<"foo2">>, <<"bar2">>},
            {<<"foo3">>, <<"bar3">>}],
    [aegis:set(?DB, Db, K, V) || {K, V} <- Rows],
    ?assertNotEqual(Rows, erlfdb:get_range(Db, <<"foo1">>, <<"foo9">>)),
    ?assertEqual(Rows, aegis:get_range(?DB, Db, <<"foo1">>, <<"foo9">>)).

get_range_startswith_test() ->
    Db = erlfdb_util:get_test_db([empty]),
    Rows = [{<<"foo1">>, <<"bar1">>},
            {<<"foo2">>, <<"bar2">>},
            {<<"foo3">>, <<"bar3">>}],
    [aegis:set(?DB, Db, K, V) || {K, V} <- Rows],
    ?assertNotEqual(Rows, erlfdb:get_range_startswith(Db, <<"foo">>)),
    ?assertEqual(Rows, aegis:get_range_startswith(?DB, Db, <<"foo">>)).

fold_range_test() ->
    Db = erlfdb_util:get_test_db([empty]),
    Rows = [{<<"foo1">>, <<"bar1">>},
            {<<"foo2">>, <<"bar2">>},
            {<<"foo3">>, <<"bar3">>}],
    {_, Values} = lists:unzip(Rows),
    [aegis:set(?DB, Db, K, V) || {K, V} <- Rows],
    Fun = fun(NewRows, Acc) -> [NewRows | Acc] end,
    ?assertNotEqual(Values, lists:reverse(erlfdb:fold_range(Db, <<"foo1">>, <<"foo9">>, Fun, []))),
    ?assertEqual(Values, lists:reverse(aegis:fold_range(?DB, Db, <<"foo1">>, <<"foo9">>, Fun, []))).

fold_range_future_test() ->
    Db = erlfdb_util:get_test_db([empty]),
    Rows = [{<<"foo1">>, <<"bar1">>},
            {<<"foo2">>, <<"bar2">>},
            {<<"foo3">>, <<"bar3">>}],
    {_, Values} = lists:unzip(Rows),
    [aegis:set(?DB, Db, K, V) || {K, V} <- Rows],
    Fun = fun(NewRows, Acc) -> [NewRows | Acc] end,
    Tx = erlfdb:create_transaction(Db),
    Future = aegis:fold_range_future(?DB, Tx, <<"foo1">>, <<"foo9">>, []),
    ?assertEqual(Values, lists:reverse(aegis:fold_range_wait(Tx, Future, Fun, []))).


