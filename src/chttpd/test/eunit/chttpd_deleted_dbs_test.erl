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

-module(chttpd_deleted_dbs_test).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(USER, "chttpd_db_test_admin").
-define(PASS, "pass").
-define(AUTH, {basic_auth, {?USER, ?PASS}}).
-define(CONTENT_JSON, {"Content-Type", "application/json"}).


setup() ->
    Hashed = couch_passwords:hash_admin_password(?PASS),
    ok = config:set("admins", ?USER, ?b2l(Hashed), _Persist=false),
    Addr = config:get("chttpd", "bind_address", "127.0.0.1"),
    Port = mochiweb_socket_server:get(chttpd, port),
    lists:concat(["http://", Addr, ":", Port, "/"]).

teardown(_Url) ->
    ok = config:delete("couchdb", "enable_database_recovery", false),
    ok = config:delete("admins", ?USER, _Persist=false).

create_db(Url) ->
    {ok, Status, _, _} = test_request:put(Url, [?CONTENT_JSON, ?AUTH], "{}"),
    ?assert(Status =:= 201 orelse Status =:= 202).

delete_db(Url) ->
    {ok, 200, _, _} = test_request:delete(Url, [?AUTH]).

deleted_dbs_test_() ->
    {
        "chttpd deleted dbs tests",
        {
            setup,
            fun chttpd_test_util:start_couch/0, fun chttpd_test_util:stop_couch/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun should_return_error_for_deleted_dbs/1,
                    fun should_list_deleted_dbs/1,
                    fun should_list_deleted_dbs_info/1,
                    fun should_undelete_db/1,
                    fun should_delete_deleted_db/1,
                    fun should_undelete_db_to_target_db/1,
                    fun should_not_undelete_db_to_existing_db/1
                ]
            }
        }
    }.


should_return_error_for_deleted_dbs(Url) ->
    ?_test(begin
        create_and_delete_db(Url),
        NewDoc = "{\"keys\": [\"db1\"]}",
        {ok, Code, _, ResultBody} = test_request:post(Url ++ "/_deleted_dbs/",
            [?CONTENT_JSON, ?AUTH], NewDoc),

        {Body} = jiffy:decode(ResultBody),
        [
            ?assertEqual(<<"bad_request">>,
                couch_util:get_value(<<"error">>, Body)),
            ?assertEqual(400, Code)
        ]
    end).


should_list_deleted_dbs(Url) ->
    ?_test(begin
        DbName1 = create_and_delete_db(Url),
        DbName2 = create_and_delete_db(Url),
        {ok, _, _, ResultBody} = test_request:get(Url ++ "/_deleted_dbs/",
            [?CONTENT_JSON, ?AUTH]),
        BodyJson = jiffy:decode(ResultBody),
        ?assertEqual(true, lists:member(DbName1, BodyJson)),
        ?assertEqual(true, lists:member(DbName2, BodyJson))
    end).


should_list_deleted_dbs_info(Url) ->
    ?_test(begin
        DbName = create_and_delete_db(Url),
        {ok, _, _, ResultBody} = test_request:get(Url ++ "/_deleted_dbs/" ++
            DbName, [?CONTENT_JSON, ?AUTH]),
        BodyJson = jiffy:decode(ResultBody),

        {Db1Data} = lists:nth(1, BodyJson),
        ?assertEqual(DbName,
            couch_util:get_value(<<"key">>, Db1Data))
    end).


should_undelete_db(Url) ->
    ?_test(begin
        DbName = create_and_delete_db(Url),
        {ok, _, _, ResultBody} = test_request:get(Url ++ "/_deleted_dbs/" ++
            DbName, [?CONTENT_JSON, ?AUTH]),
        BodyJson = jiffy:decode(ResultBody),

        {Db1Data} = lists:nth(1, BodyJson),
        TimeStamp = couch_util:get_value(<<"timestamp">>, Db1Data),

        NewDoc = "{\"undelete\": {\"source\": \"" ++ ?b2l(DbName) ++
            "\", \"source_timestamp\":\"" ++ ?b2l(TimeStamp) ++ "\"}}",

        {ok, Status, _, _} = test_request:post(Url ++ "/_deleted_dbs",
            [?CONTENT_JSON, ?AUTH], NewDoc),
        ?assertEqual(200, Status),

        {ok, Status2, _, _} = test_request:get(Url ++ DbName,
            [?CONTENT_JSON, ?AUTH], NewDoc),
        ?assertEqual(200, Status2)
    end).


should_delete_deleted_db(Url) ->
    ?_test(begin
        DbName = create_and_delete_db(Url),
        {ok, _, _, ResultBody} = test_request:get(Url ++ "/_deleted_dbs/" ++
            DbName, [?CONTENT_JSON, ?AUTH]),
        BodyJson = jiffy:decode(ResultBody),

        {Db1Data} = lists:nth(1, BodyJson),
        TimeStamp = couch_util:get_value(<<"timestamp">>, Db1Data),

        NewDoc = "{\"delete\": {\"source\": \"" ++ ?b2l(DbName) ++
            "\", \"source_timestamp\":\"" ++ ?b2l(TimeStamp) ++ "\"}}",

        {ok, Status, _, _} = test_request:post(Url ++ "/_deleted_dbs",
            [?CONTENT_JSON, ?AUTH], NewDoc),
        ?assertEqual(200, Status)
    end).


should_undelete_db_to_target_db(Url) ->
    ?_test(begin
        DbName = create_and_delete_db(Url),
        {ok, _, _, ResultBody} = test_request:get(Url ++ "/_deleted_dbs/" ++
            DbName, [?CONTENT_JSON, ?AUTH]),
        BodyJson = jiffy:decode(ResultBody),
    
        {Db1Data} = lists:nth(1, BodyJson),
        TimeStamp = couch_util:get_value(<<"timestamp">>, Db1Data),

        NewDbName = ?tempdb(),
        NewDoc = "{\"undelete\": {\"source\": \"" ++ ?b2l(DbName) ++
            "\", \"source_timestamp\":\"" ++ ?b2l(TimeStamp) ++
            "\", \"target\": \"" ++ ?b2l(NewDbName) ++ "\" }}",
    
        {ok, RC2, _, _} = test_request:post(Url ++ "/_deleted_dbs",
            [?CONTENT_JSON, ?AUTH], NewDoc),
        ?assertEqual(200, RC2),
    
        {ok, RC3, _, _} = test_request:get(Url ++ NewDbName,
            [?CONTENT_JSON, ?AUTH]),
        ?assertEqual(200, RC3)
    end).


should_not_undelete_db_to_existing_db(Url) ->
    ?_test(begin
        DbName = create_and_delete_db(Url),
        {ok, _, _, ResultBody} = test_request:get(Url ++ "/_deleted_dbs/" ++
            DbName, [?CONTENT_JSON, ?AUTH]),
        BodyJson = jiffy:decode(ResultBody),

        {Db1Data} = lists:nth(1, BodyJson),
        TimeStamp = couch_util:get_value(<<"timestamp">>, Db1Data),

        NewDbName = ?tempdb(),
        create_db(Url ++ NewDbName),
        NewDoc = "{\"undelete\": {\"source\": \"" ++ ?b2l(DbName) ++
            "\", \"source_timestamp\":\"" ++ ?b2l(TimeStamp) ++
            "\", \"target\": \"" ++ ?b2l(NewDbName) ++ "\" }}",

        {ok, RC, _, Body} = test_request:post(Url ++ "/_deleted_dbs",
            [?CONTENT_JSON, ?AUTH], NewDoc),
        {JsonBody} = jiffy:decode(Body),
        [
            ?assertEqual(<<"file_exists">>,
                couch_util:get_value(<<"error">>, JsonBody)),
            ?assertEqual(412, RC)
        ]
    end).


create_and_delete_db(BaseUrl) ->
    DbName = ?tempdb(),
    DbUrl = BaseUrl ++ DbName,
    create_db(DbUrl),
    ok = config:set("couchdb", "enable_database_recovery", "true", false),
    delete_db(DbUrl),
    DbName.
