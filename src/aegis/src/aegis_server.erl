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

-module(aegis_server).

-behaviour(gen_server).

-vsn(1).


-include("aegis.hrl").


%% aegis_server API
-export([
    start_link/0,
    init_db/2,
    open_db/2,
    encrypt/3,
    decrypt/3
]).

%% gen_server callbacks
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

%% workers callbacks
-export([
    do_encrypt/5,
    do_decrypt/5
]).


-define(INIT_TIMEOUT, 60000).
-define(TIMEOUT, 10000).


-record(entry, {uuid, encryption_key}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec init_db(Db :: #{}, Options :: list()) -> boolean().
init_db(#{} = Db, Options) ->
    #{
        uuid := UUID
    } = Db,
    case ?AEGIS_KEY_MANAGER:init_db(Db, Options) of
        {ok, Key} ->
            ok = gen_server:call(?MODULE, {insert_key, UUID, Key}),
            true;
        false ->
            false
    end.


-spec open_db(Db :: #{}, Options :: list()) -> boolean().
open_db(#{} = Db, Options) ->
    #{
        uuid := UUID
    } = Db,
    case gen_server:call(?MODULE, {has_key, UUID}) of
        true ->
            true;
        false ->
            case ?AEGIS_KEY_MANAGER:open_db(Db, Options) of
                {ok, Key} ->
                    ok = gen_server:call(?MODULE, {insert_key, UUID, Key}),
                    true;
                false ->
                    false
            end
    end.


-spec encrypt(Db :: #{}, Key :: binary(), Value :: binary()) -> binary().
encrypt(#{} = Db, Key, Value)  when is_binary(Key), is_binary(Value) ->
    gen_server:call(?MODULE, {encrypt, Db, Key, Value}).


-spec decrypt(Db :: #{}, Key :: binary(), Value :: binary()) -> binary().
decrypt(#{} = Db, Key, Value) when is_binary(Key), is_binary(Value) ->
    gen_server:call(?MODULE, {decrypt, Db, Key, Value}).


%% gen_server functions

init([]) ->
    process_flag(sensitive, true),
    Cache = ets:new(?MODULE, [set, private, {keypos, #entry.uuid}]),

    St = #{
        cache => Cache
    },
    {ok, St, ?INIT_TIMEOUT}.


terminate(_Reason, _St) ->
    ok.

handle_call({insert_key, UUID, EncryptionKey}, _From, St) ->
    #{
        cache := Cache
    } = St,
    true = insert(Cache, UUID, EncryptionKey),
    {reply, ok, St, ?TIMEOUT};

handle_call({has_key, UUID}, _From, St) ->
    #{
        cache := Cache
    } = St,
    case lookup(Cache, UUID) of
        {ok, _Key} ->
            {reply, true, St, ?TIMEOUT};
        {error, not_found} ->
            {reply, false, St, ?TIMEOUT}
    end.


handle_call({encrypt, Db, Key, Value}, From, St) ->
    maybe_spawn_worker(St, From, do_encrypt, Db, Key, Value);

handle_call({decrypt, Db, Key, Value}, From, St) ->
    maybe_spawn_worker(St, From, do_decrypt, Db, Key, Value);

handle_call(_Msg, _From, St) ->
    {noreply, St}.


handle_cast(_Msg, St) ->
    {noreply, St}.


handle_info(_Msg, St) ->
    {noreply, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


%% workers functions

do_encrypt(From, DbKey, #{uuid := UUID}, Key, Value) ->
    process_flag(sensitive, true),
    try
        EncryptionKey = crypto:strong_rand_bytes(32),
        <<WrappedKey:320>> = aegis_keywrap:key_wrap(DbKey, EncryptionKey),

        {CipherText, <<CipherTag:128>>} =
            ?aes_gcm_encrypt(
               EncryptionKey,
               <<0:96>>,
               <<UUID/binary, 0:8, Key/binary>>,
               Value),
        <<1:8, WrappedKey:320, CipherTag:128, CipherText/binary>>
    of
        Resp ->
            gen_server:reply(From, Resp)
    catch
        _:Error ->
            gen_server:reply(From, {error, Error})
    end.


do_decrypt(From, DbKey, #{uuid := UUID}, Key, Value) ->
    process_flag(sensitive, true),
    try
        case Value of
            <<1:8, WrappedKey:320, CipherTag:128, CipherText/binary>> ->
                case aegis_keywrap:key_unwrap(DbKey, <<WrappedKey:320>>) of
                    fail ->
                        erlang:error(decryption_failed);
                    DecryptionKey ->
                        Decrypted =
                        ?aes_gcm_decrypt(
                            DecryptionKey,
                            <<0:96>>,
                            <<UUID/binary, 0:8, Key/binary>>,
                            CipherText,
                            <<CipherTag:128>>),
                        if Decrypted /= error -> Decrypted; true ->
                            erlang:error(decryption_failed)
                        end
                end;
            _ ->
                erlang:error(not_ciphertext)
        end
    of
        Resp ->
            gen_server:reply(From, Resp)
    catch
        _:Error ->
            gen_server:reply(From, {error, Error})
    end.


%% private functions

maybe_spawn_worker(St, From, Action, #{uuid := UUID} = Db, Key, Value) ->
    #{
        cache := Cache
    } = St,

    case lookup(Cache, UUID) of
        {ok, DbKey} ->
            erlang:spawn(?MODULE, Action, [From, DbKey, Db, Key, Value]),
            {noreply, St, ?TIMEOUT};
        {error, not_found} ->
            {reply, {error, encryption_key_not_found}, St, ?TIMEOUT}
     end.


%% cache functions

insert(Cache, UUID, DbKey) ->
    Entry = #entry{uuid = UUID, encryption_key = DbKey},
    ets:insert(Cache, Entry).


lookup(Cache, UUID) ->
    case ets:lookup(Cache, UUID) of
        [#entry{uuid = UUID, encryption_key = DbKey}] ->
            {ok, DbKey};
        [] ->
            {error, not_found}
    end.
