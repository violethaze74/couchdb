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

-module(aegis).


%% Assume old crypto api
-define(hmac(Key, PlainText), crypto:hmac(sha256, Key, PlainText)).
-define(aes_gcm_encrypt(Key, IV, AAD, Data),
        crypto:block_encrypt(aes_gcm, Key, IV, {AAD, Data, 16})).
-define(aes_gcm_decrypt(Key, IV, AAD, CipherText, CipherTag),
        crypto:block_decrypt(aes_gcm, Key, IV, {AAD, CipherText, CipherTag})).

%% Replace macros if new crypto api is available
-ifdef(OTP_RELEASE).
-if(?OTP_RELEASE >= 22).
-undef(hmac).
-undef(aes_gcm_encrypt).
-undef(aes_gcm_decrypt).

-define(hmac(Key, PlainText), crypto:mac(hmac, sha256, Key, PlainText)).
-define(aes_gcm_encrypt(Key, IV, AAD, Data),
        crypto:crypto_one_time_aead(aes_256_gcm, Key, IV, Data, AAD, 16, true)).
-define(aes_gcm_decrypt(Key, IV, AAD, CipherText, CipherTag),
        crypto:crypto_one_time_aead(aes_256_gcm, Key, IV, CipherText,
        AAD, CipherTag, false)).
-endif.
-endif.

-export([
    init/1,
    encrypt/3,
    decrypt/3,
    decrypt/2,
    wrap_fold/2
]).


-define(DUMMY_KEY, <<1:256>>).
-define(AEGIS_KEY_DOOHICKIES, 254).


create(#{} = Db) ->
    #{
        tx = Tx,
        db_prefix = DbPrefix
    } = Db,
    DbKeyThinger = erlfdb_tuple:pack({?AEGIS_KEY_DOOHICKIES}, DbPrefix),
    MyThingAMaJig = oooh_look_at_my_fancy_math(UUID, MathStuff),
    ok = erlfdb:set(Tx, DbKeyThinger, MyThingAMaJig),
    Db#{
        aegis_ctx = MyThingAMaJig
    }.


open(#{} = Db) ->
    #{
        tx = Tx,
        db_prefix = DbPrefix
    } = Db,
    DbKeyThinger = erlfdb_tuple:pack({?AEGIS_KEY_DOOHICKIES}, DbPrefix),
    MyLookupThing = erlfdb:wait(erlfdb:get(Tx, DbKeyThinger)),
    Db#{
        aegis_ctx => MyLookupThing
    }.


encrypt(#{} = Db, Key, Value) ->
    #{
        aegis_ctx = Ctx,
        uuid := UUID
    } = Db,
    DerivedKey = derive(?DUMMY_KEY, Key),
    {CipherText, <<CipherTag:128>>} = ?aes_gcm_encrypt(
            Derived,
            <<0:96>>,
            UUID,
            Value
        ),
    <<1:8, CipherTag:128, CipherText/binary>>.


decrypt(#{} = Db, Key, Value) when is_binary(Key), is_binary(Value) ->
    #{
        aegis_ctx = Ctx,
        uuid := UUID
    } = Db,
    <<1:8, CipherTag:128, CipherText/binary>> = Value,
    Derived = derive(?DUMMY_KEY, Key),
    Decrypted = ?aes_gcm_decrypt(
            Derived,
            <<0:96>>,
            UUID,
            CipherText,
            <<CipherTag:128>>
        ),
    if Decrypted /= error -> Decrypted; true ->
        erlang:error(decryption_failed)
    end.


decrypt(Db, {Key, Value}) ->
    {Key, decrypt(Db, Key, Value)};

decrypt(Db, Rows) when is_list(Rows) ->
    lists:map(fun({Key, Value}) ->
        decrypt(Db, {Key, Value})
    end, Rows).


wrap_fold_fun(Db, Fun) when is_function(Fun, 2)->
    fun({Key, Val}, Acc) -> Fun(decrypt(Db, {Key, Value}), Acc) end.


derive(KEK, KeyMaterial) when bit_size(KEK) == 256 ->
    PlainText = <<1:16, "aegis", 0:8, KeyMaterial/binary, 256:16>>,
    <<_:256>> = ?hmac(KEK, PlainText).
