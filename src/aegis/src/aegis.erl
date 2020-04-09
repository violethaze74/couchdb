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

-define(IS_AEGIS_FUTURE, {aegis_future, _}).
%% encapsulation violation :/
-define(IS_FUTURE, {erlfdb_future, _, _}).

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
-define(hmac(Key, PlainText), crypto:mac(hmac, sha256, Key, PlainText)).
-undef(aes_gcm_encrypt).
-define(aes_gcm_encrypt(Key, IV, AAD, Data),
        crypto:crypto_one_time_aead(aes_256_gcm, Key, IV, Data, AAD, 16, true)).
-undef(aes_gcm_decrypt).
-define(aes_gcm_decrypt(Key, IV, AAD, CipherText, CipherTag),
        crypto:crypto_one_time_aead(aes_256_gcm, Key, IV, CipherText,
        AAD, CipherTag, false)).
-endif.
-endif.

-export([
    fold_range/6,
    fold_range/7,
    fold_range_future/5,
    fold_range_wait/4,
    get/3,
    get_range/4,
    get_range/5,
    get_range_startswith/3,
    get_range_startswith/4,
    set/4,
    wait/1
]).


fold_range(EncryptionContext, DbOrTx, StartKey, EndKey, Fun, Acc) ->
    fold_range(EncryptionContext, DbOrTx, StartKey, EndKey, Fun, Acc, []).


fold_range(EncryptionContext, DbOrTx, StartKey, EndKey, Fun, Acc, Options) ->
    validate_encryption_context(EncryptionContext),
    erlfdb:fold_range(DbOrTx, StartKey, EndKey, decrypt_fun(EncryptionContext, Fun), Acc, Options).


fold_range_future(EncryptionContext, TxOrSs, StartKey, EndKey, Options) ->
    validate_encryption_context(EncryptionContext),
    Future = erlfdb:fold_range_future(TxOrSs, StartKey, EndKey, Options),
    {aegis_fold_future, EncryptionContext, Future}.


fold_range_wait(Tx, {aegis_fold_future, EncryptionContext, Future}, Fun, Acc) ->
    validate_encryption_context(EncryptionContext),
    erlfdb:fold_range_wait(Tx, Future, decrypt_fun(EncryptionContext, Fun), Acc).


get(EncryptionContext, DbOrTx, Key) ->
    validate_encryption_context(EncryptionContext),
    Result = erlfdb:get(DbOrTx, Key),
    decrypt(EncryptionContext, Key, Result).


get_range(EncryptionContext, DbOrTx, StartKey, EndKey) ->
    get_range(EncryptionContext, DbOrTx, StartKey, EndKey, []).


get_range(EncryptionContext, DbOrTx, StartKey, EndKey, Options) ->
    validate_encryption_context(EncryptionContext),
    Result = erlfdb:get_range(DbOrTx, StartKey, EndKey, Options),
    decrypt(EncryptionContext, Result).


get_range_startswith(EncryptionContext, DbOrTx, Prefix) ->
    get_range_startswith(EncryptionContext, DbOrTx, Prefix, []).


get_range_startswith(EncryptionContext, DbOrTx, Prefix, Options) ->
    validate_encryption_context(EncryptionContext),
    Result = erlfdb:get_range_startswith(DbOrTx, Prefix, Options),
    decrypt(EncryptionContext, Result).


set(EncryptionContext, DbOrTx, Key, Value) ->
    validate_encryption_context(EncryptionContext),
    erlfdb:set(DbOrTx, Key, encrypt(EncryptionContext, Key, Value)).


wait({aegis_future, EncryptionContext, Future}) ->
    Value = erlfdb:wait(Future),
    decrypt(EncryptionContext, Value);

wait({aegis_future, EncryptionContext, Key, Future}) ->
    Value = erlfdb:wait(Future),
    decrypt(EncryptionContext, Key, Value);

wait(Result) ->
    Result.


%% Private functions

validate_encryption_context(#{uuid := _UUID}) ->
    ok;
validate_encryption_context(_) ->
    error(invalid_encryption_context).


-define(DUMMY_KEY, <<1:256>>).

encrypt(#{uuid := UUID}, Key, Value) ->
    {CipherText, <<CipherTag:128>>} =
        ?aes_gcm_encrypt(
           derive(?DUMMY_KEY, Key),
           <<0:96>>,
           UUID,
           Value),
    <<1:8, CipherTag:128, CipherText/binary>>.


decrypt(EncryptionContext, ?IS_FUTURE = Future) ->
    decrypt_future(EncryptionContext, Future);

decrypt(EncryptionContext, {Key, Value})
  when is_binary(Key), is_binary(Value) ->
    decrypt(EncryptionContext, Key, Value);

decrypt(EncryptionContext, Rows) when is_list(Rows) ->
    [{Key, decrypt(EncryptionContext, Row)} || {Key, _} = Row <- Rows].


decrypt(EncryptionContext, Key, ?IS_FUTURE = Future) ->
    decrypt_future(EncryptionContext, Key, Future);

decrypt(#{uuid := UUID}, Key, Value) when is_binary(Value) ->
    <<1:8, CipherTag:128, CipherText/binary>> = Value,
    Decrypted =
        ?aes_gcm_decrypt(
           derive(?DUMMY_KEY, Key),
           <<0:96>>,
           UUID,
           CipherText,
           <<CipherTag:128>>),
    case Decrypted of
        error ->
            erlang:error(decryption_failed);
        Decrypted ->
            Decrypted
    end;

decrypt(_EncryptionContext, _Key, Value) when not is_binary(Value) ->
    Value.


decrypt_future(EncryptionContext, ?IS_FUTURE = Future) ->
    {aegis_future, EncryptionContext, Future}.

decrypt_future(EncryptionContext, Key, ?IS_FUTURE = Future) ->
    {aegis_future, EncryptionContext, Key, Future}.

decrypt_fun(EncryptionContext, Fun) ->
    fun(Rows, Acc) ->
            Fun(decrypt(EncryptionContext, Rows), Acc)
    end.

derive(KEK, KeyMaterial) when bit_size(KEK) == 256 ->
    PlainText = <<1:16, "aegis", 0:8, KeyMaterial/binary, 256:16>>,
    <<_:256>> = ?hmac(KEK, PlainText).
