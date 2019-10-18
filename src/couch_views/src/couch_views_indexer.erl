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

-module(couch_views_indexer).

-export([
    spawn_link/0
]).


-export([
    init/0
]).

-include("couch_views.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("fabric/include/fabric2.hrl").

% TODO:
%  * Handle timeouts of transaction and other errors


spawn_link() ->
    proc_lib:spawn_link(?MODULE, init, []).


init() ->
    {ok, Job, Data} = couch_jobs:accept(?INDEX_JOB_TYPE, #{}),
    #{
        <<"db_name">> := DbName,
        <<"ddoc_id">> := DDocId,
        <<"sig">> := JobSig
    } = Data,

    {ok, Db} = try
        fabric2_db:open(DbName, [?ADMIN_CTX])
    catch error:database_does_not_exist ->
        couch_jobs:finish(undefined, Job, Data#{
            error => database_does_not_exist,
            reason => <<"Database was deleted">>
        }),
        exit(normal)
    end,

    {ok, DDoc} = fabric2_db:open_doc(Db, DDocId),
    {ok, Mrst} = couch_views_util:ddoc_to_mrst(DbName, DDoc),
    HexSig = fabric2_util:to_hex(Mrst#mrst.sig),

    if  HexSig == JobSig -> ok; true ->
        couch_jobs:finish(undefined, Job, Data#{
            error => sig_changed,
            reason => <<"Design document was modified">>
        }),
        exit(normal)
    end,

    State = #{
        tx_db => undefined,
        view_seq => undefined,
        last_seq => undefined,
        job => Job,
        job_data => Data,
        count => 0,
        limit => num_changes(),
        batch_size => batch_size(),
        workers => [],
        design_opts => Mrst#mrst.design_opts
    },

    update(Db, Mrst, State).


update(#{} = Db, MrSt, State0) ->
    State2 = fabric2_fdb:transactional(Db, fun(TxDb1) ->
        % In the first iteration of update we need
        % to populate our db and view sequences
        State1 = case State0 of
            #{view_seq := undefined} ->
                ViewSeq = couch_views_fdb:get_update_seq(TxDb1, MrSt),
                State0#{
                    tx_db := TxDb1,
                    view_seq := ViewSeq,
                    last_seq := ViewSeq
                };
            _ ->
                State0#{
                    tx_db := TxDb1
                }
        end,

        fold_changes(State1, MrSt)
    end),

    #{
        last_seq := LastSeq,
        count := Count,
        limit := Limit,
        workers := Workers
    } = State2,

    % Bit odd to be starting with the newest
    % worker first here but I think it's fine
    % for now since we're collecting all updates
    % into a single write transaction
    Changes = lists:foldl(fun({WPid, WRef}, Acc) ->
        receive
            {'DOWN', WRef, process, WPid, {ok, NewChanges}} ->
                NewChanges ++ Acc;
            {'DOWN', WRef, process, WPid, Reason} ->
                exit({worker_update_failed, Reason})
        after 6000 ->
            erlang:error("Timeout waiting for worker: ~p", [WPid])
        end
    end, [], Workers),

    State4 = fabric2_fdb:transactional(Db, fun(TxDb2) ->

        write_changes(TxDb2, MrSt, LastSeq, Changes),

        case Count < Limit of
            true ->
                report_progress(State2#{tx_db := TxDb2}, finished),
                finished;
            false ->
                State3 = report_progress(State2#{tx_db := TxDb2}, update),
                State3#{
                    tx_db := undefined,
                    view_seq := LastSeq,
                    count := 0,
                    workers := []
                }
        end
    end),

    if State4 == finished -> ok; true ->
        update(Db, MrSt, State4)
    end.


fold_changes(State0, MrSt) ->
    #{
        tx_db := TxDb,
        view_seq := Seq,
        limit := Limit
    } = State0,

    Fun = fun process_changes/2,
    Acc = {State0, MrSt, []},
    Opts = [{limit, Limit}],
    {ok, AccOut} = fabric2_db:fold_changes(TxDb, Seq, Fun, Acc, Opts),
    spawn_worker(AccOut).


process_changes(Change, {State0, MrSt, Changes0}) ->
    #{
        count := Count,
        batch_size := BatchSize,
        design_opts := DesignOpts
    } = State0,

    #{
        id := Id,
        sequence := LastSeq
    } = Change,

    IncludeDesign = lists:keymember(<<"include_design">>, 1, DesignOpts),

    Changes1 = case {Id, IncludeDesign} of
        {<<?DESIGN_DOC_PREFIX, _/binary>>, false} ->
            Changes0;
        _ ->
            [Change | Changes0]
    end,

    State1 = State0#{
        count := Count + 1,
        last_seq := LastSeq
    },

    case length(Changes1) < BatchSize of
        true ->
            {ok, {State1, MrSt, Changes1}};
        false ->
            State2 = spawn_worker({State1, MrSt, Changes1}),
            {ok, {State2, MrSt, []}}
    end.


spawn_worker({State, _MrSt, []}) ->
    State;

spawn_worker({State, MrSt, Changes}) when length(Changes) > 0 ->
    #{
        tx_db := #{tx := Tx} = TxDb,
        workers := Workers
    } = State,
    ReadVersion = erlfdb:wait(erlfdb:get_read_version(Tx)),
    WState = State#{
        tx_db := TxDb#{tx := {read_version, ReadVersion}},
        workers := []
    },
    Worker = spawn_monitor(fun() ->
        process_changes(WState, MrSt, Changes)
    end),
    State#{
        workers := [Worker | Workers]
    }.


process_changes(State, MrSt, Changes0) ->
    #{
        tx_db := #{tx := {read_version, ReadVersion}} = TxDb0
    } = State,
    {ok, Db} = application:get_env(fabric, db),
    exit(erlfdb:transactional(Db, fun(NewTx) ->
        erlfdb:set_read_version(NewTx, ReadVersion),
        TxDb1 = TxDb0#{tx := NewTx},
        Changes1 = fetch_docs(TxDb1, Changes0),
        Changes2 = map_docs(MrSt, Changes1),
        {ok, Changes2}
    end)).


fetch_docs(Db, Changes) ->
    {Deleted, NotDeleted} = lists:partition(fun(Change) ->
        #{deleted := Deleted} = Change,
        Deleted
    end, Changes),

    RevState = lists:foldl(fun(Change, Acc) ->
        #{id := Id} = Change,
        RevFuture = fabric2_fdb:get_winning_revs_future(Db, Id, 1),
        Acc#{
            RevFuture => {Id, Change}
        }
    end, #{}, NotDeleted),

    RevFutures = maps:keys(RevState),
    BodyState = lists:foldl(fun(RevFuture, Acc) ->
        {Id, Change} = maps:get(RevFuture, RevState),
        Revs = fabric2_fdb:get_winning_revs_wait(Db, RevFuture),

        % I'm assuming that in this changes transaction that the winning
        % doc body exists since it is listed in the changes feed as not deleted
        #{winner := true} = RevInfo = lists:last(Revs),
        BodyFuture = fabric2_fdb:get_doc_body_future(Db, Id, RevInfo),
        Acc#{
            BodyFuture => {Id, RevInfo, Change}
        }
    end, #{}, erlfdb:wait_for_all(RevFutures)),

    BodyFutures = maps:keys(BodyState),
    ChangesWithDocs = lists:map(fun (BodyFuture) ->
        {Id, RevInfo, Change} = maps:get(BodyFuture, BodyState),
        Doc = fabric2_fdb:get_doc_body_wait(Db, Id, RevInfo, BodyFuture),
        Change#{doc => Doc}
    end, erlfdb:wait_for_all(BodyFutures)),

    % This combines the deleted changes with the changes that contain docs
    % Important to note that this is now unsorted. Which is fine for now
    % But later could be an issue if we split this across transactions
    Deleted ++ ChangesWithDocs.


map_docs(MrSt, Changes) ->
    % Run all the non deleted docs through the view engine and
    {ok, QServer} = get_query_server(MrSt),

    {Deleted0, NotDeleted0} = lists:partition(fun(Change) ->
        #{deleted := Deleted} = Change,
        Deleted
    end, Changes),

    Deleted1 = lists:map(fun(Change) ->
        Change#{
            results => []
        }
    end, Deleted0),

    DocsToMap = lists:map(fun(Change) ->
        #{doc := DocRec} = Change,
        DocRec
    end, NotDeleted0),

    {ok, AllResults} = couch_eval:map_docs(QServer, DocsToMap),

    % The expanded function head here is making an assertion
    % that the results match the given doc
    NotDeleted1 = lists:zipwith(fun(#{id := Id} = Change, {Id, Results}) ->
        Change#{
            doc := [],
            results => Results
        }
    end, NotDeleted0, AllResults),

    % I'm being a bit careful here resorting the docs
    % in order of the changes feed. Theoretically this is
    % unnecessary since we're inside a single transaction.
    % However, I'm concerned if we ever split this up
    % into multiple transactions that this detail might
    % be important but forgotten.
    lists:sort(fun(A, B) ->
        #{sequence := ASeq} = A,
        #{sequence := BSeq} = B,
        ASeq =< BSeq
    end, Deleted1 ++ NotDeleted1).


write_changes(TxDb, MrSt, LastSeq, Changes) ->
    #mrst{
        views = Views,
        sig = Sig
    } = MrSt,

    ViewIds = [View#mrview.id_num || View <- Views],

    lists:foreach(fun(Change) ->
        couch_views_fdb:write_doc(TxDb, Sig, ViewIds, Change)
    end, Changes),

    couch_views_fdb:set_update_seq(TxDb, Sig, LastSeq).


get_query_server(#mrst{qserver = nil} = Mrst) ->
    #mrst{
        db_name = DbName,
        idx_name = DDocId,
        language = Language,
        sig = Sig,
        lib = Lib,
        views = Views
    } = Mrst,
    couch_eval:acquire_map_context(
            DbName,
            DDocId,
            Language,
            Sig,
            Lib,
            [View#mrview.def || View <- Views]
        ).


report_progress(State, UpdateType) ->
    #{
        tx_db := TxDb,
        job := Job1,
        job_data := JobData,
        last_seq := LastSeq
    } = State,

    #{
        <<"db_name">> := DbName,
        <<"ddoc_id">> := DDocId,
        <<"sig">> := Sig
    } = JobData,

    % Reconstruct from scratch to remove any
    % possible existing error state.
    NewData = #{
        <<"db_name">> => DbName,
        <<"ddoc_id">> => DDocId,
        <<"sig">> => Sig,
        <<"view_seq">> => LastSeq
    },

    case UpdateType of
        update ->
            case couch_jobs:update(TxDb, Job1, NewData) of
                {ok, Job2} ->
                    State#{job := Job2};
                {error, halt} ->
                    couch_log:error("~s job halted :: ~w", [?MODULE, Job1]),
                    exit(normal)
            end;
        finished ->
            case couch_jobs:finish(TxDb, Job1, NewData) of
                ok ->
                    State;
                {error, halt} ->
                    couch_log:error("~s job halted :: ~w", [?MODULE, Job1]),
                    exit(normal)
            end
    end.


num_changes() ->
    config:get_integer("couch_views", "change_limit", 1000).


batch_size() ->
    config:get_integer("couch_views", "batch_size", 100).

