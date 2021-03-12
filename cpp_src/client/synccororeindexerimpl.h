#pragma once

#include <condition_variable>
#include <future>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>
#include "client/coroqueryresults.h"
#include "client/cororeindexer.h"
#include "client/item.h"
#include "client/reindexerconfig.h"
#include "client/synccoroqueryresults.h"
#include "client/syncorotransaction.h"
#include "core/indexdef.h"
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "coroutine/channel.h"
#include "coroutine/waitgroup.h"
#include "net/ev/ev.h"
#include "replicator/updatesobserver.h"
#include "tools/errors.h"

namespace reindexer {
namespace client {
class SyncCoroTransaction;
class SyncCoroReindexerImpl {
public:
	/// Create Reindexer database object
	SyncCoroReindexerImpl(const CoroReindexerConfig & = CoroReindexerConfig());
	/// Destrory Reindexer database object
	~SyncCoroReindexerImpl();
	SyncCoroReindexerImpl(const SyncCoroReindexerImpl &) = delete;
	SyncCoroReindexerImpl &operator=(const SyncCoroReindexerImpl &) = delete;

	Error Connect(const std::string &dsn, const client::ConnectOpts &opts = client::ConnectOpts());
	Error Stop();
	Error OpenNamespace(string_view nsName, const InternalRdxContext &ctx,
						const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing());
	Error AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx);
	Error CloseNamespace(string_view nsName, const InternalRdxContext &ctx);
	Error DropNamespace(string_view nsName, const InternalRdxContext &ctx);
	Error TruncateNamespace(string_view nsName, const InternalRdxContext &ctx);
	Error RenameNamespace(string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx);
	Error AddIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error UpdateIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error DropIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error SetSchema(string_view nsName, string_view schema, const InternalRdxContext &ctx);
	Error EnumNamespaces(std::vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx);
	Error EnumDatabases(std::vector<std::string> &dbList, const InternalRdxContext &ctx);
	Error Insert(string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Update(string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Upsert(string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Update(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Delete(string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Delete(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Select(string_view query, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Select(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Commit(string_view nsName);
	Item NewItem(string_view nsName);
	Error GetMeta(string_view nsName, const std::string &key, std::string &data, const InternalRdxContext &ctx);
	Error PutMeta(string_view nsName, const std::string &key, const string_view &data, const InternalRdxContext &ctx);
	Error EnumMeta(string_view nsName, std::vector<std::string> &keys, const InternalRdxContext &ctx);
	Error SubscribeUpdates(IUpdatesObserver *observer, const UpdatesFilters &filters, SubscriptionOpts opts = SubscriptionOpts());
	Error UnsubscribeUpdates(IUpdatesObserver *observer);
	Error GetSqlSuggestions(const string_view sqlQuery, int pos, std::vector<std::string> &suggestions);
	Error Status(const InternalRdxContext &ctx);
	SyncCoroTransaction NewTransaction(string_view nsName, const InternalRdxContext &ctx);
	Error CommitTransaction(SyncCoroTransaction &tr, const InternalRdxContext &ctx);
	Error RollBackTransaction(SyncCoroTransaction &tr, const InternalRdxContext &ctx);

private:
	friend class SyncCoroQueryResults;
	friend class SyncCoroTransaction;
	Error fetchResults(int flags, SyncCoroQueryResults &result);
	Error addTxItem(SyncCoroTransaction &tr, Item &&item, ItemModifyMode mode, lsn_t lsn);
	Error modifyTx(SyncCoroTransaction &tr, Query &&q, lsn_t lsn);
	Item execNewItemTx(CoroTransaction &tr);
	Item newItemTx(CoroTransaction &tr);
	Error execAddTxItem(CoroTransaction &tr, Item &item, ItemModifyMode mode, lsn_t);
	std::unique_ptr<std::thread> loopThread_;
	void threadLoopFun(std::promise<Error> &&isRunning, const string &dsn, const client::ConnectOpts &opts);

	bool exit_ = false;
	enum cmdName {
		cmdNameNone = 0,
		cmdNameOpenNamespace = 1,
		cmdNameAddNamespace,
		cmdNameCloseNamespace,
		cmdNameDropNamespace,
		cmdNameTruncateNamespace,
		cmdNameRenameNamespace,
		cmdNameAddIndex,
		cmdNameUpdateIndex,
		cmdNameDropIndex,
		cmdNameSetSchema,
		cmdNameEnumNamespaces,
		cmdNameEnumDatabases,
		cmdNameInsert,
		cmdNameUpdate,
		cmdNameUpsert,
		cmdNameUpdateQ,
		cmdNameDelete,
		cmdNameDeleteQ,
		cmdNameNewItem,
		cmdNameSelectS,
		cmdNameSelectQ,
		cmdNameCommit,
		cmdNameGetMeta,
		cmdNamePutMeta,
		cmdNameEnumMeta,
		cmdNameGetSqlSuggestions,
		cmdNameStatus,
		cmdNameNewTransaction,
		cmdNameCommitTransaction,
		cmdNameRollBackTransaction,
		cmdNameFetchResults,
		cmdNameNewItemTx,
		cmdNameAddTxItem,
		cmdNameModifyTx,

	};

	struct commandDataBase {
		commandDataBase(cmdName id) : id_(id) {}
		cmdName id_;
		virtual ~commandDataBase() {}
	};

	template <typename R, typename... P>
	struct commandData : public commandDataBase {
		std::shared_ptr<std::promise<R>> ret;
	};

	template <typename R>
	struct commandData<R> : public commandDataBase {
		commandData(cmdName id, std::shared_ptr<std::promise<R>> r) : commandDataBase(id), ret(r) {}
		std::shared_ptr<std::promise<R>> ret;
	};

	template <typename R, typename P1>
	struct commandData<R, P1> : public commandDataBase {
		commandData(cmdName id, std::shared_ptr<std::promise<R>> r, P1 p1) : commandDataBase(id), ret(r), p1_(p1) {}
		std::shared_ptr<std::promise<R>> ret;
		P1 p1_;
	};

	template <typename R, typename P1, typename P2>
	struct commandData<R, P1, P2> : public commandDataBase {
		commandData(cmdName id, std::shared_ptr<std::promise<R>> r, P1 p1, P2 p2) : commandDataBase(id), ret(r), p1_(p1), p2_(p2) {}
		std::shared_ptr<std::promise<R>> ret;
		P1 p1_;
		P2 p2_;
	};

	template <typename R, typename P1, typename P2, typename P3>
	struct commandData<R, P1, P2, P3> : public commandDataBase {
		commandData(cmdName id, std::shared_ptr<std::promise<R>> r, P1 p1, P2 p2, P3 p3)
			: commandDataBase(id), ret(r), p1_(p1), p2_(p2), p3_(p3) {}
		std::shared_ptr<std::promise<R>> ret;
		P1 p1_;
		P2 p2_;
		P3 p3_;
	};

	template <typename R, typename P1, typename P2, typename P3, typename P4>
	struct commandData<R, P1, P2, P3, P4> : public commandDataBase {
		commandData(cmdName id, std::shared_ptr<std::promise<R>> r, P1 p1, P2 p2, P3 p3, P4 p4)
			: commandDataBase(id), ret(r), p1_(p1), p2_(p2), p3_(p3), p4_(p4) {}
		std::shared_ptr<std::promise<R>> ret;
		P1 p1_;
		P2 p2_;
		P3 p3_;
		P4 p4_;
	};

	template <typename R>
	R sendCoomand(cmdName c) {
		std::shared_ptr<std::promise<R>> promise(new std::promise<R>{});
		std::future<R> future = promise->get_future();
		commandData<R> *cmd = new commandData<R>{c, promise};
		std::unique_ptr<commandDataBase> cmdPtr(cmd);
		commandQuery_.Push(commandAsync, std::move(cmdPtr));
		future.wait();
		return future.get();
	}

	template <typename R, typename P1>
	R sendCoomand(cmdName c, P1 p1) {
		std::shared_ptr<std::promise<R>> promise(new std::promise<R>{});
		std::future<R> future = promise->get_future();
		commandData<R, P1> *cmd = new commandData<R, P1>{c, promise, p1};
		std::unique_ptr<commandDataBase> cmdPtr(cmd);
		commandQuery_.Push(commandAsync, std::move(cmdPtr));
		future.wait();
		return future.get();
	}

	template <typename R, typename P1, typename P2>
	R sendCoomand(cmdName c, P1 p1, P2 p2) {
		std::shared_ptr<std::promise<R>> promise(new std::promise<R>{});
		std::future<R> future = promise->get_future();
		commandData<R, P1, P2> *cmd = new commandData<R, P1, P2>{c, promise, p1, p2};
		std::unique_ptr<commandDataBase> cmdPtr(cmd);
		commandQuery_.Push(commandAsync, std::move(cmdPtr));
		future.wait();
		return future.get();
	}

	template <typename R, typename P1, typename P2, typename P3>
	R sendCoomand(cmdName c, P1 p1, P2 p2, P3 p3) {
		std::shared_ptr<std::promise<R>> promise(new std::promise<R>{});
		std::future<R> future = promise->get_future();
		commandData<R, P1, P2, P3> *cmd = new commandData<R, P1, P2, P3>{c, promise, p1, p2, p3};
		std::unique_ptr<commandDataBase> cmdPtr(cmd);
		commandQuery_.Push(commandAsync, std::move(cmdPtr));
		future.wait();
		return future.get();
	}

	template <typename R, typename P1, typename P2, typename P3, typename P4>
	R sendCoomand(cmdName c, P1 p1, P2 p2, P3 p3, P4 p4) {
		std::shared_ptr<std::promise<R>> promise(new std::promise<R>{});
		std::future<R> future = promise->get_future();
		commandData<R, P1, P2, P3, P4> *cmd = new commandData<R, P1, P2, P3, P4>{c, promise, p1, p2, p3, p4};
		std::unique_ptr<commandDataBase> cmdPtr(cmd);
		commandQuery_.Push(commandAsync, std::move(cmdPtr));
		future.wait();
		return future.get();
	}

	template <typename R>
	void execCommand(commandDataBase *cmd, const std::function<R()> &fun) {
		commandData<R> *cd = dynamic_cast<commandData<R> *>(cmd);
		if (cd != nullptr) {
			R r = fun();
			cd->ret->set_value(std::move(r));
		} else {
			assert(false);
		}
	}

	template <typename R, typename P1>
	void execCommand(commandDataBase *cmd, const std::function<R(P1)> &fun) {
		commandData<R, P1> *cd = dynamic_cast<commandData<R, P1> *>(cmd);
		if (cd != nullptr) {
			R r = fun(cd->p1_);
			cd->ret->set_value(std::move(r));
		} else {
			assert(false);
		}
	}

	template <typename R, typename P1, typename P2>
	void execCommand(commandDataBase *cmd, const std::function<R(P1, P2)> &fun) {
		commandData<R, P1, P2> *cd = dynamic_cast<commandData<R, P1, P2> *>(cmd);
		if (cd != nullptr) {
			R r = fun(cd->p1_, cd->p2_);
			cd->ret->set_value(std::move(r));
		} else {
			assert(false);
		}
	}

	template <typename R, typename P1, typename P2, typename P3>
	void execCommand(commandDataBase *cmd, const std::function<R(P1, P2, P3)> &fun) {
		commandData<R, P1, P2, P3> *cd = dynamic_cast<commandData<R, P1, P2, P3> *>(cmd);
		if (cd != nullptr) {
			R r = fun(cd->p1_, cd->p2_, cd->p3_);
			cd->ret->set_value(std::move(r));
		} else {
			assert(false);
		}
	}

	template <typename R, typename P1, typename P2, typename P3, typename P4>
	void execCommand(commandDataBase *cmd, const std::function<R(P1, P2, P3, P4)> &fun) {
		commandData<R, P1, P2, P3, P4> *cd = dynamic_cast<commandData<R, P1, P2, P3, P4> *>(cmd);
		if (cd != nullptr) {
			R r = fun(cd->p1_, cd->p2_, cd->p3_, cd->p4_);
			cd->ret->set_value(std::move(r));
		} else {
			assert(false);
		}
	}

	class CommandQuery {
	public:
		CommandQuery() {}
		void Push(net::ev::async &ev, std::unique_ptr<commandDataBase> cmd);
		bool Pop(std::unique_ptr<commandDataBase> &c);

	private:
		std::queue<std::unique_ptr<commandDataBase>> queue_;
		std::mutex mtx_;
	};
	CommandQuery commandQuery_;
	net::ev::dynamic_loop loop;
	net::ev::async commandAsync;
	net::ev::async closeAsync_;
	const CoroReindexerConfig conf_;
	void coroInterpreter(reindexer::client::CoroRPCClient &rx, coroutine::channel<std::unique_ptr<commandDataBase>> &chCommand,
						 coroutine::wait_group &wg);
};
}  // namespace client
}  // namespace reindexer
