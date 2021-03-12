#pragma once

#include <functional>
#include <string>
#include "client/coroqueryresults.h"
#include "client/cororeindexerconfig.h"
#include "client/corotransaction.h"
#include "client/internalrdxcontext.h"
#include "client/item.h"
#include "client/namespace.h"
#include "cluster/config.h"
#include "core/keyvalue/p_string.h"
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "coroutine/waitgroup.h"
#include "estl/fast_hash_map.h"
#include "net/cproto/coroclientconnection.h"
#include "replicator/updatesobserver.h"
#include "tools/errors.h"
#include "urlparser/urlparser.h"

namespace reindexer {

struct ReplicationStateV2;
class SnapshotChunk;

namespace client {

class Snapshot;

using namespace net;
class CoroRPCClient {
public:
	using NodeData = cluster::NodeData;
	using RaftInfo = cluster::RaftInfo;
	typedef std::function<void(const Error &err)> Completion;
	CoroRPCClient(const CoroReindexerConfig &config);
	CoroRPCClient(const CoroRPCClient &) = delete;
	CoroRPCClient(CoroRPCClient &&) = delete;
	CoroRPCClient &operator=(const CoroRPCClient &) = delete;
	CoroRPCClient &operator=(CoroRPCClient &&) = delete;
	~CoroRPCClient();

	Error Connect(const string &dsn, ev::dynamic_loop &loop, const ConnectOpts &opts);
	Error Stop();

	Error OpenNamespace(string_view nsName, const InternalRdxContext &ctx,
						const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing());
	Error AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx);
	Error CloseNamespace(string_view nsName, const InternalRdxContext &ctx);
	Error DropNamespace(string_view nsName, const InternalRdxContext &ctx);
	Error CreateTemporaryNamespace(string_view baseName, std::string &resultName, const InternalRdxContext &ctx,
								   const StorageOpts &opts = StorageOpts().Enabled());
	Error TruncateNamespace(string_view nsName, const InternalRdxContext &ctx);
	Error RenameNamespace(string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx);
	Error AddIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error UpdateIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error DropIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error SetSchema(string_view nsName, string_view schema, const InternalRdxContext &ctx);
	Error EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx);
	Error EnumDatabases(vector<string> &dbList, const InternalRdxContext &ctx);
	Error Insert(string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Update(string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Upsert(string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Delete(string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Delete(const Query &query, CoroQueryResults &result, const InternalRdxContext &ctx);
	Error Update(const Query &query, CoroQueryResults &result, const InternalRdxContext &ctx);
	Error Select(string_view query, CoroQueryResults &result, const InternalRdxContext &ctx) {
		return selectImpl(query, result, config_.RequestTimeout, ctx);
	}
	Error Select(const Query &query, CoroQueryResults &result, const InternalRdxContext &ctx) {
		return selectImpl(query, result, config_.RequestTimeout, ctx);
	}
	Error Commit(string_view nsName);
	Item NewItem(string_view nsName);
	Error GetMeta(string_view nsName, const string &key, string &data, const InternalRdxContext &ctx);
	Error PutMeta(string_view nsName, const string &key, const string_view &data, const InternalRdxContext &ctx);
	Error EnumMeta(string_view nsName, vector<string> &keys, const InternalRdxContext &ctx);
	Error SubscribeUpdates(IUpdatesObserver *observer, const UpdatesFilters &filters, SubscriptionOpts opts = SubscriptionOpts());
	Error UnsubscribeUpdates(IUpdatesObserver *observer);
	Error GetSqlSuggestions(string_view query, int pos, std::vector<std::string> &suggests);
	Error Status(const InternalRdxContext &ctx);

	CoroTransaction NewTransaction(string_view nsName, const InternalRdxContext &ctx);
	Error CommitTransaction(CoroTransaction &tr, const InternalRdxContext &ctx);
	Error RollBackTransaction(CoroTransaction &tr, const InternalRdxContext &ctx);
	Error GetReplState(string_view nsName, ReplicationStateV2 &state, const InternalRdxContext &ctx);
	Error GetSnapshot(string_view nsName, lsn_t from, Snapshot &snapshot, const InternalRdxContext &ctx);
	Error ApplySnapshotChunk(string_view nsName, const SnapshotChunk &ch, const InternalRdxContext &ctx);

	Error SuggestLeader(const NodeData &suggestion, NodeData &response, const InternalRdxContext &ctx);
	Error LeadersPing(const NodeData &leader, const InternalRdxContext &ctx);
	Error GetRaftInfo(RaftInfo &info, const InternalRdxContext &ctx);

protected:
	Error selectImpl(string_view query, CoroQueryResults &result, milliseconds netTimeout, const InternalRdxContext &ctx);
	Error selectImpl(const Query &query, CoroQueryResults &result, milliseconds netTimeout, const InternalRdxContext &ctx);
	Error modifyItem(string_view nsName, Item &item, int mode, milliseconds netTimeout, const InternalRdxContext &ctx);
	Error subscribeImpl(bool subscribe);
	Namespace *getNamespace(string_view nsName);
	void onUpdates(const net::cproto::CoroRPCAnswer &ans);
	void startResubRoutine();

	void resubRoutine();
	void onConnFatalError(Error) noexcept { subscribed_ = false; }

	cproto::CommandParams mkCommand(cproto::CmdCode cmd, const InternalRdxContext *ctx = nullptr) const noexcept;
	static cproto::CommandParams mkCommand(cproto::CmdCode cmd, milliseconds reqTimeout, const InternalRdxContext *ctx) noexcept;

	fast_hash_map<string, Namespace::Ptr, nocase_hash_str, nocase_equal_str> namespaces_;

	CoroReindexerConfig config_;
	UpdatesObservers observers_;
	cproto::CoroClientConnection conn_;
	bool subscribed_ = false;
	bool terminate_ = false;
	coroutine::wait_group resubWg_;
	ev::dynamic_loop *loop_ = nullptr;
};

void vec2pack(const h_vector<int32_t, 4> &vec, WrSerializer &ser);

}  // namespace client
}  // namespace reindexer
