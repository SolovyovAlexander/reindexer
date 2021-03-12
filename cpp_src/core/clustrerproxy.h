#pragma once
#include "client/synccororeindexer.h"
#include "core/item.h"
#include "core/reindexerimpl.h"

namespace reindexer {

class ClusterProxy {
public:
	ClusterProxy(IClientsStats *clientsStats);
	Error Connect(const string &dsn, ConnectOpts opts = ConnectOpts());
	Error OpenNamespace(string_view nsName, const StorageOpts &opts, const InternalRdxContext &ctx);  //+
	Error AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx);					  //-
	Error CloseNamespace(string_view nsName, const InternalRdxContext &ctx);
	Error DropNamespace(string_view nsName, const InternalRdxContext &ctx);
	Error TruncateNamespace(string_view nsName, const InternalRdxContext &ctx);	 //
	Error RenameNamespace(string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx);
	Error AddIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);  //+
	Error UpdateIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error DropIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error SetSchema(string_view nsName, string_view schema, const InternalRdxContext &ctx);
	Error GetSchema(string_view nsName, int format, std::string &schema, const InternalRdxContext &ctx);
	Error EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx);
	Error Insert(string_view nsName, Item &item, const InternalRdxContext &ctx);			//+
	Error Update(string_view nsName, Item &item, const InternalRdxContext &ctx);			//+
	Error Update(const Query &query, QueryResults &result, const InternalRdxContext &ctx);	//+
	Error Upsert(string_view nsName, Item &item, const InternalRdxContext &ctx);			//+
	Error Delete(string_view nsName, Item &item, const InternalRdxContext &ctx);			//+
	Error Delete(const Query &query, QueryResults &result, const InternalRdxContext &ctx);	//+
	Error Select(string_view query, QueryResults &result, const InternalRdxContext &ctx);
	Error Select(const Query &query, QueryResults &result, const InternalRdxContext &ctx);	//+ query type only select
	Error Commit(string_view nsName);
	Item NewItem(string_view nsName, const InternalRdxContext &ctx);  //+

	Transaction NewTransaction(string_view nsName, const InternalRdxContext &ctx);
	Error CommitTransaction(Transaction &tr, QueryResults &result, const InternalRdxContext &ctx);
	Error RollBackTransaction(Transaction &tr);

	Error GetMeta(string_view nsName, const string &key, string &data, const InternalRdxContext &ctx);
	Error PutMeta(string_view nsName, const string &key, string_view data, const InternalRdxContext &ctx);
	Error EnumMeta(string_view nsName, vector<string> &keys, const InternalRdxContext &ctx);
	Error GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string> &suggestions, const InternalRdxContext &ctx);
	Error Status();
	Error GetProtobufSchema(WrSerializer &ser, vector<string> &namespaces);
	Error GetReplState(string_view nsName, ReplicationStateV2 &state, const InternalRdxContext &ctx);

	bool NeedTraceActivity();
	Error EnableStorage(const string &storagePath, bool skipPlaceholderCheck, const InternalRdxContext &ctx);
	Error InitSystemNamespaces();
	Error SubscribeUpdates(IUpdatesObserver *observer, const UpdatesFilters &filters, SubscriptionOpts opts);
	Error ApplySnapshotChunk(string_view nsName, const SnapshotChunk &ch, const InternalRdxContext &ctx);
	Error SuggestLeader(const cluster::NodeData &suggestion, cluster::NodeData &response);
	Error LeadersPing(const cluster::NodeData &leader);
	Error GetRaftInfo(cluster::RaftInfo &info, const InternalRdxContext &ctx);
	Error UnsubscribeUpdates(IUpdatesObserver *observer);

	Error CreateTemporaryNamespace(string_view baseName, std::string &resultName, const StorageOpts &opts, const InternalRdxContext &ctx);
	Error GetSnapshot(string_view nsName, lsn_t from, Snapshot &snapshot, const InternalRdxContext &ctx);

private:
	ReindexerImpl impl_;
	std::atomic<int32_t> leaderId_;
	std::shared_ptr<client::SyncCoroReindexer> leader_;
	std::shared_ptr<client::SyncCoroReindexer> getLeader(const InternalRdxContext &ctx);
	template <typename Fn, Fn fn, typename FnL, FnL fnl, typename... Args>
	Error funRWCall(const InternalRdxContext &ctx, Args... args);
	template <typename Fn, Fn fn, typename FnL, FnL fnl>
	Error funRWCall(string_view nsName, Item &item, const InternalRdxContext &ctx);
	template <typename Fn, Fn fn, typename FnL, FnL fnl>
	Error funRWCall(const Query &query, QueryResults &result, const InternalRdxContext &ctx);
};
}  // namespace reindexer
