#pragma once

#include <memory>
#include "core/keyvalue/variant.h"
#include "core/reindexer.h"
#include "dbmanager.h"
#include "loggerwrapper.h"
#include "net/cproto/dispatcher.h"
#include "net/listener.h"
#include "statscollect/istatswatcher.h"
#include "tools/semversion.h"

namespace reindexer {
struct TxStats;
}

namespace reindexer_server {

using std::string;
using std::pair;
using namespace reindexer::net;
using namespace reindexer;

struct ClusterManagerClientData : public cproto::ClientData {
	~ClusterManagerClientData();

	AuthContext auth;
	int connID;
	SemVersion rxVersion;
};

class ClusterManagementServer {
public:
	ClusterManagementServer(DBManager &dbMgr, LoggerWrapper &logger, IClientsStats *clientsStats, bool allocDebug = false,
							IStatsWatcher *statsCollector = nullptr);
	~ClusterManagementServer();

	bool Start(const string &addr, ev::dynamic_loop &loop, bool enableStat);
	void Stop() { listener_->Stop(); }

	Error Ping(cproto::Context &ctx);
	Error Login(cproto::Context &ctx, p_string login, p_string password, p_string db, cproto::optional<bool> createDBIfMissing,
				cproto::optional<bool> checkClusterID, cproto::optional<int> expectedClusterID, cproto::optional<p_string> clientRxVersion,
				cproto::optional<p_string> appName);
	Error OpenDatabase(cproto::Context &ctx, p_string db, cproto::optional<bool> createDBIfMissing);

	Error SuggestLeader(cproto::Context &ctx, p_string suggestion);
	Error LeadersPing(cproto::Context &ctx, p_string leader);
	Error GetRaftInfo(cproto::Context &ctx);

	Error CheckAuth(cproto::Context &ctx);
	void Logger(cproto::Context &ctx, const Error &err, const cproto::Args &ret);
	void OnClose(cproto::Context &ctx, const Error &err);
	void OnResponse(cproto::Context &ctx);

protected:
	Reindexer getDB(cproto::Context &ctx, UserRole role);
	constexpr static string_view statsSourceName() { return "cluster_rpc"_sv; }

	DBManager &dbMgr_;
	cproto::Dispatcher dispatcher_;
	std::unique_ptr<Listener> listener_;

	LoggerWrapper logger_;
	bool allocDebug_;
	IStatsWatcher *statsWatcher_;

	IClientsStats *clientsStats_;

	std::chrono::system_clock::time_point startTs_;
};

}  // namespace reindexer_server
