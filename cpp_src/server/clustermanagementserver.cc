#include "clustermanagementserver.h"
#include <sys/stat.h>
#include <sstream>
#include "cluster/config.h"
#include "cluster/raftmanager.h"
#include "core/cjson/jsonbuilder.h"
#include "core/iclientsstats.h"
#include "net/cproto/cproto.h"
#include "net/cproto/serverconnection.h"
#include "net/listener.h"
#include "reindexer_version.h"

namespace reindexer_server {

ClusterManagementServer::ClusterManagementServer(DBManager &dbMgr, LoggerWrapper &logger, IClientsStats *clientsStats, bool allocDebug,
												 IStatsWatcher *statsCollector)
	: dbMgr_(dbMgr),
	  logger_(logger),
	  allocDebug_(allocDebug),
	  statsWatcher_(statsCollector),
	  clientsStats_(clientsStats),
	  startTs_(std::chrono::system_clock::now()) {}

ClusterManagementServer::~ClusterManagementServer() {}

Error ClusterManagementServer::Ping(cproto::Context &) {
	//
	return 0;
}

static std::atomic<int> connCounter = {0};

Error ClusterManagementServer::Login(cproto::Context &ctx, p_string login, p_string password, p_string db,
									 cproto::optional<bool> createDBIfMissing, cproto::optional<bool> checkClusterID,
									 cproto::optional<int> expectedClusterID, cproto::optional<p_string> clientRxVersion,
									 cproto::optional<p_string> appName) {
	if (ctx.GetClientData()) {
		return Error(errParams, "Already logged in");
	}

	std::unique_ptr<ClusterManagerClientData> clientData(new ClusterManagerClientData);

	clientData->connID = connCounter.fetch_add(1, std::memory_order_relaxed);
	clientData->auth = AuthContext(login.toString(), password.toString());

	auto dbName = db.toString();
	if (checkClusterID.hasValue() && checkClusterID.value()) {
		assert(expectedClusterID.hasValue());
		clientData->auth.SetExpectedClusterID(expectedClusterID.value());
	}
	auto status = dbMgr_.Login(dbName, clientData->auth);
	if (!status.ok()) {
		return status;
	}

	if (clientRxVersion.hasValue()) {
		clientData->rxVersion = SemVersion(string_view(clientRxVersion.value()));
	} else {
		clientData->rxVersion = SemVersion();
	}

	if (clientsStats_) {
		reindexer::ClientConnectionStat conn;
		conn.connectionStat = ctx.writer->GetConnectionStat();
		conn.ip = string(ctx.clientAddr);
		conn.userName = clientData->auth.Login();
		conn.dbName = clientData->auth.DBName();
		conn.userRights = string(UserRoleName(clientData->auth.UserRights()));
		conn.clientVersion = clientData->rxVersion.StrippedString();
		conn.appName = appName.hasValue() ? appName.value().toString() : string();
		clientsStats_->AddConnection(clientData->connID, std::move(conn));
	}

	ctx.SetClientData(std::move(clientData));
	if (statsWatcher_) {
		statsWatcher_->OnClientConnected(dbName, statsSourceName());
	}
	int64_t startTs = std::chrono::duration_cast<std::chrono::seconds>(startTs_.time_since_epoch()).count();
	static string_view version = REINDEX_VERSION;

	status = db.length() ? OpenDatabase(ctx, db, createDBIfMissing) : errOK;
	if (status.ok()) {
		ctx.Return({cproto::Arg(p_string(&version)), cproto::Arg(startTs)}, status);
	}

	return status;
}

static ClusterManagerClientData *getClientDataUnsafe(cproto::Context &ctx) {
	return dynamic_cast<ClusterManagerClientData *>(ctx.GetClientData());
}

static ClusterManagerClientData *getClientDataSafe(cproto::Context &ctx) {
	auto ret = dynamic_cast<ClusterManagerClientData *>(ctx.GetClientData());
	if (!ret) std::abort();	 // It should be set by middleware
	return ret;
}

Error ClusterManagementServer::OpenDatabase(cproto::Context &ctx, p_string db, cproto::optional<bool> createDBIfMissing) {
	auto *clientData = getClientDataSafe(ctx);
	if (clientData->auth.HaveDB()) {
		return Error(errParams, "Database already opened");
	}
	auto status = dbMgr_.OpenDatabase(db.toString(), clientData->auth, createDBIfMissing.hasValue() && createDBIfMissing.value());
	if (!status.ok()) {
		clientData->auth.ResetDB();
	}
	return status;
}

Error ClusterManagementServer::CheckAuth(cproto::Context &ctx) {
	cproto::ClientData *ptr = ctx.GetClientData();
	auto clientData = dynamic_cast<ClusterManagerClientData *>(ptr);

	if (ctx.call->cmd == cproto::kCmdLogin || ctx.call->cmd == cproto::kCmdPing) {
		return errOK;
	}

	if (!clientData) {
		return Error(errForbidden, "You should login");
	}

	return errOK;
}

void ClusterManagementServer::OnClose(cproto::Context &ctx, const Error &err) {
	(void)ctx;
	(void)err;

	if (statsWatcher_) {
		auto clientData = getClientDataUnsafe(ctx);
		if (clientData) {
			statsWatcher_->OnClientDisconnected(clientData->auth.DBName(), statsSourceName());
		}
	}
	if (clientsStats_) {
		auto clientData = dynamic_cast<ClusterManagerClientData *>(ctx.GetClientData());
		if (clientData) clientsStats_->DeleteConnection(clientData->connID);
	}
	logger_.info("RPC: Client disconnected");
}

void ClusterManagementServer::OnResponse(cproto::Context &ctx) {
	if (statsWatcher_) {
		auto clientData = getClientDataUnsafe(ctx);
		auto dbName = (clientData != nullptr) ? clientData->auth.DBName() : "<unknown>";
		statsWatcher_->OnOutputTraffic(dbName, statsSourceName(), ctx.stat.sizeStat.respSizeBytes);
		if (ctx.stat.sizeStat.respSizeBytes) {
			// Don't update stats on responses like "updates push"
			statsWatcher_->OnInputTraffic(dbName, statsSourceName(), ctx.stat.sizeStat.reqSizeBytes);
		}
	}
}

void ClusterManagementServer::Logger(cproto::Context &ctx, const Error &err, const cproto::Args &ret) {
	auto clientData = getClientDataUnsafe(ctx);
	WrSerializer ser;

	if (clientData) {
		ser << "c='"_sv << clientData->connID << "' db='"_sv << clientData->auth.Login() << "@"_sv << clientData->auth.DBName() << "' "_sv;
	} else {
		ser << "- - "_sv;
	}

	if (ctx.call) {
		ser << cproto::CmdName(ctx.call->cmd) << " "_sv;
		ctx.call->args.Dump(ser);
	} else {
		ser << '-';
	}

	ser << " -> "_sv << (err.ok() ? "OK"_sv : err.what());
	if (ret.size()) {
		ser << ' ';
		ret.Dump(ser);
	}

	HandlerStat statDiff = HandlerStat() - ctx.stat.allocStat;
	ser << ' ' << statDiff.GetTimeElapsed() << "us"_sv;

	if (allocDebug_) {
		ser << " |  allocs: "_sv << statDiff.GetAllocsCnt() << ", allocated: " << statDiff.GetAllocsBytes() << " byte(s)";
	}

	logger_.info("{}", ser.Slice());
}

Reindexer ClusterManagementServer::getDB(cproto::Context &ctx, UserRole role) {
	auto rawClientData = ctx.GetClientData();
	if (rawClientData) {
		auto clientData = dynamic_cast<ClusterManagerClientData *>(rawClientData);
		if (clientData) {
			Reindexer *db = nullptr;
			auto status = clientData->auth.GetDB(role, &db);
			if (!status.ok()) {
				throw status;
			}
			if (db != nullptr) {
				return db->NeedTraceActivity() ? db->WithTimeout(ctx.call->execTimeout)
													 .WithActivityTracer(ctx.clientAddr, clientData->auth.Login(), clientData->connID)
											   : db->WithTimeout(ctx.call->execTimeout);
			}
		}
	}
	throw Error(errParams, "Database is not opened, you should open it first");
}

Error ClusterManagementServer::SuggestLeader(cproto::Context &ctx, p_string suggestion) {
	cluster::NodeData sug, res;
	auto err = sug.FromJSON(giftStr(suggestion));
	if (err.ok()) {
		err = getDB(ctx, kRoleDataWrite).SuggestLeader(sug, res);
	}
	if (err.ok()) {
		WrSerializer ser;
		res.GetJSON(ser);
		auto serSlice = ser.Slice();
		ctx.Return({cproto::Arg(p_string(&serSlice))});
	}
	return err;
}

Error ClusterManagementServer::LeadersPing(cproto::Context &ctx, p_string leader) {
	cluster::NodeData l;
	auto err = l.FromJSON(giftStr(leader));
	if (err.ok()) {
		err = getDB(ctx, kRoleDataWrite).LeadersPing(l);
	}
	return err;
}

Error ClusterManagementServer::GetRaftInfo(cproto::Context &ctx) {
	cluster::RaftInfo info;
	auto err = getDB(ctx, kRoleDataWrite).GetRaftInfo(info);
	if (err.ok()) {
		WrSerializer ser;
		info.GetJSON(ser);
		auto serSlice = ser.Slice();
		ctx.Return({cproto::Arg(p_string(&serSlice))});
	}
	return err;
}

bool ClusterManagementServer::Start(const string &addr, ev::dynamic_loop &loop, bool enableStat) {
	dispatcher_.Register(cproto::kCmdPing, this, &ClusterManagementServer::Ping);
	dispatcher_.Register(cproto::kCmdLogin, this, &ClusterManagementServer::Login, true);
	dispatcher_.Register(cproto::kCmdOpenDatabase, this, &ClusterManagementServer::OpenDatabase, true);
	dispatcher_.Register(cproto::kCmdSuggestLeader, this, &ClusterManagementServer::SuggestLeader);
	dispatcher_.Register(cproto::kCmdLeadersPing, this, &ClusterManagementServer::LeadersPing);
	dispatcher_.Register(cproto::kCmdGetRaftInfo, this, &ClusterManagementServer::GetRaftInfo);
	dispatcher_.Middleware(this, &ClusterManagementServer::CheckAuth);
	dispatcher_.OnClose(this, &ClusterManagementServer::OnClose);
	dispatcher_.OnResponse(this, &ClusterManagementServer::OnResponse);

	if (logger_) {
		dispatcher_.Logger(this, &ClusterManagementServer::Logger);
	}

	listener_.reset(new Listener(loop, cproto::ServerConnection::NewFactory(dispatcher_, enableStat, 1)));
	return listener_->Bind(addr);
}

ClusterManagerClientData::~ClusterManagerClientData() {
	Reindexer *db = nullptr;
	auth.GetDB(kRoleNone, &db);
}

}  // namespace reindexer_server
