#pragma once

#include "estl/fast_hash_set.h"
#include "estl/span.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace gason {
struct JsonNode;
}

namespace Yaml {
class Node;
}

namespace reindexer {

class JsonBuilder;
class WrSerializer;

namespace cluster {

struct NodeData {
	int serverId = 0;
	int electionsTerm = 0;
	std::string dsn;

	Error FromJSON(span<char> json);
	Error FromJSON(const gason::JsonNode &v);
	void GetJSON(JsonBuilder &jb) const;
	void GetJSON(WrSerializer &ser) const;
};

struct RaftInfo {
	enum class Role : uint8_t { None, Leader, Follower, Candidate };
	int32_t leaderId = -1;
	Role role = Role::None;

	bool operator==(const RaftInfo &rhs) const noexcept { return role == rhs.role && leaderId == rhs.leaderId; }
	bool operator!=(const RaftInfo &rhs) const noexcept { return !(*this == rhs); }

	Error FromJSON(span<char> json);
	Error FromJSON(const gason::JsonNode &root);
	void GetJSON(JsonBuilder &jb) const;
	void GetJSON(WrSerializer &ser) const;
	static string_view RoleToStr(Role);
	static Role RoleFromStr(string_view);
};

struct NodeConfig {
	std::string GetRPCDsn() const;
	std::string GetManagementDsn() const;
	void FromYML(Yaml::Node &yaml);

	bool operator==(const NodeConfig &rdata) const noexcept {
		return (serverId == rdata.serverId) && (ipAddr == rdata.ipAddr) && (dbName == rdata.dbName) && (rpcPort == rdata.rpcPort) &&
			   (managementPort == rdata.managementPort);
	}

	int serverId = -1;
	std::string ipAddr;
	std::string dbName;
	uint16_t rpcPort = 0;
	uint16_t managementPort = 0;
};

struct ClusterConfigData {
	Error FromYML(const std::string &yaml);

	bool operator==(const ClusterConfigData &rdata) const noexcept {
		return (nodes == rdata.nodes) && (retrySyncIntervalMSec == rdata.retrySyncIntervalMSec) &&
			   (updatesTimeoutSec == rdata.updatesTimeoutSec) && (enableCompression == rdata.enableCompression) &&
			   (appName == rdata.appName) && (syncThreadsCount == rdata.syncThreadsCount);
	}
	bool operator!=(const ClusterConfigData &rdata) const noexcept { return !operator==(rdata); }

	std::vector<NodeConfig> nodes;
	fast_hash_set<string, nocase_hash_str, nocase_equal_str> namespaces;
	std::string appName = "rx_node";
	int updatesTimeoutSec = 20;
	int retrySyncIntervalMSec = 3000;
	int syncThreadsCount = 4;
	bool enableCompression = true;
};

}  // namespace cluster
}  // namespace reindexer
