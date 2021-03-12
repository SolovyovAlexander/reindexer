#include "cluster/config.h"

#include "core/cjson/jsonbuilder.h"
#include "gason/gason.h"
#include "yaml/yaml.h"

namespace reindexer {
namespace cluster {

Error NodeData::FromJSON(span<char> json) {
	try {
		FromJSON(gason::JsonParser().Parse(json));
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "NodeData: %s", ex.what());
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error NodeData::FromJSON(const gason::JsonNode &root) {
	try {
		serverId = root["server_id"].As<int>(serverId);
		electionsTerm = root["elections_term"].As<int>(electionsTerm);
		dsn = root["dsn"].As<std::string>();
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "ReplicationConfigData: %s", ex.what());
	}
	return errOK;
}

void NodeData::GetJSON(JsonBuilder &jb) const {
	jb.Put("server_id", serverId);
	jb.Put("elections_term", electionsTerm);
	jb.Put("dsn", dsn);
}

void NodeData::GetJSON(WrSerializer &ser) const {
	JsonBuilder jb(ser);
	GetJSON(jb);
}

Error RaftInfo::FromJSON(span<char> json) {
	try {
		FromJSON(gason::JsonParser().Parse(json));
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "NodeData: %s", ex.what());
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error RaftInfo::FromJSON(const gason::JsonNode &root) {
	try {
		leaderId = root["leader_id"].As<int>(leaderId);
		role = RoleFromStr(root["role"].As<string_view>());
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "ReplicationConfigData: %s", ex.what());
	}
	return errOK;
}

void RaftInfo::GetJSON(JsonBuilder &jb) const {
	jb.Put("leader_id", leaderId);
	jb.Put("role", RoleToStr(role));
}

void RaftInfo::GetJSON(WrSerializer &ser) const {
	JsonBuilder jb(ser);
	GetJSON(jb);
}

string_view RaftInfo::RoleToStr(RaftInfo::Role role) {
	switch (role) {
		case RaftInfo::Role::None:
			return "none"_sv;
		case RaftInfo::Role::Leader:
			return "leader"_sv;
		case RaftInfo::Role::Candidate:
			return "candidate"_sv;
		case RaftInfo::Role::Follower:
			return "follower"_sv;
		default:
			return "unknown"_sv;
	}
}

RaftInfo::Role RaftInfo::RoleFromStr(string_view role) {
	if (role == "leader"_sv) {
		return RaftInfo::Role::Leader;
	} else if (role == "candidate"_sv) {
		return RaftInfo::Role::Candidate;
	} else if (role == "follower"_sv) {
		return RaftInfo::Role::Follower;
	} else {
		return RaftInfo::Role::None;
	}
}

std::string NodeConfig::GetRPCDsn() const { return fmt::format("cproto://{}:{}/{}", ipAddr, rpcPort, dbName); }

std::string NodeConfig::GetManagementDsn() const { return fmt::format("cproto://{}:{}/{}", ipAddr, managementPort, dbName); }

void NodeConfig::FromYML(Yaml::Node &root) {
	serverId = root["server_id"].As<int>(serverId);
	dbName = root["database"].As<std::string>(dbName);
	rpcPort = root["rpc_port"].As<uint16_t>(rpcPort);
	ipAddr = root["ip_addr"].As<std::string>(ipAddr);
	managementPort = root["management_port"].As<uint16_t>(managementPort);
}

Error ClusterConfigData::FromYML(const std::string &yaml) {
	Yaml::Node root;
	try {
		Yaml::Parse(root, yaml);
		appName = root["app_name"].As<std::string>(appName);
		syncThreadsCount = root["sync_threads"].As<int>(syncThreadsCount);
		updatesTimeoutSec = root["updates_timeout_sec"].As<int>(updatesTimeoutSec);
		retrySyncIntervalMSec = root["retry_sync_interval_msec"].As<int>(retrySyncIntervalMSec);
		enableCompression = root["enable_compression"].As<bool>(enableCompression);
		{
			auto &node = root["namespaces"];
			namespaces.clear();
			for (unsigned i = 0; i < node.Size(); i++) {
				namespaces.insert(node[i].As<std::string>());
			}
		}
		{
			auto &node = root["nodes"];
			nodes.clear();
			for (unsigned i = 0; i < node.Size(); i++) {
				NodeConfig conf;
				conf.FromYML(node[i]);
				nodes.emplace_back(std::move(conf));
			}
		}
		return errOK;
	} catch (const Yaml::Exception &ex) {
		return Error(errParams, "yaml parsing error: '%s'", ex.Message());
	} catch (const Error &err) {
		return err;
	}
}

}  // namespace cluster
}  // namespace reindexer
