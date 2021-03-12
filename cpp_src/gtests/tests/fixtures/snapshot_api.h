#pragma once

#include <tools/fsops.h>
#include "client/snapshot.h"
#include "core/namespace/snapshot/snapshot.h"
#include "rpcclient_api.h"

class SnapshotTestApi : public RPCClientTestApi {
public:
	SnapshotTestApi() {}
	virtual ~SnapshotTestApi() {}

protected:
	struct NsDataState {
		lsn_t lsn;
		uint64_t dataHash = 0;
		int64_t dataCount = 0;
	};

	void SetUp() {
		fs::RmDirAll(kBaseTestsetDbPath);
		StartServer();
	}
	void TearDown() { RPCClientTestApi::StopAllServers(); }

	void StartServer() {
		const std::string dbPath = string(kBaseTestsetDbPath) + "/" + std::to_string(kDefaultRPCPort);
		RPCClientTestApi::AddRealServer(dbPath, kDefaultRPCServerAddr, kDefaultHttpPort, kDefaultClusterPort);
		RPCClientTestApi::StartServer(kDefaultRPCServerAddr);
	}

	template <typename RxT>
	void InitNS(RxT& rx, string_view nsName) {
		Error err = rx.OpenNamespace(kNsName);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddIndex(nsName, {"id", {"id"}, "hash", "int", IndexOpts().PK()});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddIndex(nsName, {"int", {"int"}, "tree", "int", IndexOpts()});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddIndex(nsName, {"string", {"string"}, "hash", "string", IndexOpts()});
		ASSERT_TRUE(err.ok()) << err.what();
	}

	template <typename RxT>
	void FillData(RxT& rx, string_view nsName, size_t from) {
		const int kUpsertItemsCnt = 500;
		const int kUpdateItemsCnt = kUpsertItemsCnt / 10;
		const int kDeleteItemsCnt = kUpsertItemsCnt / 10;
		const int kTxItemsCnt = kUpsertItemsCnt;

		Error err;
		for (int i = 0; i < kUpsertItemsCnt; ++i) {
			auto item = rx.NewItem(nsName);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			// clang-format off
			err = item.FromJSON("{"
								"\"id\": " + std::to_string(i + from) + "," ""
								"\"int\": " + std::to_string(rand()) + ","
								"\"string\": \"" + randStringAlph(15) + "\""
								"}");
			// clang-format on
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx.Upsert(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
		}

		for (int i = 0; i < kUpdateItemsCnt; ++i) {
			auto item = rx.NewItem(nsName);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			// clang-format off
			err = item.FromJSON("{"
								"\"id\": " + std::to_string(from + i * 5) + "," ""
								"\"int\": " + std::to_string(rand()) + ","
								"\"string\": \"" + randStringAlph(15) + "\""
								"}");
			// clang-format on
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx.Upsert(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
		}

		for (int i = 0; i < kDeleteItemsCnt; ++i) {
			auto item = rx.NewItem(nsName);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			// clang-format off
			err = item.FromJSON("{"
								"\"id\": " + std::to_string(from + i * 7) + "," ""
								"\"int\": " + std::to_string(rand()) + ","
								"\"string\": \"" + randStringAlph(15) + "\""
								"}");
			// clang-format on
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx.Delete(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
		}

		auto tx = rx.NewTransaction(nsName);
		ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
		for (int i = 0; i < kTxItemsCnt; ++i) {
			auto item = rx.NewItem(nsName);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			// clang-format off
			err = item.FromJSON("{"
								"\"id\": " + std::to_string(from + i * 2) + "," ""
								"\"int\": " + std::to_string(rand()) + ","
								"\"string\": \"" + randStringAlph(15) + "\""
								"}");
			// clang-format on
			ASSERT_TRUE(err.ok()) << err.what();
			if (i % 2) {
				tx.Upsert(std::move(item));
			} else {
				tx.Delete(std::move(item));
			}
		}
		commitTx(rx, tx);
	}

	template <typename RxT>
	NsDataState GetNsDataState(RxT& rx, const std::string& ns) {
		Query qr = Query("#memstats").Where("name", CondEq, ns);
		typename RxT::QueryResultsT res;
		auto err = rx.Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();
		NsDataState state;
		for (auto it : res) {
			WrSerializer ser;
			err = it.GetJSON(ser, false);
			EXPECT_TRUE(err.ok()) << err.what();
			gason::JsonParser parser;
			auto root = parser.Parse(ser.Slice());
			state.lsn.FromJSON(root["replication"]["last_lsn_v2"]);
			state.dataCount = root["replication"]["data_count"].As<int64_t>();
			state.dataHash = root["replication"]["data_hash"].As<uint64_t>();
		}
		return state;
	}

	void Connect(net::ev::dynamic_loop& loop, reindexer::client::CoroReindexer& rxClient, reindexer::Reindexer& localRx) {
		auto err = localRx.Connect("builtin://" + kLocalDbPath + "/db1");
		ASSERT_TRUE(err.ok()) << err.what();

		reindexer::client::ConnectOpts opts;
		opts.CreateDBIfMissing();
		err = rxClient.Connect(string("cproto://") + kDefaultRPCServerAddr + "/db1", loop, opts);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void CompareData(reindexer::client::CoroReindexer& rxClient, reindexer::Reindexer& localRx) {
		auto remoteState = GetNsDataState(rxClient, kNsName);
		auto localState = GetNsDataState(localRx, kNsName);
		EXPECT_EQ(remoteState.dataHash, localState.dataHash);
		EXPECT_EQ(remoteState.dataCount, localState.dataCount);
		EXPECT_TRUE(remoteState.lsn == localState.lsn) << "remote: " << remoteState.lsn << "; local: " << localState.lsn;
	}

	void CompareWalSnapshots(reindexer::client::CoroReindexer& rxClient, reindexer::Reindexer& localRx) {
		client::Snapshot csn;
		rxClient.GetSnapshot(kNsName, lsn_t(0, 0), csn);
		Snapshot sn;
		localRx.GetSnapshot(kNsName, lsn_t(0, 0), sn);
		auto it2 = sn.begin();
		for (auto it1 = csn.begin(); it1 != csn.end() || it2 != sn.end(); ++it1, ++it2) {
			ASSERT_TRUE(it1 != csn.end());
			ASSERT_TRUE(it2 != sn.end());
			auto ch1 = it1.Chunk();
			auto ch2 = it2.Chunk();
			EXPECT_EQ(ch1.Records().size(), ch2.Records().size());
			EXPECT_EQ(ch1.IsTx(), ch2.IsTx());
			EXPECT_EQ(ch1.IsWAL(), ch2.IsWAL());
			EXPECT_EQ(ch1.IsShallow(), ch2.IsShallow());
		}
	}

	const std::string kBaseTestsetDbPath = fs::JoinPath(fs::GetTempDir(), "rx_test/SnapshotApi");
	static const uint16_t kDefaultRPCPort = 25685;
	const uint16_t kDefaultHttpPort = 33433;
	const uint16_t kDefaultClusterPort = 33933;
	static const std::string kDefaultRPCServerAddr;
	const std::string kLocalDbPath = string(kBaseTestsetDbPath) + "/local";
	const std::string kNsName = "snapshot_test_ns";

private:
	void commitTx(client::CoroReindexer& rx, client::CoroTransaction& tx) {
		auto err = rx.CommitTransaction(tx);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	void commitTx(Reindexer& rx, Transaction& tx) {
		QueryResults qr;
		auto err = rx.CommitTransaction(tx, qr);
		ASSERT_TRUE(err.ok()) << err.what();
	}
};
