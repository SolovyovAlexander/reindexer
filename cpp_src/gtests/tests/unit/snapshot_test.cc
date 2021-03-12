#include "core/type_consts.h"
#include "coroutine/waitgroup.h"
#include "net/ev/ev.h"
#include "snapshot_api.h"

const std::string SnapshotTestApi::kDefaultRPCServerAddr = std::string("127.0.0.1:") + std::to_string(SnapshotTestApi::kDefaultRPCPort);

TEST_F(SnapshotTestApi, ForceSyncFromLocalToRemote) {
	// Check if we can apply snapshot from local rx instance to remote rx instance via RPC
	ev::dynamic_loop loop;
	bool finished = false;
	loop.spawn([this, &loop, &finished] {
		reindexer::Reindexer localRx;
		reindexer::client::CoroReindexer rxClient;
		Connect(loop, rxClient, localRx);

		InitNS(localRx, kNsName);
		FillData(localRx, kNsName, 0);

		// Checking full snapshot
		{
			Snapshot crsn;
			auto err = localRx.GetSnapshot(kNsName, lsn_t(), crsn);
			ASSERT_TRUE(err.ok()) << err.what();

			std::string tmpNsName;
			err = rxClient.WithLSN(lsn_t(0, 0)).CreateTemporaryNamespace(kNsName, tmpNsName, StorageOpts().Enabled(false));
			ASSERT_TRUE(err.ok()) << err.what();

			for (auto& it : crsn) {
				auto ch = it.Chunk();
				err = rxClient.WithLSN(lsn_t(0, 0)).ApplySnapshotChunk(tmpNsName, ch);
				ASSERT_TRUE(err.ok()) << err.what();
			}
			err = rxClient.WithLSN(lsn_t(0, 0)).RenameNamespace(tmpNsName, kNsName);
			ASSERT_TRUE(err.ok()) << err.what();
		}
		CompareData(rxClient, localRx);
		CompareWalSnapshots(rxClient, localRx);

		// Checking WAL snapshot
		FillData(localRx, kNsName, 1000);
		{
			auto remoteState = GetNsDataState(rxClient, kNsName);
			Snapshot crsn;
			auto err = localRx.GetSnapshot(kNsName, remoteState.lsn, crsn);
			ASSERT_TRUE(err.ok()) << err.what();
			for (auto& it : crsn) {
				auto ch = it.Chunk();
				err = rxClient.WithLSN(lsn_t(0, 0)).ApplySnapshotChunk(kNsName, ch);
				ASSERT_TRUE(err.ok()) << err.what();
			}
		}
		CompareData(rxClient, localRx);
		CompareWalSnapshots(rxClient, localRx);

		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
}

TEST_F(SnapshotTestApi, ForceSyncFromRemoteToLocal) {
	// Check if we can apply snapshot from remote rx instance to local rx instance via RPC
	ev::dynamic_loop loop;
	bool finished = false;
	loop.spawn([this, &loop, &finished] {
		reindexer::Reindexer localRx;
		reindexer::client::CoroReindexer rxClient;
		Connect(loop, rxClient, localRx);

		InitNS(rxClient, kNsName);
		FillData(rxClient, kNsName, 0);

		// Checking full snapshot
		{
			client::Snapshot crsn;
			auto err = rxClient.GetSnapshot(kNsName, lsn_t(), crsn);
			ASSERT_TRUE(err.ok()) << err.what();

			std::string tmpNsName;
			err = localRx.WithLSN(lsn_t(0, 0)).CreateTemporaryNamespace(kNsName, tmpNsName, StorageOpts().Enabled(false));
			ASSERT_TRUE(err.ok()) << err.what();

			for (auto& it : crsn) {
				auto& ch = it.Chunk();
				err = localRx.WithLSN(lsn_t(0, 0)).ApplySnapshotChunk(tmpNsName, ch);
				ASSERT_TRUE(err.ok()) << err.what();
			}

			err = localRx.WithLSN(lsn_t(0, 0)).RenameNamespace(tmpNsName, kNsName);
			ASSERT_TRUE(err.ok()) << err.what();
		}
		CompareData(rxClient, localRx);
		CompareWalSnapshots(rxClient, localRx);

		// Checking WAL snapshot
		FillData(rxClient, kNsName, 1000);
		{
			auto remoteState = GetNsDataState(localRx, kNsName);
			client::Snapshot crsn;
			auto err = rxClient.GetSnapshot(kNsName, remoteState.lsn, crsn);
			ASSERT_TRUE(err.ok()) << err.what();
			for (auto& it : crsn) {
				auto& ch = it.Chunk();
				err = localRx.WithLSN(lsn_t(0, 0)).ApplySnapshotChunk(kNsName, ch);
				ASSERT_TRUE(err.ok()) << err.what();
			}
		}
		CompareData(rxClient, localRx);
		CompareWalSnapshots(rxClient, localRx);

		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
}
