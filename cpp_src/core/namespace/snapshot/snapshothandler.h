#pragma once

#include "cluster/updaterecord.h"
#include "snapshot.h"

namespace reindexer {

class NamespaceImpl;
class Namespace;

class SnapshotHandler {
public:
	SnapshotHandler(NamespaceImpl& ns) : ns_(ns) {}

	Snapshot CreateSnapshot(lsn_t from) const;
	void ApplyChunk(const SnapshotChunk& ch, h_vector<cluster::UpdateRecord, 1>& repl);

private:
	struct ChunkContext {
		bool wal = false;
		bool shallow = false;
		bool tx = false;
	};

	void applyRecord(const SnapshotRecord& rec, const ChunkContext& ctx, h_vector<cluster::UpdateRecord, 1>& repl);
	Error applyShallowRecord(lsn_t lsn, WALRecType type, const PackedWALRecord& wrec, const ChunkContext& chCtx);
	Error applyRealRecord(lsn_t lsn, const SnapshotRecord& snRec, const ChunkContext& chCtx, h_vector<cluster::UpdateRecord, 1>& repl);
	WALRecord rebuildUpdateWalRec(const PackedWALRecord& wrec, const ChunkContext& chCtx);

	NamespaceImpl& ns_;
	RdxContext dummyCtx_;
};

class SnapshotTxHandler {
public:
	SnapshotTxHandler(Namespace& ns) : ns_(ns) {}
	void ApplyChunk(const SnapshotChunk& ch, const RdxContext& rdxCtx);

private:
	Namespace& ns_;
};

}  // namespace reindexer
