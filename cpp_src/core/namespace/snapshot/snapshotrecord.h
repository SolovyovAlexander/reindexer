#pragma once

#include "replicator/walrecord.h"
#include "tools/lsn.h"
#include "tools/serializer.h"

namespace reindexer {

enum SnapshotRecordOpts { kShallowSnapshotChunk = 1 << 0, kWALSnapshotChunk = 1 << 1, kTxSnapshotChunk = 1 << 2 };

class SnapshotRecord {
public:
	SnapshotRecord() = default;
	SnapshotRecord(lsn_t lsn, PackedWALRecord &&wrec) : lsn_(lsn), rec_(std::move(wrec)) {}
	void Deserialize(Serializer &ser);
	void Serilize(WrSerializer &ser) const;
	WALRecord Unpack() const { return WALRecord(rec_); }
	const PackedWALRecord &Record() const { return rec_; }
	lsn_t LSN() const noexcept { return lsn_; }

private:
	lsn_t lsn_ = lsn_t();
	PackedWALRecord rec_;
};

class SnapshotChunk {
public:
	const std::vector<SnapshotRecord> &Records() const noexcept { return records; }

	void Deserialize(Serializer &ser);
	void Serilize(WrSerializer &ser) const;

	void MarkShallow(bool v = true) noexcept { opts = v ? opts | kShallowSnapshotChunk : opts & ~(kShallowSnapshotChunk); }
	void MarkWAL(bool v = true) noexcept { opts = v ? opts | kWALSnapshotChunk : opts & ~(kWALSnapshotChunk); }
	void MarkTx(bool v = true) noexcept { opts = v ? opts | kTxSnapshotChunk : opts & ~(kTxSnapshotChunk); }

	bool IsShallow() const noexcept { return opts & kShallowSnapshotChunk; }
	bool IsWAL() const noexcept { return opts & kWALSnapshotChunk; }
	bool IsTx() const noexcept { return opts & kTxSnapshotChunk; }

	std::vector<SnapshotRecord> records;
	uint16_t opts = 0;
};

}  // namespace reindexer
