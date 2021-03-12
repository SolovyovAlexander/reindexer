#include "snapshotrecord.h"

namespace reindexer {

void SnapshotRecord::Deserialize(Serializer& ser) {
	lsn_ = lsn_t(ser.GetVarint());
	auto recSV = ser.GetSlice();
	rec_.resize(recSV.size());
	memcpy(rec_.data(), recSV.data(), recSV.size());
}

void SnapshotRecord::Serilize(WrSerializer& ser) const {
	ser.PutVarint(int64_t(lsn_));
	ser.PutSlice(string_view(reinterpret_cast<const char*>(rec_.data()), rec_.size()));
}

void SnapshotChunk::Deserialize(Serializer& ser) {
	opts = ser.GetVarUint();
	auto size = ser.GetVarUint();
	records.resize(size);
	for (auto& rec : records) {
		rec.Deserialize(ser);
	}
}

void SnapshotChunk::Serilize(WrSerializer& ser) const {
	ser.PutVarUint(opts);
	ser.PutVarUint(records.size());
	for (auto& rec : records) {
		rec.Serilize(ser);
	}
}

}  // namespace reindexer
