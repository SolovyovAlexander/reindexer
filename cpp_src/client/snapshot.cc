#include "snapshot.h"

namespace reindexer {
namespace client {

Snapshot::Snapshot(net::cproto::CoroClientConnection* conn, int id, int64_t count, int64_t rawCount, string_view data,
				   std::chrono::milliseconds timeout)
	: id_(id), count_(count), rawCount_(rawCount), conn_(conn), requestTimeout_(timeout) {
	if (id < 0) {
		throw Error(errLogic, "Unexpectd snapshot id: %d", id);
	}
	if (count < 0) {
		throw Error(errLogic, "Unexpectd snapshot size: %d", count);
	}

	if (count_ > 0) {
		parseFrom(data);
	}
}

void Snapshot::fetchNext(size_t idx) {
	auto ret = conn_->Call({net::cproto::kCmdFetchSnapshot, requestTimeout_, std::chrono::milliseconds(0), lsn_t(), int(-1), nullptr}, id_,
						   int64_t(idx));
	if (!ret.Status().ok()) {
		throw ret.Status();
	}

	auto args = ret.GetArgs(1);
	parseFrom(p_string(args[0]));
}

void Snapshot::parseFrom(string_view data) {
	SnapshotChunk ch;
	Serializer ser(data);
	ch.Deserialize(ser);
	data_ = std::move(ch);
}

}  // namespace client
}  // namespace reindexer
