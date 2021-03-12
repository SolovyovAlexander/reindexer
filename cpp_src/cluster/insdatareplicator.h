#pragma once

#include "core/rdxcontext.h"
#include "updaterecord.h"

namespace reindexer {
namespace cluster {

using UpdatesContainer = h_vector<cluster::UpdateRecord, 1>;

struct INsDataReplicator {
	virtual Error Replicate(UpdateRecord &&rec, std::function<void()> beforeWaitF, const RdxContext &ctx) = 0;
	virtual Error Replicate(UpdatesContainer &&recs, std::function<void()> beforeWaitF, const RdxContext &ctx) = 0;
	virtual void AwaitSynchronization(string_view nsName, const RdxContext &ctx) const = 0;
	virtual bool IsSynchronized(string_view name) const = 0;

	virtual ~INsDataReplicator() {}
};

}  // namespace cluster
}  // namespace reindexer
