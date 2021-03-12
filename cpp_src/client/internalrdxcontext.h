#pragma once

#include <chrono>
#include "core/rdxcontext.h"
#include "tools/errors.h"

namespace reindexer {
namespace client {

using std::chrono::milliseconds;

class InternalRdxContext {
public:
	typedef std::function<void(const Error& err)> Completion;
	explicit InternalRdxContext(const IRdxCancelContext* cancelCtx, Completion cmpl = nullptr, milliseconds execTimeout = milliseconds(0),
								lsn_t lsn = lsn_t(), int serverId = -1) noexcept
		: cmpl_(cmpl),
		  execTimeout_((execTimeout.count() < 0) ? milliseconds(0) : execTimeout),
		  cancelCtx_(cancelCtx),
		  lsn_(lsn),
		  serverId_(serverId) {}
	explicit InternalRdxContext(Completion cmpl = nullptr, milliseconds execTimeout = milliseconds(0), lsn_t lsn = lsn_t(),
								int serverId = -1) noexcept
		: cmpl_(cmpl),
		  execTimeout_((execTimeout.count() < 0) ? milliseconds(0) : execTimeout),
		  cancelCtx_(nullptr),
		  lsn_(lsn),
		  serverId_(serverId) {}

	InternalRdxContext WithCancelContext(const IRdxCancelContext* cancelCtx) noexcept {
		return InternalRdxContext(cancelCtx, cmpl_, execTimeout_, lsn_);
	}
	InternalRdxContext WithCompletion(Completion cmpl, InternalRdxContext&) noexcept {
		return InternalRdxContext(cmpl, execTimeout_, lsn_);
	}
	InternalRdxContext WithCompletion(Completion cmpl) const noexcept { return InternalRdxContext(cmpl, execTimeout_, lsn_, serverId_); }
	InternalRdxContext WithTimeout(milliseconds execTimeout) const noexcept {
		return InternalRdxContext(cmpl_, execTimeout, lsn_, serverId_);
	}
	InternalRdxContext WithLSN(lsn_t lsn) const noexcept { return InternalRdxContext(cmpl_, execTimeout_, lsn, serverId_); }
	InternalRdxContext WithServerId(unsigned short serverId) const noexcept {
		return InternalRdxContext(cmpl_, execTimeout_, lsn_, serverId);
	}

	Completion cmpl() const noexcept { return cmpl_; }
	milliseconds execTimeout() const noexcept { return execTimeout_; }
	const IRdxCancelContext* getCancelCtx() const noexcept { return cancelCtx_; }
	lsn_t lsn() const noexcept { return lsn_; }
	int serverId() const noexcept { return serverId_; }

private:
	Completion cmpl_;
	milliseconds execTimeout_;
	const IRdxCancelContext* cancelCtx_;
	lsn_t lsn_;
	int serverId_;
};

}  // namespace client
}  // namespace reindexer
