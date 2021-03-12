#include "namespace.h"
#include "core/storage/storagefactory.h"
#include "snapshot/snapshothandler.h"
#include "snapshot/snapshotrecord.h"
#include "tools/fsops.h"
#include "tools/logger.h"

namespace reindexer {

#define handleInvalidation(Fn) nsFuncWrapper<decltype(&Fn), &Fn>

void Namespace::CommitTransaction(Transaction& tx, QueryResults& result, const NsContext& ctx) {
	auto ns = atomicLoadMainNs();
	bool enablePerfCounters = ns->enablePerfCounters_.load(std::memory_order_relaxed);
	if (enablePerfCounters) {
		txStatsCounter_.Count(tx);
	}
	PerfStatCalculatorMT txCommitCalc(commitStatsCounter_, enablePerfCounters);
	if (needNamespaceCopy(ns, tx)) {
		PerfStatCalculatorMT calc(ns->updatePerfCounter_, enablePerfCounters);
		contexted_unique_lock<Mutex, const RdxContext> lck(clonerMtx_, &ctx.rdxContext);
		ns = ns_;
		if (needNamespaceCopy(ns, tx)) {
			PerfStatCalculatorMT nsCopyCalc(copyStatsCounter_, enablePerfCounters);
			calc.SetCounter(ns->updatePerfCounter_);
			calc.LockHit();
			logPrintf(LogTrace, "Namespace::CommitTransaction creating copy for (%s)", ns->name_);
			hasCopy_.store(true, std::memory_order_release);
			ns->cancelCommit_ = true;  // -V519
			try {
				auto lck = ns->rLock(ctx.rdxContext);
				auto storageLock = ns->locker_.StorageLock();
				ns->cancelCommit_ = false;	// -V519
				nsCopy_.reset(new NamespaceImpl(*ns));
				nsCopyCalc.HitManualy();
				NsContext nsCtx(ctx);
				nsCtx.CopiedNsRequest();
				nsCopy_->CommitTransaction(tx, result, nsCtx);
				nsCopy_->optimizeIndexes(nsCtx);
				calc.SetCounter(nsCopy_->updatePerfCounter_);
				ns->markReadOnly();
				atomicStoreMainNs(nsCopy_.release());
				hasCopy_.store(false, std::memory_order_release);
			} catch (...) {
				calc.enable_ = false;
				nsCopy_.reset();
				hasCopy_.store(false, std::memory_order_release);
				throw;
			}
			return;
		}
	}
	handleInvalidation(NamespaceImpl::CommitTransaction)(tx, result, ctx);
}

NamespacePerfStat Namespace::GetPerfStat(const RdxContext& ctx) {
	NamespacePerfStat stats = handleInvalidation(NamespaceImpl::GetPerfStat)(ctx);
	stats.transactions = txStatsCounter_.Get();
	auto copyStats = copyStatsCounter_.Get<PerfStat>();
	stats.transactions.totalCopyCount = copyStats.totalHitCount;
	stats.transactions.minCopyTimeUs = copyStats.minTimeUs;
	stats.transactions.maxCopyTimeUs = copyStats.maxTimeUs;
	stats.transactions.avgCopyTimeUs = copyStats.totalTimeUs / (copyStats.totalHitCount ? copyStats.totalHitCount : 1);
	auto commitStats = commitStatsCounter_.Get<PerfStat>();
	stats.transactions.totalCount = commitStats.totalHitCount;
	stats.transactions.minCommitTimeUs = commitStats.minTimeUs;
	stats.transactions.maxCommitTimeUs = commitStats.maxTimeUs;
	stats.transactions.avgCommitTimeUs = commitStats.totalTimeUs / (commitStats.totalHitCount ? commitStats.totalHitCount : 1);
	return stats;
}

void Namespace::ApplySnapshotChunk(const SnapshotChunk& ch, const RdxContext& ctx) {
	if (!ch.IsTx() || ch.IsShallow() || !ch.IsWAL()) {
		return handleInvalidation(NamespaceImpl::ApplySnapshotChunk)(ch, ctx);
	} else {
		SnapshotTxHandler handler(*this);
		handler.ApplyChunk(ch, ctx);
	}
}

bool Namespace::needNamespaceCopy(const NamespaceImpl::Ptr& ns, const Transaction& tx) const noexcept {
	auto stepsCount = tx.GetSteps().size();
	auto startCopyPolicyTxSize = static_cast<uint32_t>(startCopyPolicyTxSize_.load(std::memory_order_relaxed));
	auto copyPolicyMultiplier = static_cast<uint32_t>(copyPolicyMultiplier_.load(std::memory_order_relaxed));
	auto txSizeToAlwaysCopy = static_cast<uint32_t>(txSizeToAlwaysCopy_.load(std::memory_order_relaxed));
	return ((stepsCount >= startCopyPolicyTxSize) && (ns->GetItemsCapacity() <= copyPolicyMultiplier * stepsCount)) ||
		   (stepsCount >= txSizeToAlwaysCopy);
}

void Namespace::doRename(Namespace::Ptr dst, const std::string& newName, const std::string& storagePath, const RdxContext& ctx) {
	std::string dbpath;
	handleInvalidation(NamespaceImpl::flushStorage)(ctx);
	auto lck = handleInvalidation(NamespaceImpl::wLock)(ctx);
	auto& srcNs = *atomicLoadMainNs();	// -V758
	std::unique_lock<NamespaceImpl::Mutex> dstLck;
	NamespaceImpl::Ptr dstNs;
	if (dst) {
		while (true) {
			try {
				dstNs = dst->awaitMainNs(ctx);
				dstLck = std::unique_lock<NamespaceImpl::Mutex>(*dstNs->wLock(ctx).release(), adopt_lock_t());
				break;
			} catch (const Error& e) {
				if (e.code() != errNamespaceInvalidated) {
					throw;
				} else {
					std::this_thread::yield();
				}
			}
		}

		dstNs->checkClusterRole(ctx);
		dbpath = dstNs->dbpath_;
	} else if (newName == srcNs.name_) {
		return;
	}
	srcNs.checkClusterRole(ctx);

	if (dbpath.empty()) {
		dbpath = fs::JoinPath(storagePath, newName);
	} else {
		dstNs->deleteStorage();
	}

	bool hadStorage = (srcNs.storage_ != nullptr);
	auto storageType = StorageType::LevelDB;
	if (hadStorage) {
		storageType = srcNs.storage_->Type();
		srcNs.storage_.reset();
		fs::RmDirAll(dbpath);
		int renameRes = fs::Rename(srcNs.dbpath_, dbpath);
		if (renameRes < 0) {
			if (dst) {
				assert(dstLck.owns_lock());
				dstLck.unlock();
			}
			throw Error(errParams, "Unable to rename '%s' to '%s'", srcNs.dbpath_, dbpath);
		}
	}

	if (dst) {
		logPrintf(LogInfo, "Rename namespace '%s' to '%s'", srcNs.name_, dstNs->name_);
		srcNs.name_ = dstNs->name_;
		assert(dstLck.owns_lock());
		dstLck.unlock();
	} else {
		logPrintf(LogInfo, "Rename namespace '%s' to '%s'", srcNs.name_, newName);
		srcNs.name_ = newName;
	}
	srcNs.payloadType_.SetName(srcNs.name_);

	if (hadStorage) {
		logPrintf(LogTrace, "Storage was moved from %s to %s", srcNs.dbpath_, dbpath);
		srcNs.dbpath_ = std::move(dbpath);
		srcNs.storage_.reset(datastorage::StorageFactory::create(storageType));
		auto status = srcNs.storage_->Open(srcNs.dbpath_, srcNs.storageOpts_);
		if (!status.ok()) {
			throw status;
		}
		if (srcNs.repl_.temporary) {
			srcNs.repl_.temporary = false;
			srcNs.saveReplStateToStorage();
		}
	}
}

}  // namespace reindexer
