#pragma once

#include "core/cjson/tagsmatcher.h"
#include "core/queryresults/queryresults.h"
#include "snapshotrecord.h"

namespace reindexer {

class Snapshot {
public:
	Snapshot() = default;
	Snapshot(PayloadType pt, TagsMatcher tm, lsn_t lastLsn, uint64_t expectedDatahash, QueryResults &&wal,
			 QueryResults &&raw = QueryResults());
	Snapshot(const Snapshot &) = delete;
	Snapshot(Snapshot &&) = default;
	Snapshot &operator=(const Snapshot &) = delete;
	Snapshot &operator=(Snapshot &&);
	~Snapshot();

	class Iterator {
	public:
		Iterator(Iterator &&) = default;
		Iterator(const Iterator &it) noexcept : sn_(it.sn_), idx_(it.idx_) {}
		Iterator(const Snapshot *sn, size_t idx) : sn_(sn), idx_(idx) {}
		Iterator &operator=(const Iterator &it) noexcept {
			sn_ = it.sn_;
			idx_ = it.idx_;
			return *this;
		}
		Iterator &operator=(Iterator &&it) = default;
		SnapshotChunk Chunk() const;
		Iterator &operator++() noexcept;
		Iterator &operator+(size_t delta) noexcept;
		bool operator!=(const Iterator &other) const noexcept { return idx_ != other.idx_; }
		bool operator==(const Iterator &other) const noexcept { return idx_ == other.idx_; }
		Iterator &operator*() { return *this; }

	private:
		const Snapshot *sn_;
		size_t idx_;
		mutable WrSerializer ser_;
	};
	Iterator begin() const { return Iterator{this, 0}; }
	Iterator end() const { return Iterator{this, rawData_.Size() + walData_.Size()}; }
	size_t Size() const noexcept { return rawData_.Size() + walData_.Size(); }
	size_t RawDataSize() const noexcept { return rawData_.Size(); }
	bool HasRawData() const noexcept { return rawData_.Size(); }
	uint64_t ExpectedDatahash() const noexcept { return expectedDatahash_; }
	lsn_t LastLSN() const noexcept { return lastLsn_; }
	std::string Dump();

private:
	struct Chunk {
		bool txChunk = false;
		std::vector<ItemRef> items;
	};

	class ItemsContainer {
	public:
		void AddItem(ItemRef &&item);
		size_t Size() const noexcept { return data_.size(); }
		size_t ItemsCount() const noexcept { return itemsCount_; }
		const std::vector<Chunk> &Data() const noexcept { return data_; }
		void LockItems(PayloadType pt, bool lock);

	private:
		void lockItem(PayloadType pt, ItemRef &itemref, bool lock);

		std::vector<Chunk> data_;
		size_t itemsCount_ = 0;
	};

	void addRawData(QueryResults &&);
	void addWalData(QueryResults &&);
	void appendQr(ItemsContainer &container, QueryResults &&qr);
	void lockItems(bool lock);

	PayloadType pt_;
	TagsMatcher tm_;
	ItemsContainer rawData_;
	ItemsContainer walData_;
	uint64_t expectedDatahash_ = 0;
	lsn_t lastLsn_;
	friend class Iterator;
};

}  // namespace reindexer
