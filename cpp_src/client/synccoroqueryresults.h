#pragma once

#include <chrono>
#include "client/item.h"
#include "client/resultserializer.h"
#include "client/coroqueryresults.h"


namespace reindexer {
class TagsMatcher;
namespace net {
namespace cproto {
class CoroClientConnection;
}
}  // namespace net

namespace client {

using std::chrono::seconds;
using std::chrono::milliseconds;

class Namespace;
using NSArray = h_vector<Namespace*, 1>;
class SyncCoroReindexer;

class SyncCoroQueryResults {
public:
	SyncCoroQueryResults(SyncCoroReindexer& rx, int fetchFlags = 0);
	SyncCoroQueryResults(const SyncCoroQueryResults&) = delete;
	SyncCoroQueryResults(SyncCoroQueryResults&&) = default;
	SyncCoroQueryResults& operator=(const SyncCoroQueryResults&) = delete;
	SyncCoroQueryResults& operator=(SyncCoroQueryResults&& obj) = default;

	class Iterator {
	public:
		Error GetJSON(WrSerializer& wrser, bool withHdrLen = true);
		Error GetCJSON(WrSerializer& wrser, bool withHdrLen = true);
		Error GetMsgPack(WrSerializer& wrser, bool withHdrLen = true);
		Item GetItem();
		int64_t GetLSN();
		bool IsRaw();
		string_view GetRaw();
		Iterator& operator++();
		Error Status() const noexcept{ return qr_->results_.status_; }
		bool operator!=(const Iterator&other) const noexcept { return idx_ != other.idx_; }
		bool operator==(const Iterator&other) const noexcept { return idx_ == other.idx_; };
		Iterator& operator*() { return *this; }
		void readNext();
		void getJSONFromCJSON(string_view cjson, WrSerializer& wrser, bool withHdrLen = true);

		const SyncCoroQueryResults* qr_;
		int idx_, pos_, nextPos_;
		ResultSerializer::ItemParams itemParams_;
	};

	Iterator begin() const { return Iterator{this, 0, 0, 0, {}}; }
	Iterator end() const { return Iterator{this, results_.queryParams_.qcount, 0, 0, {}}; }

	size_t Count() const { return results_.queryParams_.qcount; }
	int TotalCount() const { return results_.queryParams_.totalcount; }
	bool HaveRank() const { return results_.queryParams_.flags & kResultsWithRank; }
	bool NeedOutputRank() const { return results_.queryParams_.flags & kResultsNeedOutputRank; }
	const string& GetExplainResults() const { return results_.queryParams_.explainResults; }
	const vector<AggregationResult>& GetAggregationResults() const { return results_.queryParams_.aggResults; }
	Error Status() { return results_.status_; }
	h_vector<string_view, 1> GetNamespaces() const;
	bool IsCacheEnabled() const { return results_.queryParams_.flags & kResultsWithItemID; }

	TagsMatcher getTagsMatcher(int nsid) const;

private:
	friend class SyncCoroReindexer;
	friend class SyncCoroReindexerImpl;
	void Bind(string_view rawResult, int queryID);
	void fetchNextResults();

	CoroQueryResults results_;
	SyncCoroReindexer& rx_;
};
}  // namespace client
}  // namespace reindexer
