

#include "client/synccoroqueryresults.h"
#include "client/namespace.h"
#include "client/synccororeindexer.h"
#include "core/cjson/baseencoder.h"
#include "core/keyvalue/p_string.h"
#include "net/cproto/coroclientconnection.h"
#include "tools/logger.h"

namespace reindexer {
namespace client {

using namespace reindexer::net;

SyncCoroQueryResults::SyncCoroQueryResults(SyncCoroReindexer &rx, int fetchFlags) : results_(fetchFlags), rx_(rx) {}

void SyncCoroQueryResults::Bind(string_view rawResult, int queryID) { results_.Bind(rawResult, queryID); }

void SyncCoroQueryResults::fetchNextResults() {
	int flags = results_.fetchFlags_ ? (results_.fetchFlags_ & ~kResultsWithPayloadTypes) : kResultsCJson;
	rx_.impl_->fetchResults(flags, *this);
}

h_vector<string_view, 1> SyncCoroQueryResults::GetNamespaces() const { return results_.GetNamespaces(); }

TagsMatcher SyncCoroQueryResults::getTagsMatcher(int nsid) const { return results_.nsArray_[nsid]->tagsMatcher_; }

class AdditionalRank : public IAdditionalDatasource<JsonBuilder> {
public:
	AdditionalRank(double r) : rank_(r) {}
	void PutAdditionalFields(JsonBuilder &builder) const final { builder.Put("rank()", rank_); }
	IEncoderDatasourceWithJoins *GetJoinsDatasource() final { return nullptr; }

private:
	double rank_;
};

void SyncCoroQueryResults::Iterator::getJSONFromCJSON(string_view cjson, WrSerializer &wrser, bool withHdrLen) {
	auto tm = qr_->getTagsMatcher(itemParams_.nsid);
	JsonEncoder enc(&tm);
	JsonBuilder builder(wrser, ObjType::TypePlain);
	if (qr_->NeedOutputRank()) {
		AdditionalRank additionalRank(itemParams_.proc);
		if (withHdrLen) {
			auto slicePosSaver = wrser.StartSlice();
			enc.Encode(cjson, builder, &additionalRank);
		} else {
			enc.Encode(cjson, builder, &additionalRank);
		}
	} else {
		if (withHdrLen) {
			auto slicePosSaver = wrser.StartSlice();
			enc.Encode(cjson, builder, nullptr);
		} else {
			enc.Encode(cjson, builder, nullptr);
		}
	}
}

Error SyncCoroQueryResults::Iterator::GetMsgPack(WrSerializer &wrser, bool withHdrLen) {
	readNext();
	int type = qr_->results_.queryParams_.flags & kResultsFormatMask;
	if (type == kResultsMsgPack) {
		if (withHdrLen) {
			wrser.PutSlice(itemParams_.data);
		} else {
			wrser.Write(itemParams_.data);
		}
	} else {
		return Error(errParseBin, "Impossible to get data in MsgPack because of a different format: %d", type);
	}
	return errOK;
}

Error SyncCoroQueryResults::Iterator::GetJSON(WrSerializer &wrser, bool withHdrLen) {
	readNext();
	try {
		switch (qr_->results_.queryParams_.flags & kResultsFormatMask) {
			case kResultsCJson: {
				getJSONFromCJSON(itemParams_.data, wrser, withHdrLen);
				break;
			}
			case kResultsJson:
				if (withHdrLen) {
					wrser.PutSlice(itemParams_.data);
				} else {
					wrser.Write(itemParams_.data);
				}
				break;
			default:
				return Error(errParseBin, "Server returned data in unknown format %d",
							 qr_->results_.queryParams_.flags & kResultsFormatMask);
		}
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error SyncCoroQueryResults::Iterator::GetCJSON(WrSerializer &wrser, bool withHdrLen) {
	readNext();
	try {
		switch (qr_->results_.queryParams_.flags & kResultsFormatMask) {
			case kResultsCJson:
				if (withHdrLen) {
					wrser.PutSlice(itemParams_.data);
				} else {
					wrser.Write(itemParams_.data);
				}
				break;
			case kResultsMsgPack:
				return Error(errParseBin, "Server returned data in msgpack format, can't process");
			case kResultsJson:
				return Error(errParseBin, "Server returned data in json format, can't process");
			default:
				return Error(errParseBin, "Server returned data in unknown format %d",
							 qr_->results_.queryParams_.flags & kResultsFormatMask);
		}
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Item SyncCoroQueryResults::Iterator::GetItem() {
	readNext();
	try {
		Error err;
		Item item = qr_->results_.nsArray_[itemParams_.nsid]->NewItem();
		switch (qr_->results_.queryParams_.flags & kResultsFormatMask) {
			case kResultsMsgPack: {
				size_t offset = 0;
				err = item.FromMsgPack(itemParams_.data, offset);
				break;
			}
			case kResultsCJson: {
				err = item.FromCJSON(itemParams_.data);
				item.setID(itemParams_.id);
				break;
			}
			case kResultsJson: {
				char *endp = nullptr;
				err = item.FromJSON(itemParams_.data, &endp);
				break;
			}
			default:
				return Item();
		}
		if (err.ok()) {
			return item;
		}
	} catch (const Error &) {
	}
	return Item();
}

int64_t SyncCoroQueryResults::Iterator::GetLSN() {
	readNext();
	return itemParams_.lsn;
}

bool SyncCoroQueryResults::Iterator::IsRaw() {
	readNext();
	return itemParams_.raw;
}

string_view SyncCoroQueryResults::Iterator::GetRaw() {
	readNext();
	assert(itemParams_.raw);
	return itemParams_.data;
}

void SyncCoroQueryResults::Iterator::readNext() {
	if (nextPos_ != 0) return;

	string_view rawResult(qr_->results_.rawResult_.data(), qr_->results_.rawResult_.size());

	ResultSerializer ser(rawResult.substr(pos_));

	try {
		itemParams_ = ser.GetItemParams(qr_->results_.queryParams_.flags);
		if (qr_->results_.queryParams_.flags & kResultsWithJoined) {
			int joinedCnt = ser.GetVarUint();
			(void)joinedCnt;
		}
		nextPos_ = pos_ + ser.Pos();
	} catch (const Error &err) {
		const_cast<SyncCoroQueryResults *>(qr_)->results_.status_ = err;
	}
}

SyncCoroQueryResults::Iterator &SyncCoroQueryResults::Iterator::operator++() {
	try {
		readNext();
		idx_++;
		pos_ = nextPos_;
		nextPos_ = 0;

		if (idx_ != qr_->results_.queryParams_.qcount && idx_ == qr_->results_.queryParams_.count + qr_->results_.fetchOffset_) {
			const_cast<SyncCoroQueryResults *>(qr_)->fetchNextResults();
			pos_ = 0;
		}
	} catch (const Error &err) {
		const_cast<SyncCoroQueryResults *>(qr_)->results_.status_ = err;
	}

	return *this;
}

}  // namespace client
}  // namespace reindexer
