#include "client/cororpcclient.h"
#include "client/itemimpl.h"
#include "client/synccororeindexer.h"
#include "client/syncorotransaction.h"
#include "core/keyvalue/p_string.h"
#include "net/cproto/coroclientconnection.h"

namespace reindexer {
namespace client {

Error SyncCoroTransaction::Insert(Item&& item, lsn_t lsn) { return rx_.addTxItem(*this, std::move(item), ModeInsert, lsn); }
Error SyncCoroTransaction::Update(Item&& item, lsn_t lsn) { return rx_.addTxItem(*this, std::move(item), ModeUpdate, lsn); }
Error SyncCoroTransaction::Upsert(Item&& item, lsn_t lsn) { return rx_.addTxItem(*this, std::move(item), ModeUpsert, lsn); }
Error SyncCoroTransaction::Delete(Item&& item, lsn_t lsn) { return rx_.addTxItem(*this, std::move(item), ModeDelete, lsn); }
Error SyncCoroTransaction::Modify(Item&& item, ItemModifyMode mode, lsn_t lsn) { return rx_.addTxItem(*this, std::move(item), mode, lsn); }
Error SyncCoroTransaction::Modify(Query&& query, lsn_t lsn) { return rx_.modifyTx(*this, std::move(query), lsn); }
Item SyncCoroTransaction::NewItem() { return rx_.execNewItemTx(tr_); }

}  // namespace client
}  // namespace reindexer
