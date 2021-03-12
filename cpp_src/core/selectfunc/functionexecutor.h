#pragma once

#include "cluster/updaterecord.h"
#include "core/keyvalue/variant.h"

namespace reindexer {

class NamespaceImpl;
struct SelectFuncStruct;

class FunctionExecutor {
public:
	explicit FunctionExecutor(NamespaceImpl& ns, h_vector<cluster::UpdateRecord, 1>& replUpdates);
	Variant Execute(SelectFuncStruct& funcData);

private:
	NamespaceImpl& ns_;
	h_vector<cluster::UpdateRecord, 1>& replUpdates_;
};

}  // namespace reindexer
