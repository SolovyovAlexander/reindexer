#pragma once

#include <chrono>
#include <string>
#include "connectopts.h"

namespace reindexer {
namespace client {

using std::chrono::milliseconds;

struct CoroReindexerConfig {
	CoroReindexerConfig(int _FetchAmount = 10000, int _ReconnectAttempts = 0, milliseconds _ConnectTimeout = milliseconds(0),
						milliseconds _RequestTimeout = milliseconds(0), bool _EnableCompression = false,
						std::string _appName = "CPP-client", unsigned int _syncRxCoroCount = 10)
		: FetchAmount(_FetchAmount),
		  ReconnectAttempts(_ReconnectAttempts),
		  ConnectTimeout(_ConnectTimeout),
		  RequestTimeout(_RequestTimeout),
		  EnableCompression(_EnableCompression),
		  AppName(std::move(_appName)),
		  syncRxCoroCount(_syncRxCoroCount) {}

	int FetchAmount;
	int ReconnectAttempts;
	milliseconds ConnectTimeout;
	milliseconds RequestTimeout;
	bool EnableCompression;
	std::string AppName;
	unsigned int syncRxCoroCount;
};

}  // namespace client
}  // namespace reindexer
