#pragma once

#include <atomic>
#include <condition_variable>
#include <thread>
#include <vector>
#include "args.h"
#include "coroutine/channel.h"
#include "coroutine/waitgroup.h"
#include "cproto.h"
#include "estl/atomic_unique_ptr.h"
#include "estl/h_vector.h"
#include "net/manualconnection.h"
#include "tools/lsn.h"
#include "urlparser/urlparser.h"

namespace reindexer {

struct IRdxCancelContext;

namespace net {
namespace cproto {

using std::vector;
using std::chrono::seconds;
using std::chrono::milliseconds;

class CoroRPCAnswer {
public:
	Error Status() const { return status_; }
	Args GetArgs(int minArgs = 0) const {
		cproto::Args ret;
		Serializer ser(data_.data(), data_.size());
		ret.Unpack(ser);
		if (int(ret.size()) < minArgs) {
			throw Error(errParams, "Server returned %d args, but expected %d", int(ret.size()), minArgs);
		}

		return ret;
	}
	CoroRPCAnswer() = default;
	CoroRPCAnswer(const Error &error) : status_(error) {}
	CoroRPCAnswer(const CoroRPCAnswer &other) = delete;
	CoroRPCAnswer(CoroRPCAnswer &&other) = default;
	CoroRPCAnswer &operator=(CoroRPCAnswer &&other) = default;
	CoroRPCAnswer &operator=(const CoroRPCAnswer &other) = delete;

	void EnsureHold(chunk &&ch) {
		ch.append(string_view(reinterpret_cast<char *>(data_.data()), data_.size()));
		storage_ = std::move(ch);
		data_ = {storage_.data(), storage_.size()};
	}

protected:
	Error status_;
	span<uint8_t> data_;
	chunk storage_;
	friend class CoroClientConnection;
};

struct CommandParams {
	CommandParams(CmdCode c, milliseconds n, milliseconds e, lsn_t l, int sId, const IRdxCancelContext *ctx)
		: cmd(c), netTimeout(n), execTimeout(e), lsn(l), serverId(sId), cancelCtx(ctx) {}
	CmdCode cmd;
	milliseconds netTimeout;
	milliseconds execTimeout;
	lsn_t lsn;
	int serverId;
	const IRdxCancelContext *cancelCtx;
};

class CoroClientConnection {
public:
	using UpdatesHandlerT = std::function<void(const CoroRPCAnswer &ans)>;
	using FatalErrorHandlerT = std::function<void(Error err)>;
	using TimePointT = std::chrono::high_resolution_clock::time_point;

	struct Options {
		Options()
			: loginTimeout(0),
			  keepAliveTimeout(0),
			  createDB(false),
			  hasExpectedClusterID(false),
			  expectedClusterID(-1),
			  reconnectAttempts(),
			  enableCompression(false) {}
		Options(milliseconds _loginTimeout, milliseconds _keepAliveTimeout, bool _createDB, bool _hasExpectedClusterID,
				int _expectedClusterID, int _reconnectAttempts, bool _enableCompression, std::string _appName)
			: loginTimeout(_loginTimeout),
			  keepAliveTimeout(_keepAliveTimeout),
			  createDB(_createDB),
			  hasExpectedClusterID(_hasExpectedClusterID),
			  expectedClusterID(_expectedClusterID),
			  reconnectAttempts(_reconnectAttempts),
			  enableCompression(_enableCompression),
			  appName(std::move(_appName)) {}

		milliseconds loginTimeout;
		milliseconds keepAliveTimeout;
		bool createDB;
		bool hasExpectedClusterID;
		int expectedClusterID;
		int reconnectAttempts;
		bool enableCompression;
		std::string appName;
	};
	struct ConnectData {
		httpparser::UrlParser uri;
		Options opts;
	};

	CoroClientConnection();
	~CoroClientConnection();

	void Start(ev::dynamic_loop &loop, ConnectData connectData);
	void Stop();
	bool IsRunning() const noexcept { return isRunning_; }
	Error Status(milliseconds netTimeout, milliseconds execTimeout, const IRdxCancelContext *ctx);
	TimePointT Now() const noexcept { return now_; }
	void SetUpdatesHandler(UpdatesHandlerT handler) noexcept { updatesHandler_ = std::move(handler); }
	void SetFatalErrorHandler(FatalErrorHandlerT handler) noexcept { fatalErrorHandler_ = std::move(handler); }

	template <typename... Argss>
	CoroRPCAnswer Call(const CommandParams &opts, const Argss &...argss) {
		Args args;
		args.reserve(sizeof...(argss));
		return call(opts, args, argss...);
	}

private:
	struct RPCData {
		RPCData() : seq(0), used(false), deadline(TimePointT()), cancelCtx(nullptr), rspCh(1) {}
		uint32_t seq;
		bool used;
		TimePointT deadline;
		const reindexer::IRdxCancelContext *cancelCtx;
		coroutine::channel<CoroRPCAnswer> rspCh;
	};

	struct MarkedChunk {
		uint32_t seq;
		chunk data;
	};

	template <typename... Argss>
	inline CoroRPCAnswer call(const CommandParams &opts, Args &args, const string_view &val, const Argss &...argss) {
		args.push_back(Variant(p_string(&val)));
		return call(opts, args, argss...);
	}
	template <typename... Argss>
	inline CoroRPCAnswer call(const CommandParams &opts, Args &args, const string &val, const Argss &...argss) {
		args.push_back(Variant(p_string(&val)));
		return call(opts, args, argss...);
	}
	template <typename T, typename... Argss>
	inline CoroRPCAnswer call(const CommandParams &opts, Args &args, const T &val, const Argss &...argss) {
		args.push_back(Variant(val));
		return call(opts, args, argss...);
	}

	CoroRPCAnswer call(const CommandParams &opts, const Args &args);

	MarkedChunk packRPC(CmdCode cmd, uint32_t seq, const Args &args, const Args &ctxArgs);
	void appendChunck(std::vector<char> &buf, chunk &&ch);
	Error login(std::vector<char> &buf);
	void closeConn(Error err) noexcept;
	void handleFatalError(Error err) noexcept;
	chunk getChunk() noexcept;
	void recycleChunk(chunk &&) noexcept;

	void writerRoutine();
	void readerRoutine();
	void deadlineRoutine();
	void pingerRoutine();
	void updatesRoutine();

	TimePointT now_;
	bool terminate_ = false;
	bool isRunning_ = false;
	ev::dynamic_loop *loop_ = nullptr;

	// seq -> rpc data
	vector<RPCData> rpcCalls_;

	bool enableSnappy_ = false;
	bool enableCompression_ = false;
	std::vector<chunk> recycledChuncks_;
	coroutine::channel<MarkedChunk> wrCh_;
	coroutine::channel<uint32_t> seqNums_;
	ConnectData connectData_;
	UpdatesHandlerT updatesHandler_;
	FatalErrorHandlerT fatalErrorHandler_;
	coroutine::channel<CoroRPCAnswer> updatesCh_;
	coroutine::wait_group wg_;
	coroutine::wait_group readWg_;
	bool loggedIn_ = false;
	Error lastError_ = errOK;
	coroutine::channel<bool> errSyncCh_;
	manual_connection conn_;
};

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
