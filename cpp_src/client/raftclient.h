#pragma once

#include "client/cororeindexerconfig.h"
#include "client/cororpcclient.h"
#include "client/internalrdxcontext.h"
#include "net/ev/ev.h"

#include <chrono>

namespace reindexer {

namespace client {

/// The main Reindexer interface. Holds database object<br>
/// *Thread safety*: None of the methods are threadsafe <br>
/// CoroReindexer should be used via multiple coroutins in single thread, while ev-loop is running.
/// *Resources lifetime*: All resources aquired from Reindexer, e.g Item or QueryResults are uses Copy-On-Write
/// semantics, and have independent lifetime<br>
class RaftClient {
public:
	using NodeData = cluster::NodeData;
	using RaftInfo = cluster::RaftInfo;
	/// Completion routine
	typedef std::function<void(const Error &err)> Completion;

	/// Create Reindexer database object
	RaftClient(const CoroReindexerConfig & = CoroReindexerConfig());
	/// Destrory Reindexer database object
	~RaftClient();
	RaftClient(const RaftClient &) = delete;
	RaftClient(RaftClient &&) noexcept;
	RaftClient &operator=(const RaftClient &) = delete;
	RaftClient &operator=(RaftClient &&) noexcept;

	/// Connect - connect to reindexer server
	/// @param dsn - uri of server and database, like: `cproto://user@password:127.0.0.1:6534/dbname`
	/// @param loop - event loop for connections and coroutines handling
	/// @param opts - Connect options. May contaion any of <br>
	Error Connect(const string &dsn, net::ev::dynamic_loop &loop, const client::ConnectOpts &opts = client::ConnectOpts());
	/// Stop - shutdown connector
	Error Stop();
	Error SuggestLeader(const NodeData &suggestion, NodeData &response);
	// TODO: This may not be required. Follower can get this info from context, however in this case we have to implement additional
	// layer between Reindexer and ReindexerImpl
	Error LeadersPing(const NodeData &);
	Error GetRaftInfo(RaftInfo &info);
	//	Error GetClusterConfig();
	//	Error SetClusterConfig();
	/// Get curret connection status
	Error Status();

	/// Add cancelable context
	/// @param cancelCtx - context pointer
	RaftClient WithContext(const IRdxCancelContext *cancelCtx) { return RaftClient(impl_, ctx_.WithCancelContext(cancelCtx)); }

	/// Add execution timeout to the next query
	/// @param timeout - Optional server-side execution timeout for each subquery
	RaftClient WithTimeout(milliseconds timeout) { return RaftClient(impl_, ctx_.WithTimeout(timeout)); }

private:
	RaftClient(CoroRPCClient *impl, InternalRdxContext &&ctx) : impl_(impl), owner_(false), ctx_(std::move(ctx)) {}
	CoroRPCClient *impl_;
	bool owner_;
	InternalRdxContext ctx_;
};

}  // namespace client
}  // namespace reindexer
