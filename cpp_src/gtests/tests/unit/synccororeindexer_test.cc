#include "client/synccororeindexer.h"
#include "gtest/gtest.h"
#include "gtests/tests/fixtures/servercontrol.h"
#include "net/ev/ev.h"
#include "tools/fsops.h"

const int kmaxIndex = 100000;
using namespace reindexer;

TEST(SyncCoroRx, BaseTest) {
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/TestSyncCoroRx");
	reindexer::fs::RmDirAll(kTestDbPath);
	ServerControl server;
	const string_view nsName = "ns";
	server.InitServer(0, 8999, 9888, 10999, kTestDbPath, "db", true);
	ReplicationConfigTest config("master");
	server.Get()->MakeMaster(config);
	reindexer::client::SyncCoroReindexer client;
	Error err = client.Connect("cproto://127.0.0.1:8999/db");
	ASSERT_TRUE(err.ok()) << err.what();
	err = client.OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client.AddIndex(nsName, indDef);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
	err = client.AddIndex(nsName, indDef2);
	ASSERT_TRUE(err.ok()) << err.what();

	const int insRows = 200;
	const string strValue = "aaaaaaaaaaaaaaa";
	for (unsigned i = 0; i < insRows; i++) {
		reindexer::client::Item item = client.NewItem(nsName);
		if (item.Status().ok()) {
			std::string json = R"#({"id":)#" + std::to_string(i) + R"#(, "val":)#" + "\"" + strValue + "\"" + R"#(})#";
			err = item.FromJSON(json);
			ASSERT_TRUE(err.ok()) << err.what();
			err = client.Upsert(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
		} else {
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}
	reindexer::client::SyncCoroQueryResults qResults(client, 3);
	err = client.Select(std::string("select * from ") + std::string(nsName) + " order by id", qResults);

	unsigned int indx = 0;
	for (auto it = qResults.begin(); it != qResults.end(); ++it, indx++) {
		reindexer::WrSerializer wrser;
		reindexer::Error err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		try {
			gason::JsonParser parser;
			gason::JsonNode json = parser.Parse(wrser.Slice());
			if (json["id"].As<unsigned int>(-1) != indx || json["val"].As<string_view>() != strValue) {
				ASSERT_TRUE(false) << "item value not correct";
			}

		} catch (const Error&) {
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}
}

TEST(SyncCoroRx, TestSyncCoroRx) {
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/TestSyncCoroRx");
	reindexer::fs::RmDirAll(kTestDbPath);
	ServerControl server;
	server.InitServer(0, 8999, 9888, 10999, kTestDbPath, "db", true);
	ReplicationConfigTest config("master");
	server.Get()->MakeMaster(config);
	reindexer::client::SyncCoroReindexer client;
	Error err = client.Connect("cproto://127.0.0.1:8999/db");
	ASSERT_TRUE(err.ok()) << err.what();
	err = client.OpenNamespace("ns_test");
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client.AddIndex("ns_test", indDef);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
	err = client.AddIndex("ns_test", indDef2);
	ASSERT_TRUE(err.ok()) << err.what();

	std::chrono::system_clock::time_point t1 = std::chrono::system_clock::now();
	for (unsigned i = 0; i < kmaxIndex; i++) {
		reindexer::client::Item item = client.NewItem("ns_test");
		if (item.Status().ok()) {
			std::string json = R"#({"id":)#" + std::to_string(i) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
			err = item.FromJSON(json);
			ASSERT_TRUE(err.ok()) << err.what();
			err = client.Upsert("ns_test", item);
			ASSERT_TRUE(err.ok()) << err.what();
		} else {
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}
	std::chrono::system_clock::time_point t2 = std::chrono::system_clock::now();
	int dt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
	std::cout << "dt_ms = " << dt_ms << std::endl;

	reindexer::client::SyncCoroQueryResults qResults(client, 3);
	client.Select("select * from ns_test", qResults);

	for (auto i = qResults.begin(); i != qResults.end(); ++i) {
		reindexer::WrSerializer wrser;
		reindexer::Error err = i.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

TEST(SyncCoroRx, TestSyncCoroRxNThread) {
	reindexer::fs::RmDirAll("/tmp/TestCoroRxNThread");
	ServerControl server;
	server.InitServer(0, 8999, 9888, 10999, "/tmp/TestSyncCoroRxNThread", "db", true);
	ReplicationConfigTest config("master");
	server.Get()->MakeMaster(config);
	reindexer::client::SyncCoroReindexer client;
	client.Connect("cproto://127.0.0.1:8999/db");
	client.OpenNamespace("ns_test");
	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	client.AddIndex("ns_test", indDef);

	reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
	client.AddIndex("ns_test", indDef2);

	std::atomic<int> counter(kmaxIndex);
	auto insertThreadFun = [&client, &counter]() {
		while (true) {
			int c = counter.fetch_add(1);
			if (c < kmaxIndex * 2) {
				reindexer::client::Item item = client.NewItem("ns_test");
				std::string json = R"#({"id":)#" + std::to_string(c) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
				reindexer::Error err = item.FromJSON(json);
				ASSERT_TRUE(err.ok()) << err.what();
				client.Upsert("ns_test", item);
			} else {
				break;
			}
		}
	};

	std::chrono::system_clock::time_point t1 = std::chrono::system_clock::now();
	std::vector<std::thread> pullThread;
	for (int i = 0; i < 10; i++) {
		pullThread.emplace_back(std::thread(insertThreadFun));
	}
	for (int i = 0; i < 10; i++) {
		pullThread[i].join();
	}
	std::chrono::system_clock::time_point t2 = std::chrono::system_clock::now();
	int dt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
	std::cout << "dt_ms = " << dt_ms << std::endl;
}

TEST(SyncCoroRx, TestCoroRxNCoroutine) {
	reindexer::fs::RmDirAll("/tmp/TestCoroRx");
	ServerControl server;
	server.InitServer(0, 8999, 9888, 10999, "/tmp/TestCoroRx", "db", true);
	ReplicationConfigTest config("master");
	server.Get()->MakeMaster(config);

	std::chrono::system_clock::time_point t1 = std::chrono::system_clock::now();

	reindexer::net::ev::dynamic_loop loop;
	auto insert = [&loop]() {
		reindexer::client::CoroReindexer rx;
		rx.Connect("cproto://127.0.0.1:8999/db", loop);
		rx.OpenNamespace("ns_c");
		reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
		rx.AddIndex("ns_c", indDef);
		reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
		rx.AddIndex("ns_c", indDef2);
		reindexer::coroutine::wait_group wg;

		auto insblok = [&rx, &wg](int from, int count) {
			reindexer::coroutine::wait_group_guard wgg(wg);
			for (int i = from; i < from + count; i++) {
				reindexer::client::Item item = rx.NewItem("ns_c");
				std::string json = R"#({"id":)#" + std::to_string(i) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
				reindexer::Error err = item.FromJSON(json);
				ASSERT_TRUE(err.ok()) << err.what();
				rx.Upsert("ns_c", item);
			}
		};

		const unsigned int kcoroCount = 10;
		unsigned int n = kmaxIndex / kcoroCount;
		wg.add(kcoroCount);
		for (unsigned int k = 0; k < kcoroCount; k++) {
			loop.spawn(std::bind(insblok, n * k, n));
		}
		wg.wait();
	};

	loop.spawn(insert);
	loop.run();
	std::chrono::system_clock::time_point t2 = std::chrono::system_clock::now();
	int dt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
	std::cout << "dt_ms = " << dt_ms << std::endl;
}

TEST(SyncCoroRx, RxClient) {
	reindexer::fs::RmDirAll("/tmp/RxClient");
	ServerControl server;
	server.InitServer(0, 8999, 9888, 10999, "/tmp/RxClient", "db", true);
	ReplicationConfigTest config("master");
	server.Get()->MakeMaster(config);
	std::string nsName = "standart";
	auto control = server.Get();
	auto opt = StorageOpts().Enabled(true);
	reindexer::Error err = control->api.reindexer->OpenNamespace(nsName, opt);
	control->api.DefineNamespaceDataset(nsName, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});

	std::chrono::system_clock::time_point t1 = std::chrono::system_clock::now();
	for (unsigned i = 0; i < kmaxIndex; i++) {
		reindexer::client::Item item = control->api.NewItem(nsName);
		std::string json = R"#({"id":)#" + std::to_string(i) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
		reindexer::Error err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();
		control->api.Upsert(nsName, item);
	}
	std::chrono::system_clock::time_point t2 = std::chrono::system_clock::now();
	int dt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
	std::cout << "dt_ms = " << dt_ms << std::endl;
}

TEST(SyncCoroRx, RxClientNThread) {
	reindexer::fs::RmDirAll("/tmp/RxClientNThread");
	ServerControl server;
	server.InitServer(0, 8999, 9888, 10999, "/tmp/RxClientNThread", "db", true);
	ReplicationConfigTest config("master");
	server.Get()->MakeMaster(config);
	auto client = server.Get()->api.reindexer;
	// client.Connect("cproto://127.0.0.1:8999/db");
	Error err = client->OpenNamespace("ns_test");
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client->AddIndex("ns_test", indDef);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
	err = client->AddIndex("ns_test", indDef2);
	ASSERT_TRUE(err.ok()) << err.what();

	std::atomic<int> counter(kmaxIndex);
	auto insertThreadFun = [&client, &counter]() {
		while (true) {
			int c = counter.fetch_add(1);
			if (c < kmaxIndex * 2) {
				reindexer::client::Item item = client->NewItem("ns_test");
				std::string json = R"#({"id":)#" + std::to_string(c) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
				reindexer::Error err = item.FromJSON(json);
				ASSERT_TRUE(err.ok()) << err.what();
				client->Upsert("ns_test", item);
			} else {
				break;
			}
		}
	};

	std::chrono::system_clock::time_point t1 = std::chrono::system_clock::now();
	std::vector<std::thread> pullThread;
	for (int i = 0; i < 10; i++) {
		pullThread.emplace_back(std::thread(insertThreadFun));
	}
	for (int i = 0; i < 10; i++) {
		pullThread[i].join();
	}
	std::chrono::system_clock::time_point t2 = std::chrono::system_clock::now();
	int dt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
	std::cout << "dt_ms = " << dt_ms << std::endl;
}
