#include <unordered_map>
#include <unordered_set>
#include "client/raftclient.h"
#include "clusterization_api.h"

TEST_F(ClusterizationApi, DISABLED_t1) {
	ServerControl master;
	master.InitServer(0, 7770, 7880, 7990, std::string("/tmp") + "/master", "db", true, 1024 * 5);

	master.Get()->MakeMaster(ReplicationConfigTest("master", "appMaster"));
	std::string nsName = "nsName1";
	Error err = master.Get()->api.reindexer->OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();
	master.Get()->api.DefineNamespaceDataset(nsName, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});

	for (unsigned int i = 0; i < 10; i++) {
		reindexer::client::Item item = master.Get()->api.NewItem(nsName);
		auto err = item.FromJSON("{\"id\":" + std::to_string(i) + "}");
		ASSERT_TRUE(err.ok()) << err.what();
		master.Get()->api.Upsert(nsName, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	//	std::this_thread::sleep_for(std::chrono::seconds(1000));

	{
		Query qSel;
		qSel.FromSQL("select * from nsName1 where id=5");
		client::QueryResults selResult;
		master.Get()->api.reindexer->Select(qSel, selResult);
		for (auto it = selResult.begin(); it != selResult.end(); ++it) {
			auto item = it.GetItem();
			master.Get()->api.reindexer->Delete("nsName1", item);
		}
	}
	{
		/*		Query qDel;
				qDel.FromSQL("delete from nsName1 where id<5");
				client::QueryResults delResult;
				err = master.Get()->api.reindexer->Delete(qDel, delResult);
				ASSERT_TRUE(err.ok()) << err.what();
				std::cout << "deleted items" << std::endl;
				for (auto it = delResult.begin(); it != delResult.end(); ++it) {
					string_view itemJson = it.GetItem().GetJSON();
					std::cout << "item = " << itemJson << std::endl;
				}
		*/
	} {
		Query qSel;
		qSel.FromSQL("select * from nsName1");
		client::QueryResults selResult;
		master.Get()->api.reindexer->Select(qSel, selResult);
		std::cout << "selected items" << std::endl;
		for (auto it = selResult.begin(); it != selResult.end(); ++it) {
			auto item = it.GetItem();
			string_view itemJson = item.GetJSON();
			std::cout << itemJson << std::endl;
		}
	}
	std::cout << "test end" << std::endl;
	// std::this_thread::sleep_for(std::chrono::seconds(1000));
}

TEST_F(ClusterizationApi, ApiTestSelect) {
	const size_t kClusterSize = 4;
	net::ev::dynamic_loop loop;
	loop.spawn([&loop] {
		try {
			const std::string nsName = "ns";
			Cluster cluster(loop, 0, kClusterSize);
			auto leaderId = cluster.AwaitLeader(std::chrono::seconds(10));
			int nodeSlave = (leaderId + 1) % kClusterSize;
			reindexer::NamespaceDef nsdef(nsName);
			nsdef.AddIndex("id", "hash", "int", IndexOpts().PK());	//.AddIndex("name", "tree", "string", IndexOpts());

			Error err = cluster.GetServerControl(nodeSlave)->api.reindexer->AddNamespace(nsdef);

			auto item = cluster.GetServerControl(nodeSlave)->api.reindexer->NewItem(nsName);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			int pk = 11;
			std::string itemJson =
				"{"
				"\"id\":" +
				std::to_string(pk) +
				","
				"\"name\":\"string" +
				std::to_string(pk) +
				"\""
				"}";

			err = item.FromJSON(itemJson);
			ASSERT_TRUE(err.ok()) << err.what();
			err = cluster.GetServerControl(nodeSlave)->api.reindexer->Insert(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
			{
				reindexer::client::QueryResults qresSelectTmp;
				// reindexer::Query q1;
				// q1.FromSQL("select * from " + nsName);
				// err = cluster.GetServerControl(nodeSlave)->api.reindexer->Select(q1, qresSelectTmp);
				err = cluster.GetServerControl(nodeSlave)->api.reindexer->Select("select * from " + nsName, qresSelectTmp);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_TRUE(qresSelectTmp.Count() == 1) << "select count = " << qresSelectTmp.Count();
				auto it = qresSelectTmp.begin();
				WrSerializer wrser;
				err = it.GetJSON(wrser, false);
				ASSERT_TRUE(err.ok()) << err.what();
				std::cout << "+++++++++++++ !!! it=" << wrser.c_str() << std::endl;
			}

			cluster.StopClients();
		} catch (Error& e) {
			ASSERT_TRUE(false) << e.what();
		} catch (std::exception& e) {
			ASSERT_TRUE(false) << e.what();
		} catch (...) {
			ASSERT_TRUE(false) << "Unknown exception";
		}
	});

	loop.run();
}

TEST_F(ClusterizationApi, ApiTest) {
	//Работа через follower
	const size_t kClusterSize = 4;
	net::ev::dynamic_loop loop;
	loop.spawn([&loop] {
		try {
			const std::string nsName = "ns";
			Cluster cluster(loop, 0, kClusterSize);
			auto leaderId = cluster.AwaitLeader(std::chrono::seconds(10));
			int nodeSlave = (leaderId + 1) % kClusterSize;

			// reindexer::NamespaceDef nsdef(nsName);
			// nsdef.AddIndex("id", "hash", "int", IndexOpts().PK());	//.AddIndex("name", "tree", "string", IndexOpts());

			// Error err = cluster.GetServerControl(nodeSlave)->api.reindexer->AddNamespace(nsdef);
			Error err = cluster.GetServerControl(nodeSlave)->api.reindexer->OpenNamespace(nsName);
			ASSERT_TRUE(err.ok()) << err.what();
			cluster.GetServerControl(nodeSlave)->api.reindexer->AddIndex(nsName, {"id", "hash", "int", IndexOpts().PK()});

			auto sel = [&](int node, std::string& itemJson) {
				reindexer::Query q(nsName);
				reindexer::client::QueryResults qres;
				Error err = cluster.GetServerControl(node)->api.reindexer->Select(q, qres);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_TRUE(qres.Count() == 1);
				auto itsel = qres.begin().GetItem();
				ASSERT_TRUE(itsel.GetJSON() == itemJson) << itsel.GetJSON();
			};
			auto sel0 = [&](int node) {
				reindexer::Query q(nsName);
				reindexer::client::QueryResults qres;
				Error err = cluster.GetServerControl(node)->api.reindexer->Select(q, qres);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_TRUE(qres.Count() == 0);
			};

			int pk = 10;
			{
				auto item = cluster.GetServerControl(nodeSlave)->api.reindexer->NewItem(nsName);
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();

				std::string itemJson =
					"{"
					"\"id\":" +
					std::to_string(pk) +
					","
					"\"name\":\"string" +
					std::to_string(pk) +
					"\""
					"}";
				err = item.FromJSON(itemJson);
				ASSERT_TRUE(err.ok()) << err.what();
				err = cluster.GetServerControl(nodeSlave)->api.reindexer->Insert(nsName, item);
				ASSERT_TRUE(err.ok()) << err.what();
				sel(nodeSlave, itemJson);
				sel(leaderId, itemJson);

				std::string itemJsonUp =
					"{"
					"\"id\":" +
					std::to_string(pk) +
					","
					"\"name\":\"string_up" +
					std::to_string(pk) +
					"\""
					"}";
				err = item.FromJSON(itemJsonUp);
				err = cluster.GetServerControl(nodeSlave)->api.reindexer->Upsert(nsName, item);
				ASSERT_TRUE(err.ok()) << err.what();

				sel(nodeSlave, itemJsonUp);
				sel(leaderId, itemJsonUp);
			}

			{
				reindexer::Query q;
				q.FromSQL("select * from " + nsName + " where id=" + std::to_string(pk));
				reindexer::client::QueryResults qres;
				err = cluster.GetServerControl(nodeSlave)->api.reindexer->Select(q, qres);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(qres.Count(), 1);
				auto itsel = qres.begin().GetItem();
				std::string itemJson =
					"{"
					"\"id\":" +
					std::to_string(pk) +
					","
					"\"name\":\"string_update" +
					std::to_string(pk) +
					"\""
					"}";
				itsel.FromJSON(itemJson);
				cluster.GetServerControl(nodeSlave)->api.reindexer->Update(nsName, itsel);
				sel(nodeSlave, itemJson);
				sel(leaderId, itemJson);
			}

			{
				reindexer::Query q;
				q.FromSQL("select * from " + nsName + " where id=" + std::to_string(pk));
				reindexer::client::QueryResults qres;
				err = cluster.GetServerControl(nodeSlave)->api.reindexer->Select(q, qres);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(qres.Count(), 1);
				auto itsel = qres.begin().GetItem();
				cluster.GetServerControl(nodeSlave)->api.reindexer->Delete(nsName, itsel);
				sel0(nodeSlave);
				sel0(leaderId);
			}

			{
				for (int k = 0; k < 10; k++) {
					auto item = cluster.GetServerControl(nodeSlave)->api.reindexer->NewItem(nsName);
					ASSERT_TRUE(item.Status().ok()) << item.Status().what();
					std::string itemJson =
						"{"
						"\"id\":" +
						std::to_string(k) +
						","
						"\"name\":\"string" +
						std::to_string(k) +
						"\""
						"}";
					err = item.FromJSON(itemJson);
					ASSERT_TRUE(err.ok()) << err.what();
					err = cluster.GetServerControl(nodeSlave)->api.reindexer->Insert(nsName, item);
					ASSERT_TRUE(err.ok()) << err.what();
				}
				{
					reindexer::Query qUpdate;
					qUpdate.FromSQL("update " + nsName + " set name='up_name' where id>5");
					reindexer::client::QueryResults qres;  //не заполняется
					err = cluster.GetServerControl(nodeSlave)->api.reindexer->Update(qUpdate, qres);
					ASSERT_TRUE(err.ok()) << err.what();

					{
						reindexer::Query q;
						q.FromSQL("select name from " + nsName + " where id<=5");
						reindexer::client::QueryResults qresSelect;
						err = cluster.GetServerControl(nodeSlave)->api.reindexer->Select(q, qresSelect);
						ASSERT_TRUE(err.ok()) << err.what();
						ASSERT_TRUE(qresSelect.Count() == 6);
						int indx = 0;
						for (auto it = qresSelect.begin(); it != qresSelect.end(); ++it, indx++) {
							auto item = it.GetItem();
							auto json = item.GetJSON();
							std::string itemJson =
								"{"
								"\"name\":\"string" +
								std::to_string(indx) +
								"\""
								"}";
							ASSERT_TRUE(json == itemJson) << itemJson;
						}
					}
					{
						reindexer::Query q;
						q.FromSQL("select name from " + nsName + " where id>5");
						reindexer::client::QueryResults qresSelect;
						err = cluster.GetServerControl(nodeSlave)->api.reindexer->Select(q, qresSelect);
						ASSERT_TRUE(err.ok()) << err.what();
						ASSERT_TRUE(qresSelect.Count() == 4);
						int indx = 0;
						for (auto it = qresSelect.begin(); it != qresSelect.end(); ++it, indx++) {
							auto item = it.GetItem();
							auto json = item.GetJSON();
							std::string itemJson =
								"{"
								"\"name\":\"up_name\""
								"}";
							ASSERT_TRUE(json == itemJson) << json;
						}
					}
				}
				{
					reindexer::Query qdel;
					qdel.FromSQL("delete from " + nsName + " where id>0");
					reindexer::client::QueryResults qres;  //не заполняется
					err = cluster.GetServerControl(nodeSlave)->api.reindexer->Delete(qdel, qres);
					ASSERT_TRUE(err.ok()) << err.what();
					std::string itemJson =
						"{"
						"\"id\":" +
						std::to_string(0) +
						","
						"\"name\":\"string" +
						std::to_string(0) +
						"\""
						"}";
					sel(nodeSlave, itemJson);
					sel(leaderId, itemJson);
				}
			}
			{
				err = cluster.GetServerControl(nodeSlave)->api.reindexer->TruncateNamespace(nsName);
				ASSERT_TRUE(err.ok()) << err.what();
				sel0(nodeSlave);
				sel0(leaderId);
				auto item = cluster.GetServerControl(nodeSlave)->api.reindexer->NewItem(nsName);
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();
				std::string itemJson =
					"{"
					"\"id\":" +
					std::to_string(pk) +
					","
					"\"name\":\"string" +
					std::to_string(pk) +
					"\""
					"}";

				err = item.FromJSON(itemJson);
				ASSERT_TRUE(err.ok()) << err.what();
				err = cluster.GetServerControl(nodeSlave)->api.reindexer->Insert(nsName, item);
				ASSERT_TRUE(err.ok()) << err.what();
				{
					reindexer::client::QueryResults qresSelectTmp;
					err = cluster.GetServerControl(nodeSlave)->api.reindexer->Select("select * from " + nsName, qresSelectTmp);
					ASSERT_TRUE(err.ok()) << err.what();
					ASSERT_TRUE(qresSelectTmp.Count() == 1) << "select count = " << qresSelectTmp.Count();
				}
				reindexer::client::QueryResults qr;
				std::string q = "update " + nsName + " set name='up_name' where id=" + std::to_string(pk);
				err = cluster.GetServerControl(nodeSlave)->api.reindexer->Select(q, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				{
					reindexer::client::QueryResults qresSelect;
					err = cluster.GetServerControl(nodeSlave)->api.reindexer->Select("select name from " + nsName, qresSelect);
					ASSERT_TRUE(err.ok()) << err.what();
					ASSERT_TRUE(qresSelect.Count() == 1) << "select count = " << qresSelect.Count();
					WrSerializer wrser;
					err = qresSelect.begin().GetJSON(wrser, false);
					ASSERT_TRUE(err.ok()) << err.what();
					std::string json = wrser.c_str();
					std::string itemJsonUp =
						"{"
						"\"name\":\"up_name\""
						"}";
					ASSERT_TRUE(json == itemJsonUp) << json;
				}

				reindexer::client::QueryResults qresDel;
				err = cluster.GetServerControl(nodeSlave)->api.reindexer->Select("delete from " + nsName, qresDel);
				ASSERT_TRUE(err.ok()) << err.what();
				sel0(nodeSlave);
				sel0(leaderId);
			}

			cluster.StopClients();
		} catch (Error& e) {
			ASSERT_TRUE(false) << e.what();
		} catch (std::exception& e) {
			ASSERT_TRUE(false) << e.what();
		} catch (...) {
			ASSERT_TRUE(false) << "Unknown exception";
		}
	});

	loop.run();
}

TEST_F(ClusterizationApi, DeleteSelect) {
	const size_t kClusterSize = 4;
	net::ev::dynamic_loop loop;
	loop.spawn([&loop] {
		try {
			Cluster cluster(loop, 0, kClusterSize);
			auto leaderId = cluster.AwaitLeader(std::chrono::seconds(10));
			int nodeSlave = (leaderId + 1) % kClusterSize;
			cluster.InitNs(leaderId, "ns1");
			cluster.WaitSync("ns1");
			for (int i = 0; i < 10; i++) {
				int nodeNum = rand() % kClusterSize;
				cluster.AddRow(nodeNum, "ns1", i);
			}
			{
				client::QueryResults qr;
				cluster.GetServerControl(leaderId)->api.reindexer->Select(Query("ns1"), qr);
				ASSERT_EQ(qr.Count(), 10) << "must 10 records current " << qr.Count();
			}
			cluster.WaitSync("ns1");

			auto serverSlave = cluster.GetServerControl(nodeSlave);
			for (int k = 0; k < 10; k++) {
				{
					Query qDel;
					qDel.FromSQL("select * from ns1 where id<5");
					client::QueryResults delResult;
					Error err = serverSlave->api.reindexer->Select(qDel, delResult);
					ASSERT_EQ(delResult.Count(), 5) << "-------------------------- incorect count for delete";
					for (auto it = delResult.begin(); it != delResult.end(); ++it) {
						auto item = it.GetItem();
						Error err = serverSlave->api.reindexer->Delete("ns1", item);
						ASSERT_TRUE(err.ok()) << err.what();
					}
				}
				{
					Query qSel;
					qSel.FromSQL("select * from ns1 where id<5");
					client::QueryResults selResult;
					Error err = serverSlave->api.reindexer->Select(qSel, selResult);
					ASSERT_TRUE(selResult.Count() == 0) << "------------------------- incorrect count =" << selResult.Count();
				}
				{
					for (int i = 0; i < 5; i++) {
						cluster.AddRow(nodeSlave, "ns1", i);
					}
				}
			}
			Query qSel;
			qSel.FromSQL("select * from ns1 order by id");
			client::QueryResults selResult;
			serverSlave->api.reindexer->Select(qSel, selResult);
			std::cout << "selected items" << std::endl;
			for (auto it = selResult.begin(); it != selResult.end(); ++it) {
				std::cout << it.GetItem().GetJSON() << std::endl;
			}

			cluster.StopClients();
		} catch (Error& e) {
			ASSERT_TRUE(false) << e.what();
		} catch (std::exception& e) {
			ASSERT_TRUE(false) << e.what();
		} catch (...) {
			ASSERT_TRUE(false) << "Unknown exception";
		}
	});

	loop.run();
}

TEST_F(ClusterizationApi, DISABLED_SimpleRWTest) {
	const size_t kClusterSize = 4;
	net::ev::dynamic_loop loop;
	loop.spawn([&loop] {
		Cluster cluster(loop, 0, kClusterSize);
		auto leaderId = cluster.AwaitLeader(std::chrono::seconds(5), true);
		int oldLeaderId = -1;
		std::cout << "++++++++++++++leaderId = " << leaderId << std::endl;
		int nodeSlave = (leaderId + 1) % kClusterSize;
		cluster.InitNs(leaderId, "ns1");
		for (int i = 0; i < 1000; i++) {
			int nodeNum = rand() % kClusterSize;
			cluster.AddRow(nodeNum, "ns1", i);
		}
		cluster.WaitSync("ns1");

		auto serverSlave = cluster.GetServerControl(nodeSlave);
		Query qDel;
		qDel.FromSQL("delete from ns1 where id<500");
		client::QueryResults delResult;
		//		serverSlave->api.reindexer->Delete(qDel, delResult);
		std::cout << "deleted items" << std::endl;
		for (auto it = delResult.begin(); it != delResult.end(); ++it) {
			auto item = it.GetItem();
			string_view itemJson = item.GetJSON();
			std::cout << itemJson << std::endl;
		}

		{
			Query qSel;
			qSel.FromSQL("select * from ns1 where id<500");
			client::QueryResults selResult;
			serverSlave->api.reindexer->Select(qSel, selResult);
			int counter = 0;

			for (auto it = selResult.begin(); it != selResult.end(); ++it, counter++) {
				auto item = it.GetItem();
				if (counter % 50 == 0 && leaderId != -1) {
					cluster.StopServer(leaderId);
					oldLeaderId = leaderId;
					leaderId = -1;
				} else if (counter % 75 == 0 && oldLeaderId != -1) {
					cluster.StartServer(oldLeaderId);
					leaderId = cluster.AwaitLeader(std::chrono::seconds(10));
				}

				Error err = serverSlave->api.reindexer->Delete("ns1", item);
				ASSERT_TRUE(err.ok()) << err.what();
			}
		}

		Query qSel;
		qSel.FromSQL("select * from ns1");
		client::QueryResults selResult;
		serverSlave->api.reindexer->Select(qSel, selResult);
		std::cout << "selected items" << std::endl;
		for (auto it = selResult.begin(); it != selResult.end(); ++it) {
			string_view itemJson = it.GetItem().GetJSON();
			std::cout << itemJson << std::endl;
		}

		//		std::this_thread::sleep_for(std::chrono::seconds(1000));
		cluster.StopClients();
	});

	loop.run();
}

TEST_F(ClusterizationApi, LeaderElections) {
	// Check leader election on deffirent conditions
	const size_t kClusterSize = 4;
	net::ev::dynamic_loop loop;
	loop.spawn([&loop] {
		Cluster cluster(loop, 0, kClusterSize);
		// Await leader and make sure, that it will be elected only once
		auto leaderId = cluster.AwaitLeader(std::chrono::seconds(5), true);
		ASSERT_NE(leaderId, -1);
		std::cerr << "!!! Terminating servers..." << std::endl;
		for (size_t i = 0; i < kClusterSize; ++i) {
			ASSERT_TRUE(cluster.StopServer(i));
		}

		std::cerr << "!!! Launch half of the servers..." << std::endl;
		// Launch half of the servers (no consensus)
		for (size_t i = 0; i < (kClusterSize + 1) / 2; ++i) {
			ASSERT_TRUE(cluster.StartServer(i));
		}
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_EQ(leaderId, -1);

		std::cerr << "!!! Now we should have consensus..." << std::endl;
		// Now we should have consensus
		cluster.StartServer((kClusterSize + 1) / 2);
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_NE(leaderId, -1);

		std::cerr << "!!! Launch rest of the nodes..." << std::endl;
		// Launch rest of the nodes
		for (size_t i = (kClusterSize + 1) / 2 + 1; i < kClusterSize; ++i) {
			ASSERT_TRUE(cluster.StartServer(i));
		}
		auto newLeaderId = cluster.AwaitLeader(kMaxElectionsTime, true);
		ASSERT_EQ(leaderId, newLeaderId);

		std::cerr << "!!! Stop nodes without cluster fail..." << std::endl;
		// Stop nodes without cluster fail
		auto safeToRemoveCnt = kClusterSize - ((kClusterSize + 1) / 2) - 1;
		for (size_t i = 0; i < safeToRemoveCnt; ++i) {
			ASSERT_TRUE(cluster.StopServer(i));
		}
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_NE(leaderId, -1);

		std::cerr << "!!! Remove one more node (cluster should not be able to choose the leader)..." << std::endl;
		// Remove one more node (cluster should not be able to choose the leader)
		ASSERT_TRUE(cluster.StopServer(safeToRemoveCnt));
		loop.sleep(std::chrono::seconds(5));
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, true);
		ASSERT_EQ(leaderId, -1);
		cluster.StopClients();
	});

	loop.run();
}

TEST_F(ClusterizationApi, OnlineUpdates) {
	// Check basic online replication in cluster
	constexpr size_t kClusterSize = 5;
	net::ev::dynamic_loop loop;
	loop.spawn([&loop] {
		Cluster cluster(loop, 0, kClusterSize);
		int leadrId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leadrId, -1);

		// Create namespace and fill initial data
		const string_view kNsSome = "some";
		constexpr size_t kDataPortion = 100;
		std::cerr << "Init NS" << std::endl;
		cluster.InitNs(leadrId, kNsSome);
		std::cerr << "Fill data" << std::endl;
		cluster.FillData(leadrId, kNsSome, 0, kDataPortion);
		std::cerr << "Wait sync" << std::endl;
		cluster.WaitSync(kNsSome);

		// Stop node, fill more data and await sync
		for (size_t i = 0; i < (kClusterSize) / 2; ++i) {
			std::cerr << "Stopping " << i << std::endl;
			ASSERT_TRUE(cluster.StopServer(i));
			std::cerr << "Await leader" << std::endl;
			leadrId = cluster.AwaitLeader(kMaxElectionsTime);
			ASSERT_NE(leadrId, -1);
			std::cerr << "Fill data" << std::endl;
			cluster.FillData(leadrId, kNsSome, (i + 1) * kDataPortion, kDataPortion);
			std::cerr << "Wait sync" << std::endl;
			cluster.WaitSync(kNsSome);
		}
		std::cerr << "Done" << std::endl;

		cluster.StopClients();
	});

	loop.run();
}

TEST_F(ClusterizationApi, ForceAndWalSync) {
	// Check full cluster synchronization via all the available mechanisms
	constexpr size_t kClusterSize = 7;
	net::ev::dynamic_loop loop;
	loop.spawn([&loop] {
		Cluster cluster(loop, 0, kClusterSize);
		int leadrId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leadrId, -1);

		const std::string kNsSome = "some";
		constexpr size_t kDataPortion = 100;

		// Fill data for N/2 + 1 nodes
		for (size_t i = 0; i < (kClusterSize) / 2; ++i) {
			std::cerr << "Stopping " << i << std::endl;
			ASSERT_TRUE(cluster.StopServer(i));
		}
		leadrId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leadrId, -1);
		std::cerr << "Leader id is " << leadrId << std::endl;

		std::cerr << "Fill data 1" << std::endl;
		cluster.InitNs(leadrId, kNsSome);
		cluster.FillData(leadrId, kNsSome, 0, kDataPortion);

		{
			// Some update request with row-based replication mode
			client::QueryResults qr;
			Query q =
				Query(kNsSome).Where(kIdField, CondGe, int(10)).Where(kIdField, CondLe, int(12)).Set(kStringField, randStringAlph(15));
			auto err = cluster.GetNode(leadrId)->api.reindexer->Update(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
		}

		// Check if the data were replicated after nodes restart
		for (size_t i = 0; i < (kClusterSize) / 2; ++i) {
			std::cerr << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		std::cerr << "Wait sync 1" << std::endl;
		cluster.WaitSync(kNsSome);

		// Stop half of the nodes again
		for (size_t i = (kClusterSize) / 2 + 1; i < kClusterSize; ++i) {
			std::cerr << "Stopping " << i << std::endl;
			ASSERT_TRUE(cluster.StopServer(i));
		}

		leadrId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leadrId, -1);
		std::cerr << "Leader id is " << leadrId << std::endl;

		// Fill data and then start node. Repeat until all nodes in the cluster are alive
		std::cerr << "Fill data 2" << std::endl;
		cluster.FillData(leadrId, kNsSome, kDataPortion, kDataPortion);
		for (size_t i = (kClusterSize) / 2 + 1; i < kClusterSize; ++i) {
			std::cerr << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
			std::cerr << "Fill more" << std::endl;
			cluster.FillData(leadrId, kNsSome, (i + 2) * kDataPortion, kDataPortion);
		}
		std::cerr << "Wait sync 2" << std::endl;
		cluster.WaitSync(kNsSome);

		cluster.StopClients();
	});

	loop.run();
}

TEST_F(ClusterizationApi, InitialLeaderSync) {
	// Check if new leader is able to get newest data from other nodes after elections
	net::ev::dynamic_loop loop;
	loop.spawn([&loop] {
		constexpr size_t kClusterSize = 7;
		const std::vector<size_t> kFirstNodesGroup = {0, 1, 2};	  // Servers to stop after data fill
		const size_t kTransitionServer = 3;						  // The only server, which will have actual data. It belongs to both groups
		const std::vector<size_t> kSecondNodesGroup = {4, 5, 6};  // Empty servers, which have to perfomr sync
		Cluster cluster(loop, 0, kClusterSize);

		const string_view kNsSome = "some";
		constexpr size_t kDataPortion = 100;

		// Check force initial sync for nodes from the second group
		for (auto i : kSecondNodesGroup) {
			ASSERT_TRUE(cluster.StopServer(i));
		}

		auto leadrId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leadrId, -1);
		std::cerr << "Leader id is " << leadrId << std::endl;

		// Fill data for nodes from the first group
		std::cerr << "Fill data 1" << std::endl;
		cluster.InitNs(leadrId, kNsSome);
		cluster.FillData(leadrId, kNsSome, 0, kDataPortion);
		std::cerr << "Wait sync 1" << std::endl;
		cluster.WaitSync(kNsSome);

		// Stop cluster
		for (auto i : kFirstNodesGroup) {
			std::cerr << "Stopping " << i << std::endl;
			ASSERT_TRUE(cluster.StopServer(i));
		}
		std::cerr << "Stopping " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StopServer(kTransitionServer));

		// Start second group. kTransitionServer is the only node with data from previous step
		for (auto i : kSecondNodesGroup) {
			std::cerr << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		std::cerr << "Starting " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StartServer(kTransitionServer));

		std::cerr << "Wait sync 2" << std::endl;
		cluster.WaitSync(kNsSome);

		// Make sure, that our cluster didn't miss it's data in process
		auto state = cluster.GetNode(kClusterSize - 1)->GetState(std::string(kNsSome));
		ASSERT_EQ(state.lsn.Counter(), 102);

		// Check WAL initial sync for nodes from the first group
		leadrId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leadrId, -1);
		std::cerr << "Leader id is " << leadrId << std::endl;

		// Fill additional data for second group
		std::cerr << "Fill data 2" << std::endl;
		cluster.FillData(leadrId, kNsSome, 50, kDataPortion);

		// Stop cluster
		for (auto i : kSecondNodesGroup) {
			std::cerr << "Stopping " << i << std::endl;
			ASSERT_TRUE(cluster.StopServer(i));
		}
		std::cerr << "Stopping " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StopServer(kTransitionServer));

		// Start first group againt and make sure, that all of the daa were replicated from transition node
		for (auto i : kFirstNodesGroup) {
			std::cerr << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		std::cerr << "Starting " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StartServer(kTransitionServer));

		std::cerr << "Wait sync 3" << std::endl;
		cluster.WaitSync(kNsSome);

		state = cluster.GetNode(0)->GetState(std::string(kNsSome));
		ASSERT_EQ(state.lsn.Counter(), 202);

		cluster.StopClients();
	});

	loop.run();
}

TEST_F(ClusterizationApi, MultithreadSyncTest) {
	// Check full cluster synchronization via all the available mechanisms
	constexpr size_t kClusterSize = 5;
	net::ev::dynamic_loop loop;
	loop.spawn([&loop] {
		Cluster cluster(loop, 0, kClusterSize, std::chrono::milliseconds(200));
		int leadrId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leadrId, -1);

		const std::vector<std::string> kNsNames = {"ns1", "ns2", "ns3"};
		std::vector<std::thread> threads;
		constexpr size_t kDataPortion = 100;
		constexpr size_t kMaxDataId = 10000;
		std::atomic<bool> terminate = {false};

		leadrId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leadrId, -1);
		std::cerr << "Leader id is " << leadrId << std::endl;

		auto dataFillF = ([&cluster, &terminate, leadrId](const std::string& ns) {
			while (!terminate) {
				constexpr size_t kDataPart = kDataPortion / 10;
				for (size_t i = 0; i < 10; ++i) {
					auto idx = rand() % kMaxDataId;
					cluster.FillData(leadrId, ns, idx, kDataPart);
					std::this_thread::sleep_for(std::chrono::milliseconds(20));
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
			}
		});
		auto selectF = ([&cluster, &terminate](const std::string& ns) {
			while (!terminate) {
				auto id = rand() % kClusterSize;
				client::QueryResults qr;
				auto node = cluster.GetNode(id);
				if (node) {
					node->api.reindexer->Select(Query(ns), qr);
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
			}
		});
		auto updateF = ([&cluster, &terminate, leadrId](const std::string& ns) {
			while (!terminate) {
				int minIdx = rand() % kMaxDataId;
				int maxIdx = minIdx + 300;
				client::QueryResults qr;
				Query q = Query(ns).Where(kIdField, CondGe, minIdx).Where(kIdField, CondLe, maxIdx).Set(kStringField, randStringAlph(15));
				auto err = cluster.GetNode(leadrId)->api.reindexer->Update(q, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
			}
		});
		auto deleteF = ([&cluster, &terminate, leadrId](const std::string& ns) {
			while (!terminate) {
				int minIdx = rand() % kMaxDataId;
				int maxIdx = minIdx + 200;
				client::QueryResults qr;
				Query q = Query(ns).Where(kIdField, CondGe, minIdx).Where(kIdField, CondLe, maxIdx);
				auto err = cluster.GetNode(leadrId)->api.reindexer->Delete(q, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
			}
		});

		// Create few different thread for each namespace
		for (auto& ns : kNsNames) {
			cluster.InitNs(leadrId, ns);

			threads.emplace_back(dataFillF, std::ref(ns));
			threads.emplace_back(dataFillF, std::ref(ns));
			// threads.emplace_back(updateF, std::ref(ns));
			(void)updateF;
			threads.emplace_back(deleteF, std::ref(ns));
			(void)deleteF;
			threads.emplace_back(selectF, std::ref(ns));
			(void)selectF;
		}

		// Restart followers
		std::vector<size_t> stopped;
		{
			int i = 0;
			while (stopped.size() < kClusterSize / 2) {
				if (i != leadrId) {
					ASSERT_TRUE(cluster.StopServer(i));
					stopped.emplace_back(i);
				}
				++i;
			}
		}
		for (auto i : stopped) {
			ASSERT_TRUE(cluster.StartServer(i));
		}

		terminate = true;
		for (auto& th : threads) {
			th.join();
		}

		// Make shure, that cluster is synchronized
		for (auto& ns : kNsNames) {
			cluster.WaitSync(ns);
		}

		cluster.StopClients();
	});

	loop.run();
}
