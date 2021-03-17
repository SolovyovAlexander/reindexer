#!/bin/sh

CLUSTER_DB_NAME=cluster_db

RX_ARGS="--db $RX_DATABASE --httpaddr 0:9088 --rpccluster 0:8534 --rpcaddr 0:6534 --clusteraddr 0:7645 --webroot /usr/local/share/reindexer/web --corelog $RX_CORELOG --serverlog $RX_SERVERLOG --httplog $RX_HTTPLOG --rpclog $RX_RPCLOG --loglevel $RX_LOGLEVEL"

mkdir -p $RX_DATABASE/$CLUSTER_DB_NAME

CLUSTER_CONF=$RX_DATABASE/$CLUSTER_DB_NAME/cluster.conf
REPL_CONF=$RX_DATABASE/$CLUSTER_DB_NAME/replication.conf

echo "leveldb" > $RX_DATABASE/$CLUSTER_DB_NAME/.reindexer.storage
>$CLUSTER_CONF
echo "app_name: rx_node" >> $CLUSTER_CONF
echo "namespaces: []" >> $CLUSTER_CONF
echo "sync_threads: 4" >> $CLUSTER_CONF
echo "enable_compression: true" >> $CLUSTER_CONF
echo "updates_timeout_sec: 15" >> $CLUSTER_CONF
echo "retry_sync_interval_msec: 5000" >> $CLUSTER_CONF
echo "nodes:" >> $CLUSTER_CONF
echo "  -" >> $CLUSTER_CONF
echo "    ip_addr: node0" >> $CLUSTER_CONF
echo "    server_id: 0" >> $CLUSTER_CONF
echo "    database: $CLUSTER_DB_NAME" >> $CLUSTER_CONF
echo "    rpc_port: 8534" >> $CLUSTER_CONF
echo "    management_port: 7645" >> $CLUSTER_CONF
echo "  -" >> $CLUSTER_CONF
echo "    ip_addr: node1" >> $CLUSTER_CONF
echo "    server_id: 1" >> $CLUSTER_CONF
echo "    database: $CLUSTER_DB_NAME" >> $CLUSTER_CONF
echo "    rpc_port: 8534" >> $CLUSTER_CONF
echo "    management_port: 7645" >> $CLUSTER_CONF
echo "  -" >> $CLUSTER_CONF
echo "    ip_addr: node2" >> $CLUSTER_CONF
echo "    server_id: 2" >> $CLUSTER_CONF
echo "    database: $CLUSTER_DB_NAME" >> $CLUSTER_CONF
echo "    rpc_port: 8534" >> $CLUSTER_CONF
echo "    management_port: 7645" >> $CLUSTER_CONF
echo "  -" >> $CLUSTER_CONF
echo "    ip_addr: node3" >> $CLUSTER_CONF
echo "    server_id: 3" >> $CLUSTER_CONF
echo "    database: $CLUSTER_DB_NAME" >> $CLUSTER_CONF
echo "    rpc_port: 8534" >> $CLUSTER_CONF
echo "    management_port: 7645" >> $CLUSTER_CONF
echo "  -" >> $CLUSTER_CONF
echo "    ip_addr: node4" >> $CLUSTER_CONF
echo "    server_id: 4" >> $CLUSTER_CONF
echo "    database: $CLUSTER_DB_NAME" >> $CLUSTER_CONF
echo "    rpc_port: 8534" >> $CLUSTER_CONF
echo "    management_port: 7645" >> $CLUSTER_CONF

>$REPL_CONF
echo "role: master" >> $REPL_CONF
echo "cluster_id: 2" >> $REPL_CONF
echo "server_id: $RX_SERVER_ID" >> $REPL_CONF
cat $REPL_CONF

if [ -n "$RX_PPROF" ]; then
    RX_ARGS="$RX_ARGS --pprof --allocs"
    export TCMALLOC_SAMPLE_PARAMETER=512000
    export MALLOC_CONF=prof:true
fi

if [ -n "$RX_PROMETHEUS" ]; then
    RX_ARGS="$RX_ARGS --prometheus"
fi

if [ -z "$@" ]; then
   reindexer_server $RX_ARGS
else 
   exec "$@"
fi
