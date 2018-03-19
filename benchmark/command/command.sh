#!/bin/bash
#DiDi滴滴打车订单匹配Topology
/storm/kafka_2.10-0.10.2.1/bin/kafka-topics.sh --create --zookeeper node100:2181,node101:2181,node102:2181 --replication-factor 3 --partitions 1 --topic ordersTopic_1
cat /storm/DiDiData/orders | /storm/kafka_2.10-0.10.2.1/bin/kafka-console-producer.sh --broker-list node101:9092,node102:9092,node103:9092,node104:9092,node105:9092,node106:9092 --topic ordersTopic_1
storm jar /home/zhangfan/didiOrderMatch-2.0.0-SNAPSHOT.jar org.apache.storm.DiDiOrderMatchTopology DiDiOrderMatchTopology ordersTopic_1 3 1 6

#NASDAQ股票交易Topology
/storm/kafka_2.10-0.10.2.1/bin/kafka-topics.sh --create --zookeeper node100:2181,node101:2181,node102:2181 --replication-factor 3 --partitions 1 --topic stockdealTopic
cat /storm/StockDealData/STOCKtrace3.txt | /storm/kafka_2.10-0.10.2.1/bin/kafka-console-producer.sh --broker-list node101:9092,node102:9092,node103:9092,node104:9092,node105:9092,node106:9092 --topic stockdealTopic
storm jar NASDAQStockDeal-2.0.0-SNAPSHOT.jar org.apache.storm.StockeDealThroughputTopology StockeDealThroughputTopology stockdealTopic 30 1 30

#数据库操作命令
select avg(latency) from t_latency order by time desc limit 10;
select avg(throughput) from t_throughput
