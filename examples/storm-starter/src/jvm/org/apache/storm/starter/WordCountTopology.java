/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter;

import org.apache.storm.starter.bolt.SplitSentenceForCountBolt;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 * 提交任务
 * storm jar storm-starter-2.0.0-SNAPSHOT.jar org.apache.storm.starter.WordCountTopology wordCountTopology 30 30
 */
public class WordCountTopology extends ConfigurableTopology {
//  public static class SplitSentence extends ShellBolt implements IRichBolt {
//
//    public SplitSentence() {
//      super("python", "splitsentence.py");
//    }
//
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//      declarer.declare(new Fields("word"));
//    }
//
//    @Override
//    public Map<String, Object> getComponentConfiguration() {
//      return null;
//    }
//  }

//  public static class WordCount extends BaseBasicBolt {
//    Map<String, Integer> counts = new HashMap<String, Integer>();
//
//    @Override
//    public void execute(Tuple tuple, BasicOutputCollector collector) {
//      String word = tuple.getString(0);
//      Integer count = counts.get(word);
//      if (count == null)
//        count = 0;
//      count++;
//      counts.put(word, count);
//      collector.emit(new Values(word, count));
//    }
//
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//      declarer.declare(new Fields("word", "count"));
//    }
//  }
  public static void main(String[] args) throws Exception {
    ConfigurableTopology.start(new WordCountTopology(), args);
  }

  protected int run(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new RandomSentenceSpout(), 1);
    Integer numworkers= Integer.valueOf(args[1]);
    Integer splitInstancesNum=Integer.valueOf(args[2]);
//    builder.setBolt("split", new SplitSentence(), 3).allGrouping("spout");
//    int boltNum = 3;
//    if(args != null && args.length > 1)
//      boltNum = Integer.getInteger(args[1]);

    builder.setBolt("split", new SplitSentenceForCountBolt(), splitInstancesNum).allGrouping("spout");
    builder.setBolt("reportLatency", new LatencyReportBolt(), 1).shuffleGrouping("split");

    conf.setDebug(true);

    String topologyName = "word-count";

    conf.setNumWorkers(numworkers);
    conf.setNumAckers(0);

    if (args != null && args.length > 0) {
      topologyName = args[0];
    }
    return submit(topologyName, conf, builder);
  }
}
