/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.starter.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class SplitSentenceForCountBolt extends BaseRichBolt {
    public static final String FIELDS = "word";
    private static final Logger LOG = LoggerFactory.getLogger(SplitSentenceForCountBolt.class);
    private Long tuples;
    private Long millionTuple;
    private int thisTaskId=0;
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        tuples = 0L;
        millionTuple = 0L;
        this.thisTaskId = context.getThisTaskId();
        this.outputCollector=collector;
    }

    @Override
    public void execute(Tuple input) {
//        LOG.info("the time of receiving tuple in bolt: {}", System.currentTimeMillis());
        Long startTimeMills = input.getLongByField("timeinfo");
        Long endTimeMills=System.currentTimeMillis();
        if (tuples < 1000000){
            tuples++;
        }
        else {
            millionTuple++;
            tuples = 0L;
        }
        //LOG.info("{}: the time of receiving {} million and {} tuple at {}", thisTaskId, millionTuple, tuples, endTimeMills);
        Long delay=endTimeMills-startTimeMills;
        outputCollector.emit(new Values(thisTaskId,delay,startTimeMills));
        for (String word : splitSentence(input.getString(0))) {
            //outputCollector.emit(new Values(word));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskid","delay","timeinfo"));
    }


    public static String[] splitSentence(String sentence) {
        if (sentence != null) {
            return sentence.split("\\s+");
        }
        return null;
    }

}
