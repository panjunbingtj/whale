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
package org.apache.storm.executor.spout;

import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.Task;
import org.apache.storm.executor.TupleInfo;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MutableLong;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SpoutOutputCollectorImpl implements ISpoutOutputCollector {

    private final SpoutExecutor executor;
    private final Task taskData;
    private final int taskId;
    private final MutableLong emittedCount;
    private final boolean hasAckers;
    private final Random random;
    private final Boolean isEventLoggers;
    private final Boolean isDebug;
    private final RotatingMap<Long, TupleInfo> pending;
    public static final Logger LOG = LoggerFactory.getLogger(SpoutOutputCollectorImpl.class);

    @SuppressWarnings("unused")
    public SpoutOutputCollectorImpl(ISpout spout, SpoutExecutor executor, Task taskData, int taskId,
                                    MutableLong emittedCount, boolean hasAckers, Random random,
                                    Boolean isEventLoggers, Boolean isDebug, RotatingMap<Long, TupleInfo> pending) {
        this.executor = executor;
        this.taskData = taskData;
        this.taskId = taskId;
        this.emittedCount = emittedCount;
        this.hasAckers = hasAckers;
        this.random = random;
        this.isEventLoggers = isEventLoggers;
        this.isDebug = isDebug;
        this.pending = pending;
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        return sendSpoutMsg(streamId, tuple, messageId, null);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        sendSpoutMsg(streamId, tuple, messageId, taskId);
    }

    @Override
    public long getPendingCount() {
        return pending.size();
    }

    @Override
    public void reportError(Throwable error) {
        executor.getErrorReportingMetrics().incrReportedErrorCount();
        executor.getReportError().report(error);
    }

    /**
     * 1.首先 Spout调用sendSpoutMsg() 发送一个tuple到下游bolt
     * @param stream
     * @param values
     * @param messageId
     * @param outTaskId
     * @return
     */
    private List<Integer> sendSpoutMsg(String stream, List<Object> values, Object messageId, Integer outTaskId) {
        emittedCount.increment();

        List<Integer> outTasks;
        if (outTaskId != null) {
            outTasks = taskData.getOutgoingTasks(outTaskId, stream, values);
        } else {
            outTasks = taskData.getOutgoingTasks(stream, values);
        }

        List<Long> ackSeq = new ArrayList<>();
        boolean needAck = (messageId != null) && hasAckers;

        long rootId = MessageId.generateId(random);

        //2.根据上游spout和下游bolt之间的分组信息，将tuple发送到下游相应的task中，并且封装成TupleImpl类
        LOG.info("the time of start copying : {}", System.currentTimeMillis());
        for (Integer t : outTasks) {
            MessageId msgId;
            if (needAck) {
                long as = MessageId.generateId(random);
                msgId = MessageId.makeRootId(rootId, as);
                ackSeq.add(as);
            } else {
                msgId = MessageId.makeUnanchored();
            }

            TupleImpl tuple = new TupleImpl(executor.getWorkerTopologyContext(), values, this.taskId, stream, msgId);
            //3.outputCollector调用executor的ExecutorTransfer类的transfer方法(）将tuple添加目标taskId信息，封装成AddressTuple
            executor.getExecutorTransfer().transfer(t, tuple);
        }
        LOG.info("the time of end copying : {}", System.currentTimeMillis());

        if (isEventLoggers) {
            executor.sendToEventLogger(executor, taskData, values, executor.getComponentId(), messageId, random);
        }

        boolean sample = false;
        try {
            sample = executor.getSampler().call();
        } catch (Exception ignored) {
        }
        if (needAck) {
            TupleInfo info = new TupleInfo();
            info.setTaskId(this.taskId);
            info.setStream(stream);
            info.setMessageId(messageId);
            if (isDebug) {
                info.setValues(values);
            }
            if (sample) {
                info.setTimestamp(System.currentTimeMillis());
            }

            pending.put(rootId, info);
            List<Object> ackInitTuple = new Values(rootId, Utils.bitXorVals(ackSeq), this.taskId);
            executor.sendUnanchored(taskData, Acker.ACKER_INIT_STREAM_ID, ackInitTuple, executor.getExecutorTransfer());
        } else if (messageId != null) {
            TupleInfo info = new TupleInfo();
            info.setStream(stream);
            info.setValues(values);
            info.setMessageId(messageId);
            info.setTimestamp(0);
            Long timeDelta = sample ? 0L : null;
            info.setId("0:");
            executor.ackSpoutMsg(executor, taskData, timeDelta, info);
        }

        return outTasks;
    }
}
