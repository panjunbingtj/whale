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
package org.apache.storm.serialization;

import com.esotericsoftware.kryo.io.Output;
import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.tuple.MessageId;

import java.util.List;
import java.util.Map;

public class KryoWorkerMessageSerializer implements IWorkerMessageSerializer {
    KryoValuesSerializer _kryo;;
    Output _kryoOut;

    public KryoWorkerMessageSerializer(final Map<String, Object> conf) {
        _kryo = new KryoValuesSerializer(conf);
        _kryoOut = new Output(2000, 2000000000);
    }

    public byte[] serialize(WorkerMessage workerMessage) {
        try {
            _kryoOut.clear();
            int payload_len = 0;
            if (workerMessage.message() != null)
                payload_len =  workerMessage.message().length;

            List<Integer> task_ids = workerMessage.tasks();
            List<MessageId> messageIdList = workerMessage.getMessageIdList();
            _kryoOut.writeInt(task_ids.size(),true);

            for(int task_id : task_ids){
                if (task_id > Short.MAX_VALUE)
                    throw new RuntimeException("Task ID should not exceed "+Short.MAX_VALUE);
                _kryoOut.writeShort((short)task_id);
            }

            _kryoOut.writeInt(messageIdList.size(),true);
            for(int i=0;i<messageIdList.size();i++){
                writeMessageId(_kryoOut,messageIdList.get(i));
            }
            _kryoOut.writeInt(payload_len,true);

            if (payload_len >0)
                _kryoOut.write(workerMessage.message());

            return _kryoOut.toBytes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void writeMessageId(Output _kryoOut, MessageId messageId) throws Exception {
        _kryoOut.writeInt(messageId.getAnchorsToIds().size(),true);
        for(Map.Entry<Long, Long> anchorToId: messageId.getAnchorsToIds().entrySet()) {
            _kryoOut.writeLong(anchorToId.getKey(),true);
            _kryoOut.writeLong(anchorToId.getValue(),true);
        }
    }

//    public long crc32(Tuple tuple) {
//        try {
//            CRC32OutputStream hasher = new CRC32OutputStream();
//            _kryo.serializeInto(tuple.getValues(), hasher);
//            return hasher.getValue();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
}
