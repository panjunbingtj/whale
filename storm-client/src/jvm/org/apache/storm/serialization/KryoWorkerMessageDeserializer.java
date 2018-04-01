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

import com.esotericsoftware.kryo.io.Input;
import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.tuple.MessageId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KryoWorkerMessageDeserializer implements IWorkerMessageDeserializer {
    KryoValuesDeserializer _kryo;
    Input _kryoInput;

    public KryoWorkerMessageDeserializer(final Map<String, Object> conf) {
        _kryo = new KryoValuesDeserializer(conf);
        _kryoInput = new Input(1);
    }        

    public WorkerMessage deserialize(byte[] ser) {
        try {
            List<Integer> task_ids = new ArrayList<>();
            List<MessageId> messageIdList = new ArrayList<>();
            _kryoInput.setBuffer(ser);
            int taskSize = _kryoInput.readInt(true);
            for(int i=0;i<taskSize;i++){
                task_ids.add((int) _kryoInput.readShort());
            }

            int messageSize = _kryoInput.readInt(true);
            for(int i=0;i<messageSize;i++){
                MessageId id = MessageId.deserialize(_kryoInput);
                messageIdList.add(id);
            }
            int payload_len= _kryoInput.readInt(true);
            if (payload_len <= 0) {
                return new WorkerMessage(task_ids, messageIdList, null);
            }
            byte[] messages = _kryoInput.readBytes(payload_len);
            return new WorkerMessage(task_ids,messageIdList,messages);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
