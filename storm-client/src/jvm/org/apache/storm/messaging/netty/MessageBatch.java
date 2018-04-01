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
package org.apache.storm.messaging.netty;

import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.serialization.KryoWorkerMessageSerializer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;

import java.nio.ByteBuffer;
import java.util.ArrayList;

////////////////////////////////////优化transferAllGrouping/////////////////////////
class MessageBatch {
    private int buffer_size;
    private ArrayList<ByteBuffer> msgs;
    private int encoded_length;
    private KryoWorkerMessageSerializer serializer;

    MessageBatch(int buffer_size,KryoWorkerMessageSerializer serializer) {
        this.buffer_size = buffer_size;
        this.serializer=serializer;
        msgs=new ArrayList<ByteBuffer>();
        encoded_length = ControlMessage.EOB_MESSAGE.encodeLength();
    }

    void add(WorkerMessage msg) {
        if (msg == null)
            throw new RuntimeException("null object forbidden in message batch");

        byte[] bytes = serializer.serialize(msg);
        msgs.add(ByteBuffer.wrap(bytes));
        encoded_length +=(2+4+ bytes.length);
    }


//    private int msgEncodeLength(WorkerMessage workerMessage) {
//        if (workerMessage == null) return 0;
//
//        int size = 4; //INT
//        if (workerMessage.message() != null)
//            size += workerMessage.getEncodeLength();
//        return size;
//    }

    /**
     * @return true if this batch used up allowed buffer size
     */
    boolean isFull() {
        return encoded_length >= buffer_size;
    }

    /**
     * @return true if this batch doesn't have any messages
     */
    boolean isEmpty() {
        return msgs.isEmpty();
    }

    /**
     * @return number of msgs in this batch
     */
    int size() {
        return msgs.size();
    }

    /**
     * create a buffer containing the encoding of this batch
     */
    ChannelBuffer buffer() throws Exception {
        ChannelBufferOutputStream bout = new ChannelBufferOutputStream(ChannelBuffers.directBuffer(encoded_length));

        for(ByteBuffer buffer :msgs){
            writeWorkerMessage(bout, buffer);
        }

        //add a END_OF_BATCH indicator
        ControlMessage.EOB_MESSAGE.write(bout);

        bout.close();

        return bout.buffer();
    }

    /**
     * write a WorkerMessage into a stream
     *
     * Each WorkerMessage is encoded as:
     *  tasks_size short(2)
     *  tasks_id ... List<short>(2)
     *  _messageId_size short(2)
     *  _messageIds... List<MessageId>
     *  len ... int(4)
     *  payload ... byte[]     *  
     */
    private void writeWorkerMessage(ChannelBufferOutputStream bout, ByteBuffer buffer) throws Exception {
        bout.writeShort(-800);
        bout.writeInt(buffer.array().length);
        bout.write(buffer.array());
    }

}

