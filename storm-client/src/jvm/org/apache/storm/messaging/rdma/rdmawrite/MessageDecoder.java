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
package org.apache.storm.messaging.rdma.rdmawrite;

import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.messaging.netty.ControlMessage;
import org.apache.storm.messaging.netty.SaslMessageToken;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

////////////////////////////////////优化transferAllGrouping/////////////////////////
public class MessageDecoder {
    /*
     * Each ControlMessage is encoded as:
     *  code (<0) ... short(2)
     * Each WorkerMessage is encoded as:
     *  tasks_size short(2)
     *  tasks_id ... List<short>(2)
     *  len ... int(4)
     *  payload ... byte[]    *
     */
    protected Object decode( ByteBuffer buf) throws Exception {
        // Make sure that we have received at least a short 
        long available = buf.remaining();
        if (available < 2) {
            //need more data
            return null;
        }

        List<Object> ret = new ArrayList<>();

        // Use while loop, try to decode as more messages as possible in single call
        while (available >= 2) {

            // Mark the current buffer position before reading task/len field
            // because the whole frame might not be in the buffer yet.
            // We will reset the buffer position to the marked position if
            // there's not enough bytes in the buffer.
            buf.mark();

            // read the short field
            short code = buf.getShort();
            available -= 2;

            // case 1: Control message
            ControlMessage ctrl_msg = ControlMessage.mkMessage(code);
            if (ctrl_msg != null) {

                if (ctrl_msg == ControlMessage.EOB_MESSAGE) {
                    continue;
                } else {
                    return ctrl_msg;
                }
            }
            
            //case 2: SaslTokenMessageRequest
            if(code == SaslMessageToken.IDENTIFIER) {
            	// Make sure that we have received at least an integer (length) 
                if (buf.remaining() < 4) {
                    //need more data
                    buf.reset();
                    return null;
                }
                
                // Read the length field.
                int length = buf.getInt();
                if (length<=0) {
                    return new SaslMessageToken(null);
                }
                
                // Make sure if there's enough bytes in the buffer.
                if (buf.remaining() < length) {
                    // The whole bytes were not received yet - return null.
                    buf.reset();
                    return null;
                }
                
                // There's enough bytes in the buffer. Read it.
                byte[] payload =new byte[length];
                buf.get(payload);
                
                // Successfully decoded a frame.
                // Return a SaslTokenMessageRequest object
                return new SaslMessageToken(payload);
            }

            // case 3: Worker Message

            // Make sure that we have received at least an integer (length)
            if (available < 4+2*code) {
                // need more data
                buf.reset();
                break;
            }

            // Read the tasks_id field
            List<Integer> task_ids=new ArrayList<>();
            for(int i=0;i<code;i++){
                task_ids.add((int)buf.getShort());
            }

            // Read the length field.
            int length = buf.getInt();

            available -= (4+2*code);

            if (length <= 0) {
                ret.add(new WorkerMessage(task_ids, null));
                break;
            }

            // Make sure if there's enough bytes in the buffer.
            if (available < length) {
                // The whole bytes were not received yet - return null.
                buf.reset();
                break;
            }
            available -= length;

            // There's enough bytes in the buffer. Read it.
            // There's enough bytes in the buffer. Read it.
            byte[] payload =new byte[length];
            buf.get(payload);

            //RDMA Whale Benchmark
            //Integer workerNums=6;
//            for(int i=0;i<workerNums;i++) {
//                buf.getShort();
//                available -= 2;
//            }

            // Successfully decoded a frame.
            // Return a TaskMessage object
            ret.add(new WorkerMessage(task_ids, payload));
        }

        if (ret.size() == 0) {
            return null;
        } else {
            return ret;
        }
    }
}
