package org.apache.storm.messaging;

import org.apache.storm.tuple.MessageId;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * locate org.apache.storm.messaging
 * Created by tjmaster on 18-2-22.
 * 往Worker进程中发送消息
 */
public class WorkerMessage {
    private List<Integer> _taskIds;
    private List<MessageId> _messageIdList;
    private byte[] _message;

    public WorkerMessage(List<Integer> _taskIds,  List<MessageId> _messageIdList, byte[] _message) {
        this._taskIds = _taskIds;
        this._messageIdList=_messageIdList;
        this._message = _message;
    }

    public List<MessageId> getMessageIdList() {
        return _messageIdList;
    }

    public List<Integer> tasks() {
        return _taskIds;
    }

    public byte[] message() {
        return _message;
    }

    public ByteBuffer serialize() {
        ByteBuffer bb = ByteBuffer.allocate(2+_message.length+2*_taskIds.size());
        bb.putShort((short)_taskIds.size());
        for(int _task : _taskIds){
            bb.putShort((short)_task);
        }
        bb.putShort((short)_messageIdList.size());
        for(MessageId _msgId : _messageIdList){
            bb.putShort((short)_msgId.getAnchorsToIds().size());
            for(Map.Entry<Long, Long> anchorToId: _msgId.getAnchorsToIds().entrySet()) {
                bb.putLong(anchorToId.getKey());
                bb.putLong(anchorToId.getValue());
            }
        }
        bb.put(_message);
        return bb;
    }

    public void deserialize(ByteBuffer packet) {
        if (packet==null) return;
        int totalsize=0;
        int taskidsize=packet.getShort();
        for(int i=0;i<taskidsize;i++){
            _taskIds.add((int) packet.getShort());
        }
        int messageidsize=packet.getShort();
        for(int i=0;i<messageidsize;i++){
            MessageId messageId;
            HashMap<Long,Long> AnchorsToIds=new HashMap<>();
            int AnchorsToIdsSize=packet.getShort();
            for(int j=0;j<AnchorsToIdsSize;j++){
                long key = packet.getLong();
                long value = packet.getLong();
                AnchorsToIds.put(key,value);
            }
            messageId=MessageId.makeId(AnchorsToIds);
            _messageIdList.add(messageId);
            totalsize+=(AnchorsToIdsSize*8+2);
        }
        totalsize+=(2+2+2*taskidsize);
        _message = new byte[packet.limit()-totalsize];
        packet.get(_message);
    }

    public int getEncodeLength(){
        int total=0;
        total=2 + 2 + _message.length + 2*_taskIds.size();
        for(MessageId messageId:_messageIdList){
            int AnchorsToIdsSize = messageId.getAnchorsToIds().size();
            total+=(2+16*AnchorsToIdsSize);
        }
        return total;
    }

    @Override
    public String toString() {
        return "WorkerMessage{" +
                "_taskIds=" + _taskIds +
                ", _message=" + Arrays.toString(_message) +
                '}';
    }
}
