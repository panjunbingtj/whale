package org.apache.storm.messaging;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * locate org.apache.storm.messaging
 * Created by tjmaster on 18-2-22.
 * 往Worker进程中发送消息
 */
public class WorkerMessage {
    private long _startTimeMills=0L;
    private List<Integer> _taskIds;
    private byte[] _message;

    public WorkerMessage(List<Integer> _taskIds, byte[] _message) {
        this._taskIds = _taskIds;
        this._message = _message;
    }

    public List<Integer> tasks() {
        return _taskIds;
    }

    public byte[] message() {
        return _message;
    }

    public WorkerMessage(List<Integer> _taskIds, Long startTimeMills, byte[] message) {
        this._taskIds = _taskIds;
        this._startTimeMills=startTimeMills;
        _message = message;
    }

    public ByteBuffer serialize() {
        ByteBuffer bb = ByteBuffer.allocate(2+_message.length+2*_taskIds.size());
        bb.putShort((short)_taskIds.size());
        for(int _task : _taskIds){
            bb.putShort((short)_task);
        }
        bb.putLong(_startTimeMills);
        bb.put(_message);
        return bb;
    }

    public void deserialize(ByteBuffer packet) {
        if (packet==null) return;
        int size=packet.getShort();
        for(int i=0;i<size;i++){
            _taskIds.add((int) packet.getShort());
        }
        _startTimeMills=packet.getLong();
        _message = new byte[packet.limit()-2*size];
        packet.get(_message);
    }

    public int getEncodeLength(){
        return 2+_message.length+2*_taskIds.size()+8;
    }

    @Override
    public String toString() {
        return "WorkerMessage{" +
                "_taskIds=" + _taskIds +
                ", _message=" + Arrays.toString(_message) +
                '}';
    }

    public long getStartTimeMills() {
        return _startTimeMills;
    }

    public void setStartTimeMills(long _startTimeMills) {
        this._startTimeMills = _startTimeMills;
    }
}
