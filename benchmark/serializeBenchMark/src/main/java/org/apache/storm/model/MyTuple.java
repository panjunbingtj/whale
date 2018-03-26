package org.apache.storm.model;

import java.io.Serializable;
import java.util.List;

/**
 * locate org.apache.storm.model
 * Created by mastertj on 2018/3/23.
 */
public class MyTuple implements Serializable{
    private final int taskid;

    private final String streamId;

    private final List<Object> values;

    public MyTuple(int taskid, String streamId, List<Object> values) {
        this.taskid = taskid;
        this.streamId = streamId;
        this.values = values;
    }

    public int getTaskid() {
        return taskid;
    }

    public String getStreamId() {
        return streamId;
    }

    public List<Object> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "MyTuple{" +
                "taskid=" + taskid +
                ", streamId='" + streamId + '\'' +
                ", values=" + values +
                '}';
    }


}
