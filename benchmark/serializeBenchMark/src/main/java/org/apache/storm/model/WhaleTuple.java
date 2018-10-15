package org.apache.storm.model;

import java.util.List;

/**
 * locate org.apache.storm.model
 * Created by mastertj on 2018/3/23.
 */
public class WhaleTuple {
    private final List<Integer> taskids;

    private final String streamId;

    private final List<Object> values;

    public WhaleTuple(List<Integer> taskids, String streamId, List<Object> values) {
        this.taskids = taskids;
        this.streamId = streamId;
        this.values = values;
    }

    public List<Integer> getTaskids() {
        return taskids;
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
                "taskid=" + taskids +
                ", streamId='" + streamId + '\'' +
                ", values=" + values +
                '}';
    }


}
