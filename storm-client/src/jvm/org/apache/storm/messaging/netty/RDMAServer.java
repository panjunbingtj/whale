package org.apache.storm.messaging.netty;

import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.metric.api.IStatefulObject;
import org.jboss.netty.channel.Channel;

import java.util.Collection;
import java.util.Map;

/**
 * locate org.apache.storm.messaging.netty
 * Created by mastertj on 2018/8/23.
 */
public class RDMAServer extends ConnectionWithStatus implements IStatefulObject, ISaslServer  {
    @Override
    public Status status() {
        return null;
    }

    @Override
    public void registerRecv(IConnectionCallback cb) {

    }

    @Override
    public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {

    }

    @Override
    public void send(int taskId, byte[] payload) {

    }

    @Override
    public void send(WorkerMessage msgs) {

    }

    @Override
    public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
        return null;
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String secretKey() {
        return null;
    }

    @Override
    public void authenticated(Channel c) {

    }

    @Override
    public void channelConnected(Channel c) {

    }

    @Override
    public void received(Object message, String remote, Channel channel) throws InterruptedException {

    }

    @Override
    public void closeChannel(Channel c) {

    }

    @Override
    public Object getState() {
        return null;
    }
}
