package org.apache.storm.utils;

import org.junit.Test;

/**
 * locate org.apache.storm.utils
 * Created by mastertj on 2018/3/15.
 */
public class TimeUtilsTest {
    @Test
    public void waitForTimeNanos() throws Exception {
        PropertiesUtil.init("/storm-client-version-info.properties");
        long delay=Long.valueOf(PropertiesUtil.getProperties("serializationtime"));
        TimeUtils.waitForTimeNanos(delay);
    }

}
