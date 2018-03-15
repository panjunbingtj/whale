package org.apache.storm.utils;

import org.junit.Test;

/**
 * locate org.apache.storm.utils
 * Created by mastertj on 2018/3/15.
 */
public class TimeUtilsTest {
    @Test
    public void waitForTimeNanos() throws Exception {
        TimeUtils.waitForTimeNanos(1000000);
    }

}
