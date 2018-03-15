package org.apache.storm.utils;

/**
 * locate org.apache.storm.utils
 * Created by mastertj on 2018/3/15.
 */
public class TimeUtils {
    public static void waitForTimeNanos(long timeNanos){
        if(timeNanos!=0) {
            Long startTimeNanos = System.nanoTime();
            while (true) {
                Long endTimeNanos = System.nanoTime();
                if ((endTimeNanos - startTimeNanos) >= timeNanos)
                    break;
            }
        }
    }
}
