package org.apache.storm.util;

/**
 * locate org.apache.storm.utils
 * Created by mastertj on 2018/3/15.
 */
public class TimeUtils {
    /**
     * waitForTimeMills等待毫秒
     * @param timeMills
     */
    public static void waitForTimeMills(long timeMills){
        if(timeMills!=0) {
            Long startTimeMllls = System.currentTimeMillis();
            while (true) {
                Long endTimeMills = System.currentTimeMillis();
                if ((endTimeMills - startTimeMllls) >= timeMills)
                    break;
            }
        }
    }

    /**
     * waitForNanos 等待纳秒
     * @param nanoTime
     */
    public static void waitForNanos(long nanoTime){
        if(nanoTime!=0) {
            Long startTimeMllls = System.nanoTime();
            while (true) {
                Long endTimeMills = System.nanoTime();
                if ((endTimeMills - startTimeMllls) >= nanoTime)
                    break;
            }
        }
    }

}
