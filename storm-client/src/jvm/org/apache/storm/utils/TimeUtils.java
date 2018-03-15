package org.apache.storm.utils;

/**
 * locate org.apache.storm.utils
 * Created by mastertj on 2018/3/15.
 */
public class TimeUtils {
    public static void waitForTimeMills(long timeMills){
        if(timeMills!=0) {
            Long startTimeMllls = System.currentTimeMillis();
            while (true) {
                Long endTimeMills = System.currentTimeMillis();
                if (endTimeMills - startTimeMllls >= timeMills)
                    break;
            }
        }
    }
}
