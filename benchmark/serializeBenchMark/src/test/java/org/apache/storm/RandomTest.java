package org.apache.storm;

import org.junit.Test;

import java.util.Random;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/10/11.
 */
public class RandomTest {
    @Test
    public void testRandom(){
        for (int i =0; i < 16; i++) {
            int randomNum = RandomTest.getRandomNum(700, 1300);
            System.out.println(randomNum);
        }
    }

    // 获得一个给定范围的随机整数
    public static int getRandomNum(int smallistNum, int BiggestNum) {
        Random random = new Random();
        return (Math.abs(random.nextInt()) % (BiggestNum - smallistNum + 1))+ smallistNum;
    }
}
