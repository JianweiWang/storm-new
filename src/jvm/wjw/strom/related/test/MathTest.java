package wjw.strom.related.test;

import wjw.storm.util.StormMonitor;

import java.lang.Long;
import java.lang.Math;
import java.util.Date;


public class MathTest {
    public static void main(String[] args) {
        /*Long start = System.nanoTime();

        for(int i = 10; i > 1; i--) {
            Math.atan(i);
        }
        Long finish = System.nanoTime();
        System.out.println(finish - start + "ns");*/
//        int a = new StormMonitor().getExecutorNum("wckafka");
//        int b = new StormMonitor().getWorkerNum("wckafka");
//        System.out.println(a);
//        System.out.println(b);
        String time_seconds = "1001445";
        String s = "NWPU";
        char a = '1';
        long seconds = (Long.valueOf(time_seconds.substring(0,1))) * 24 * 3600
                +
                Long.valueOf(time_seconds.substring(1,3)) * 3600
                + Long.valueOf(time_seconds.substring(3,5)) * 60 + Long.valueOf(time_seconds.substring(5,7));
        System.out.println(seconds);
    }
}