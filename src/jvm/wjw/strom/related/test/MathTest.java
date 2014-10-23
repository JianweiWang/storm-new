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
        int a = new StormMonitor().getExecutorNum("wckafka");
        int b = new StormMonitor().getWorkerNum("wckafka");
        System.out.println(a);
        System.out.println(b);
    }
}