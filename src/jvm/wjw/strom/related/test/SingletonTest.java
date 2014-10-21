package wjw.strom.related.test;

import backtype.storm.utils.Utils;
import wjw.storm.util.MyConcurrentQueue;
import wjw.storm.util.MySingletonThread;
import wjw.storm.util.SamplingThread;

import java.util.HashMap;

/**
 * Created by wjw on 14-10-21.
 */
public class SingletonTest {
    public static void main(String[] args) {
        Thread thread = MySingletonThread.getThread(new SamplingThread());
        Thread thread1 = MySingletonThread.getThread(new SamplingThread());
       // thread.start();
        thread1.stop();
        Utils.sleep(300L);
        System.out.println(thread1.isAlive());
    }
}
