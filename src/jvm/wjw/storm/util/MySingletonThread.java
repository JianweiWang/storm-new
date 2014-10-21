package wjw.storm.util;

/**
 * Created by wjw on 14-10-21.
 */
public class MySingletonThread {
    private static Thread thread = null;
//    private MySingletonThread (Runnable runnable) {
//        thread = new Thread(runnable);
//    }
    public static Thread getThread(Runnable runnable) {
        if(null != thread) {
            return thread;
        } else {
            thread = new Thread(runnable);
            return thread;
        }
    }


}
