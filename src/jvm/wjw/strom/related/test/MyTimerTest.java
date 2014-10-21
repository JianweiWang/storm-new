package wjw.strom.related.test;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created with IntelliJ IDEA.
 * User: wjw
 * Date: 14-10-10
 * Time: 下午9:16
 */
public class MyTimerTest  {

    public static void main(String[] args) {
        final MyCounter myCounter = new MyCounter();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    myCounter.add();
                    System.out.println("thread1 :" + myCounter.get_count());
                }
                }

        });

        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(3000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    //myCounter.add();
                    System.out.println("thread2 :" + myCounter.get_count());
                }
            }

        });
       // thread.start();
        //thread1.start();
        Timer timer = new Timer();
        MyTask myTask = new MyTask(myCounter);
        timer.schedule(myTask,2000,3000);
    }
}

class  MyTask extends TimerTask {
    MyCounter myCounter = null;
    public MyTask(MyCounter myCounter) {
        this.myCounter = myCounter;
    }
    @Override
    public void run() {
        (new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("hello");
            }
        })).start();
    }
}

class MyCounter {
    int count = 0;
    synchronized void add() {
        count ++;
    }

    synchronized  int get_count() {
        return count;
    }
}