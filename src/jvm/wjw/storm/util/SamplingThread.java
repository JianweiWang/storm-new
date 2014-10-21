package wjw.storm.util;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.Utils;

import javax.swing.text.DateFormatter;

public class SamplingThread implements Runnable {
   // private HashMap<String, MyConcurrentQueue> map = null;
    private final  Date date = new Date();
    private  static String filePath = "/home/wjw/storm/experiment/experiment_result/" +
            new SimpleDateFormat("yyyy-MM-dd").format((Calendar.getInstance()).getTime()) +".txt";

    public SamplingThread() {

    }

    public static void main(String[] args)  {
        // TODO Auto-generated method stub
        HashMap<String, MyConcurrentQueue> myConcurrentQueueHashMap = new HashMap<String, MyConcurrentQueue>();
        SamplingThread samplingThread = new SamplingThread();
//        File file = new File("/home/wjw/test.txt");
//        file.createNewFile();
       // System.out.println(new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss").format((Calendar.getInstance()).getTime()));
        new Thread(samplingThread).start();
        //Utils.sleep(5000L);
//        while (true) {
//            Utils.sleep(3000L);
//            System.out.println(myConcurrentQueueHashMap);
//        }

    }


    @Override
    public void run() {

        System.out.println("====================================================sampling......========================");
        FilePrinter filePrinter = new FilePrinter(filePath);
        while (true) {
            Utils.sleep(5000);
            StormMonitor sm = new StormMonitor();
            List<TopologyInfo> t_list = sm.getTopology();
            Iterator<TopologyInfo> t_iter = t_list.iterator();
            TopologyInfo topology;
            Iterator<ExecutorSummary> e_iter = null;
            ExecutorSummary executor;
            String tname, bname;
            double completeTime = 0;
            int ackedSize = 0;
            int failedSize = 0;
            int throughput = 0;
            long startTime = 0;

            while (t_iter.hasNext()) {
                SamplingInfo samplingInfo = null;
                topology = t_iter.next();
                tname = topology.get_name();
                e_iter = topology.get_executors_iterator();
                startTime = topology.get_uptime_secs();
                //MyConcurrentQueue queue = map.get(tname);
                try {
                    filePrinter.print("\n");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                while (e_iter.hasNext()) {
                    startTime = topology.get_uptime_secs();
                    executor = e_iter.next();
                    bname = executor.get_component_id();
                    if (executor.get_stats().get_specific().is_set_spout()) {

                        completeTime = MyUtils.average(executor.get_stats().get_specific().get_spout().get_complete_ms_avg().get("600").values());
                        ackedSize = (int) MyUtils.sum(executor.get_stats().get_specific().get_spout().get_acked().get(":all-time").values());
                        failedSize = (int) MyUtils.sum(executor.get_stats().get_specific().get_spout().get_failed().get(":all-time").values());
                        if(startTime > 600) {
                            throughput = (int) MyUtils.sum(executor.get_stats().get_specific().get_spout().get_acked().get("600").values()) / 600;
                        } else {
                            throughput = (int) ((int) MyUtils.sum(executor.get_stats().get_specific().get_spout().get_acked().get("600").values()) / startTime);
                        }
                        samplingInfo = new SamplingInfo(completeTime,ackedSize,failedSize,startTime,tname,throughput);
                        try {
                            filePrinter.print(samplingInfo);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        //System.out.println(samplingInfo);

//                        if (queue != null) {
//                            queue.add(completeTime);
//                        } else {
//                            queue = new MyConcurrentQueue();
//                            queue.add(completeTime);
//                            //map.put(tname, queue);
//                        }
                    }

                }
            }
            sm = null;
        }
    }

}
