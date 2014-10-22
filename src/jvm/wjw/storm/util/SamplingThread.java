package wjw.storm.util;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.Utils;

import javax.swing.text.DateFormatter;

public class SamplingThread {
   // private HashMap<String, MyConcurrentQueue> map = null;
    private final  Date date = new Date();
    private   String filePath = null;
    StormMonitor sm = null;
    public SamplingThread(StormMonitor sm) {
        this.sm = sm;
    }

    public static void main(String[] args)  {
        // TODO Auto-generated method stub
        HashMap<String, MyConcurrentQueue> myConcurrentQueueHashMap = new HashMap<String, MyConcurrentQueue>();
        SamplingThread samplingThread = new SamplingThread(new StormMonitor());
        samplingThread.sample();
//        File file = new File("/home/wjw/test.txt");
//        file.createNewFile();
       // System.out.println(new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss").format((Calendar.getInstance()).getTime()));
       // new Thread(samplingThread).start();
        //Utils.sleep(5000L);
//        while (true) {
//            Utils.sleep(3000L);
//            System.out.println(myConcurrentQueueHashMap);
//        }

    }


    //@Override
    public void sample() {
       filePath = "/home/wjw/storm/experiment/experiment_result/" +
                new SimpleDateFormat("yyyy-MM-dd").format((Calendar.getInstance()).getTime()) +".txt";
        //System.out.println(new Date());
        FilePrinter filePrinter = new FilePrinter(filePath);
//        try {
//            filePrinter.print(new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss").format((Calendar.getInstance()).getTime()) + "\n");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
       // while (true) {
       //     Utils.sleep(5000);
       //     StormMonitor sm = new StormMonitor();
            List<TopologyInfo> t_list = sm.getTopology();
            Iterator<TopologyInfo> t_iter = t_list.iterator();
            TopologyInfo topology;
            Iterator<ExecutorSummary> e_iter = null;
            ExecutorSummary executor;
            String tname, bname;


            while (t_iter.hasNext()) {
                HashMap<String,Double> capacityMap = new HashMap<String, Double>();
                double completeTime = 0;
                int ackedSize = 0;
                int ackedSize_600 =0;
                int failedSize = 0;
                int throughput = 0;
                long startTime = 0;
                SamplingInfo samplingInfo = null;
                topology = t_iter.next();
                tname = topology.get_name();
                e_iter = topology.get_executors_iterator();
                startTime = topology.get_uptime_secs();
                int workload_600 = 0;
                //MyConcurrentQueue queue = map.get(tname);

                try {
                    filePrinter.print("\n");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                while (e_iter.hasNext()) {
                    //startTime = topology.get_uptime_secs();
                    executor = e_iter.next();
                    startTime= executor.get_uptime_secs();
                    bname = executor.get_component_id();
                    if (executor.get_stats().get_specific().is_set_spout()) {

                        completeTime = MyUtils.average(executor.get_stats().get_specific().get_spout().get_complete_ms_avg().get("600").values());
                        ackedSize += (int) MyUtils.sum(executor.get_stats().get_specific().get_spout().get_acked().get(":all-time").values());
                        failedSize += (int) MyUtils.sum(executor.get_stats().get_specific().get_spout().get_failed().get(":all-time").values());
                        ackedSize_600 += (int) MyUtils.sum(executor.get_stats().get_specific().get_spout().get_acked().get("600").values());
                        workload_600 = (int) MyUtils.sum(executor.get_stats().get_emitted().get("600").values());


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
                System.out.println(ackedSize_600);

                if(startTime > 600) {
                    throughput = ackedSize_600 / 600;
                    workload_600 = workload_600 / 600;
                } else {
                    throughput = (int) (ackedSize_600 / startTime);
                    workload_600 = (int) (workload_600 /startTime);
                }
                samplingInfo = new SamplingInfo(completeTime,ackedSize,failedSize,startTime,tname,throughput,workload_600);
                System.out.println(samplingInfo);
                try {
                    filePrinter.print(samplingInfo);
                } catch (Exception e) {
                    e.printStackTrace();
                }
           // }

        }
        sm = null;
    }


}
