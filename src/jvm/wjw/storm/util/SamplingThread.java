package wjw.storm.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.Utils;

public class SamplingThread implements Runnable{
	private HashMap<String,MyConcurrentQueue> map ;
	public SamplingThread(HashMap<String,MyConcurrentQueue> map) {
		this.map = map;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	@Override
	public void run() {
		while(true) {
			Utils.sleep(5000);
			StormMonitor sm = new StormMonitor();
			List<TopologyInfo> t_list = sm.getTopology();
			Iterator<TopologyInfo> t_iter = t_list.iterator();
			TopologyInfo topology;
			Iterator<ExecutorSummary> e_iter;
			ExecutorSummary executor;			
			String tname,bname;
			double bProcessTime = 0;
			
			while(t_iter.hasNext()) {
				topology = t_iter.next();
				tname = topology.get_name();
				e_iter = topology.get_executors_iterator();
				MyConcurrentQueue queue = map.get(tname);
				while(e_iter.hasNext()) {
					executor = e_iter.next();
					bname = executor.get_component_id();
					if(executor.get_stats().get_specific().is_set_bolt())
					{
						
						bProcessTime = MyUtils.average(executor.get_stats().get_specific().get_bolt().get_process_ms_avg().get("600").values());
						if(queue != null) {
							queue.add(bProcessTime);
						} else {
							queue = new MyConcurrentQueue();
							queue.add(bProcessTime);
							map.put(tname, queue);
						}
					}
					
				}			
			}
            sm = null;
		}
	}

}
