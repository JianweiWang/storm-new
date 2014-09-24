package wjw.storm.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.TopologyInfo;

public class MainMonitor {
	
	public void getBoltInfo(TopologyBoltProcessTimeHashMap tbpth) {
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
			while(e_iter.hasNext()) {
				executor = e_iter.next();
				bname = executor.get_component_id();
				if(executor.get_stats().get_specific().is_set_bolt())
				{
					bProcessTime = MyUtils.average(executor.get_stats().get_specific().get_bolt().get_process_ms_avg().get("600").values());
					tbpth.put(tname, new BoltProcessTime(bname,bProcessTime));
				}
				
			}			
		}		
	}
	public static void main(String[] args) {
		String tname = "wordcount1000tps";
		TopologyBoltProcessTimeHashMap tbpth = new TopologyBoltProcessTimeHashMap();
		MainMonitor mm = new MainMonitor();
		mm.getBoltInfo(tbpth);
		try {
			System.out.println(tbpth.getMinTimeBolt(tname));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(tbpth.getTBMap());
		System.out.println(tbpth.getBoltParallisms("wordcount1000tps"));
	}

}
