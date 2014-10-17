package wjw.storm.util;

import java.util.*;

public class MyUtils {


	public static double average(Collection<Double> c) {
		int size = c.size();
		double average ;
		double sum = 0;
		Iterator iter = c.iterator();
		while(iter.hasNext()) {
			sum = sum +((Double)iter.next());
		}
		average = sum / size;
		return average;
	}

    public static void sortHashMap(List resultList, HashMap sourceMap) {
        List tmpList = new ArrayList(sourceMap.entrySet());
        HashMap tmpMap = new HashMap();
        List tnameList = new ArrayList();
        List bnameList = new ArrayList();
        for(Object o : sourceMap.keySet()) {
            String[] strs = ((String) o).split("-");
            tmpList.add(strs[0]);
            tmpList.add(strs[1]);
        }

        Collections.sort(tmpList,new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {

                return 0;
            }
        });

    }


	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}



}
