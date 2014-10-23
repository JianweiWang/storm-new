package wjw.storm.util;

import java.text.SimpleDateFormat;
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

    public static long sum(Collection<Long> collection) {
        int size = collection.size();
        long sum = 0;
        Iterator iterator = collection.iterator();
        while(iterator.hasNext()) {
            sum = sum + (Long) iterator.next();
        }
        return sum;
    }

    public static String getString(HashMap h) {
        String result = "[";
        Iterator<String> iterator = h.keySet().iterator();
        while(iterator.hasNext()) {
            String key = iterator.next();
            result += ("\t|\t" + key + "\t|\t" + h.get(key));
        }
        result += "\t]";
        return result;
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
    public static String getTimeSecond() {
        return new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format((Calendar.getInstance()).getTime());
    }

    public static String getTimeDate() {
        return new SimpleDateFormat("yyyy-MM-dd").format((Calendar.getInstance()).getTime());
    }



	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}



}
