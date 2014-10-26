package wjw.storm.util;

import java.text.SimpleDateFormat;
import java.util.*;

public class MyUtils {


	public static double average(Collection<Double> c) {
		int size = c.size();
        int count = 0;
		Double average = 0.0;
		double sum = 0;
		Iterator iter = c.iterator();
		while(iter.hasNext()) {
            Double tmp = (Double)iter.next();
            if(tmp != null) {
               // System.out.println(tmp);
                System.out.println("average: " + tmp);
                sum = sum +tmp;
                count ++;
            }

		}
        if(count != 0) {
            average = sum / count;
        }

        //if(average != null)

		return average;
	}

    public static long sum(Collection<Long> collection) {
        int size = collection.size();
        long sum = 0;
        int count = 0;
        Iterator iterator = collection.iterator();
        while(iterator.hasNext()) {
            Long tmp = (Long) iterator.next();
            if(null != tmp) {
                sum = sum + tmp;
                count++;
            }

        }
        if(count == 0) {
            return -1;
        } else {
            return sum;
        }

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
