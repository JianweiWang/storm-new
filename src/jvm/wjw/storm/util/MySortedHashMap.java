package wjw.storm.util;

import java.util.*;

/**
 * Created by wjw on 10/17/14.
 */
public class MySortedHashMap {

    private HashMap<String, String> map = null;
    private HashMap<String, List<MyTuple>> sortedMap = new HashMap<String, List<MyTuple>>();

    public MySortedHashMap(HashMap map) {
        this.map = map;
        sort();
    }

    public HashMap getSortedMap() {
        return sortedMap;
    }

    private void prepare() {
        List<MyTuple> tupleList = new ArrayList<MyTuple>();
        for (String key : map.keySet()) {
//            System.out.println(key);
//            System.out.println(map.get(key));
            String tname = ((String) key).split("-")[0];
            String bname = ((String) key).split("-")[1];
   //         System.out.println(bname);
            List<MyTuple> tmpList = null;
            if (sortedMap.containsKey(tname)) {
                tmpList = sortedMap.get(tname);
                tmpList.add(new MyTuple(bname, map.get(key)));
            } else {
                tmpList = new ArrayList<MyTuple>();
                sortedMap.put(tname, tmpList);
                tmpList.add(new MyTuple(bname, map.get(key)));
            }
        }
    }

    private void sort() {
        System.out.println("sort....");
        prepare();
        List<List<MyTuple>> tmpList = new ArrayList<List<MyTuple>>(sortedMap.values()) ;
        for (List list : tmpList) {
            Collections.sort(list, new Comparator() {
                @Override
                public int compare(Object o1, Object o2) {

                    return Double.valueOf(((MyTuple) o2).getCapacity()).compareTo(Double.valueOf(((MyTuple) o1).getCapacity()));
                }
            });
        }

    }


}
