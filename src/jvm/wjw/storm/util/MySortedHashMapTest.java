package wjw.storm.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

public class MySortedHashMapTest  {
   public static void main(String[] args) {
       HashMap<String,String> hashMap = new HashMap<String, String>();
       Random random = new Random();
       for(int i = 0 ; i < 10; i++) {
           hashMap.put(String.valueOf(3)  + "-" +  String.valueOf(random.nextInt(100)) , String.valueOf(random.nextDouble()));
       }
      // System.out.println(hashMap);
       MySortedHashMap mySortedHashMap = new MySortedHashMap(hashMap);
       //System.out.println(mySortedHashMap.getSortedMap());
       Iterator iterator = mySortedHashMap.getSortedMap().values().iterator();
       while(iterator.hasNext()) {
           System.out.println(iterator.next());
       }
   }
}