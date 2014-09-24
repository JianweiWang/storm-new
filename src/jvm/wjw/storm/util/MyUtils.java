package wjw.storm.util;

import java.util.Collection;
import java.util.Iterator;

public class MyUtils {

	/**
	 * @param args
	 */
	public static double average(Collection<Double> c) {
		int size = c.size();
		double average ;
		double sum = 0;
		Iterator iter = c.iterator();
		while(iter.hasNext()) {
			sum = sum +((double)iter.next());
		}
		average = sum / size;
		return average;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
