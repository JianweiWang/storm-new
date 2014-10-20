package wjw.strom.related.test;

import java.lang.Long;
import java.lang.Math;


public class MathTest {
    public static void main(String[] args) {
        Long start = System.nanoTime();

        for(int i = 10; i > 1; i--) {
            Math.atan(i);
        }
        Long finish = System.nanoTime();
        System.out.println(finish - start + "ns");
    }
}