package wjw.storm.util;

/**
 * Created by wjw on 10/17/14.
 */
public class MyTuple {
    private String bname = null;
    private String capacity = null;

    @Override
    public String toString() {
        return "MyTuple{" +
                "bname='" + bname + '\'' +
                ", capacity='" + capacity + '\'' +
                '}' + "\n";
    }

    public MyTuple(String bname,String capacity) {
        this.capacity = capacity;
        this.bname = bname;
    }

    public void setCapacity(String capacity) {
        this.capacity = capacity;
    }

    public void setBname(String bname) {
        this.bname = bname;
    }

    public String getBname() {
        return bname;
    }

    public String getCapacity() {
        return capacity;
    }
}
