package wjw.storm.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by wjw on 14-10-21.
 */
public class SamplingInfo {

    private double completeTime = 0;
    private int ackedSize = 0;
    private int failedSize = 0;
    private long startTime = 0;
    private String tName = null;
    private long throught = 0;
    private String currentTime =new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss").format((Calendar.getInstance()).getTime());

    public SamplingInfo(double completeTime, int ackedSize, int failedSize, long startTime, String tName, long throught) {
        this.completeTime = completeTime;
        this.ackedSize = ackedSize;
        this.failedSize = failedSize;
        this.startTime = startTime;
        this.tName = tName;
        this.throught = throught;
    }


    public double getCompleteTime() {
        return completeTime;
    }

    public void setCompleteTime(double completeTime) {
        this.completeTime = completeTime;
    }

    public int getAckedSize() {
        return ackedSize;
    }

    public void setAckedSize(int ackedSize) {
        this.ackedSize = ackedSize;
    }

    public int getFailedSize() {
        return failedSize;
    }

    public void setFailedSize(int failedSize) {
        this.failedSize = failedSize;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String gettName() {
        return tName;
    }

    public void settName(String tName) {
        this.tName = tName;
    }

    public long getThrought() {
        return throught;
    }

    public void setThrought(long throught) {
        this.throught = throught;
    }

    public String getSamplingInfo() {
        return "[" + currentTime +  "\t|\t" +tName + "\t|\t" + completeTime + "\t|\t" + ackedSize + "\t|\t"
                + failedSize + "\t|\t" + startTime + "\t|\t" + throught +"]";
    }

    @Override
    public String toString() {
        return getSamplingInfo();
//        return "SamplingInfo{" +
//                "completeTime=" + completeTime +
//                ", ackedSize=" + ackedSize +
//                ", failedSize=" + failedSize +
//                ", startTime=" + startTime +
//                ", tName='" + tName + '\'' +
//                ", throught=" + throught +
//                '}';
    }
}
