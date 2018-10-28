package com.happyheng.zoo.bean;

/**
 *
 * Created by happyheng on 2018/9/9.
 */
public class JobMessage {

    private String jobData;

    /**
     * @see StatusEnum
     */
    private int status;

    public String getJobData() {
        return jobData;
    }

    public void setJobData(String jobData) {
        this.jobData = jobData;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public enum StatusEnum{

        INIT(0),
        WORKING(1),
        WORK_FINISH(2),
        WORK_ERROR(3);

        private int value;

        StatusEnum(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }
}
