package com.happyheng.utils;

/**
 *
 * Created by happyheng on 2018/9/5.
 */
public class LogUtils {

    public static void printLog(String log) {
        System.out.println("当前线程为 " + Thread.currentThread().getName() + "   " + log);
    }

}
