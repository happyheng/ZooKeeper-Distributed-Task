package com.happyheng.zoo;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 *
 * Created by happyheng on 2018/8/24.
 */
public class ZooService {

    private static String zooAdress = "127.0.0.1:2181";
    private static int sessionTimeOut = 15000;

    private static ZooKeeper zooKeeper;

    public void startConnect(){
        try {
            // 同步阻塞方法
            zooKeeper = new ZooKeeper(zooAdress, sessionTimeOut, new ZooWatcher());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ZooKeeper getZooKeeper() {
        return zooKeeper;
    }



    public void disconnect() {
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
