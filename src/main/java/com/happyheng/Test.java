package com.happyheng;

import com.happyheng.zoo.ZooClient;
import com.happyheng.zoo.ZooService;
import com.happyheng.zoo.role.ZooJob;

/**
 *
 * Created by happyheng on 2018/8/24.
 */
public class Test {

    public static void main(String[] args) {

        // 连接可以放到Spring中
        ZooService service = new ZooService();
        service.startConnect();

        Thread thread1 = new Thread(()->{
            ZooClient zooClient = new ZooClient();
            zooClient.begin();
        });


        Thread thread2 = new Thread(()->{
            ZooClient zooClient = new ZooClient();
            zooClient.begin();
        });

        Thread thread3 = new Thread(()->{
            ZooClient zooClient = new ZooClient();
            zooClient.begin();
        });

        thread1.start();
        thread2.start();
        thread3.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Thread thread4 = new Thread(()->{
            ZooJob job = new ZooJob();
            job.createJobNode();
        });
        thread4.start();


        try {
            // 注意需要让主线程sleep足够长的时间，这样zookeeper线程才能运行，否则会断开连接
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
