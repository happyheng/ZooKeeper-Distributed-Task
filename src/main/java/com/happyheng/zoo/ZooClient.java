package com.happyheng.zoo;

import com.happyheng.utils.LogUtils;
import com.happyheng.zoo.node.ZooNode;

/**
 *
 * 此为模拟一个zooKeeper的客户端
 * Created by happyheng on 2018/9/5.
 */
public class ZooClient {


    public void begin() {

        // 创建出对应的zooNode
        ZooNode node = new ZooNode();
        try {
            node.createMaster();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LogUtils.printLog("是否为master " + node.isMaster);

        // 开始执行任务
        node.doJob();
    }

}
