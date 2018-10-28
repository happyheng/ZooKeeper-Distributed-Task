package com.happyheng.zoo.role;

import com.alibaba.fastjson.JSON;
import com.happyheng.utils.LogUtils;
import com.happyheng.zoo.ZooService;
import com.happyheng.zoo.bean.JobMessage;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 *
 * Created by happyheng on 2018/9/25.
 */
public class ZooJob {


    private ZooKeeper zooKeeper;

    /**
     * 创建job节点
     */
    public void createJobNode() {


        LogUtils.printLog("ZooJob 开始");

        zooKeeper = ZooService.getZooKeeper();
        JobMessage message = new JobMessage();
        message.setJobData("testData");
        message.setStatus(JobMessage.StatusEnum.INIT.getValue());

        String messageData = JSON.toJSONString(message);

        try {
            String jobPath = zooKeeper.create("/job/job", messageData.getBytes(), OPEN_ACL_UNSAFE , CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 监听

    }


}
