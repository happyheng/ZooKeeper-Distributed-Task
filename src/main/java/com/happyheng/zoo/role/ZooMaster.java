package com.happyheng.zoo.role;

import com.alibaba.fastjson.JSON;
import com.happyheng.utils.LogUtils;
import com.happyheng.zoo.ZooService;
import com.happyheng.zoo.bean.JobMessage;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * master相关角色
 * Created by happyheng on 2018/9/9.
 */
public class ZooMaster implements ZooRole{


    private ZooKeeper zooKeeper;

    @Override
    public void doJob() {

        zooKeeper = ZooService.getZooKeeper();

        LogUtils.printLog("doMasterJob");

        // 创建从节点 与 job节点
        try {

            // 初始节点应该先创建好，而不是在master中创建
            //zooKeeper.create("/slave", "".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //zooKeeper.create("/job", "".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            // zooKeeper.create("/assgin", "".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            // 监听job与slave
            zooKeeper.getChildren("/slave", new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    LogUtils.printLog("master /slave watchedEvent --- " + watchedEvent);
                }
            }, new Stat());

            zooKeeper.getChildren("/job", new MasterJobWatcher(), new Stat());

            zooKeeper.getChildren("/assign", new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    LogUtils.printLog("master /assign watchedEvent --- " + watchedEvent);
                }
            }, new Stat());


        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    private class MasterJobWatcher implements Watcher{

        @Override
        public void process(WatchedEvent event) {
            onNodeChangeCircle(event);
        }
    }


    /**
     * 循环监听的方法
     */
    private void onNodeChangeCircle(WatchedEvent event) {

        // 调用回调方法
        onNodeChange(event);

        // 再次监听
        try {
            zooKeeper.getChildren("/job", new MasterJobWatcher(), new Stat());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private void onNodeChange(WatchedEvent event){
        LogUtils.printLog("master /job event --- " + event);

        // 如果有job，那么遍历子节点，看哪个子节点下面没有任务，（遍历这块有待商榷）
        // 那么就给其分配任务


        // 只处理节点数量更新
        LogUtils.printLog("master /job eventType为" + event.getType().name());
        if (Watcher.Event.EventType.NodeChildrenChanged != event.getType()) {
            LogUtils.printLog("master /job 非子节点更新");
            return;
        }


        try {
            // 如果节点数量更新，那么遍历子节点，获取第一个节点内容不为true的job
            List<String> jobPathList = zooKeeper.getChildren("/job", false);
            if (CollectionUtils.isEmpty(jobPathList)) {
                return;
            }

            JobMessage initJobMessage = null;
            String initJobPath = "";
            for (String jobPath : jobPathList) {
                LogUtils.printLog("jobPath --为-- " + jobPath);
                String jobCurrentPath = "/job/" + jobPath;
                byte[] jobDataByteArray = zooKeeper.getData(jobCurrentPath, false, null);
                String jobData = new String(jobDataByteArray);

                if (StringUtils.isEmpty(jobData)) {
                    LogUtils.printLog(jobPath + "  数据为空");
                    continue;
                }

                // 将其转换为 jobMessage ，其中的status为 0 ，也就是待分配的时候，才会分配
                JobMessage jobMessage = JSON.parseObject(jobData, JobMessage.class);
                if (jobMessage == null) {
                    LogUtils.printLog(jobPath + "  temporaryJobMessage为空");
                    continue;
                }

                // 将path设置进去
                if (JobMessage.StatusEnum.INIT.getValue() == jobMessage.getStatus()) {
                    initJobMessage = jobMessage;
                    initJobPath = jobPath;
                    LogUtils.printLog(jobPath + "  对应的job可以分配，其信息为  " + jobData);
                    break;
                }
            }

            if (initJobMessage == null) {
                LogUtils.printLog("没有初始化的jobMessage");
                return;
            }

            // 遍历 /slave 节点，如果对应节点在 /assign 中没有子节点，那么将其分配在/ assgin 中
            List<String> slaveNodeList = zooKeeper.getChildren("/slave", false);
            if (CollectionUtils.isEmpty(slaveNodeList)) {
                LogUtils.printLog("没有可执行的slave");
                return;
            }

            boolean assignSuccess = false;
            for (String slaveNodePath : slaveNodeList) {

                String assignSlaveCurrentPath = "/assign/" + slaveNodePath;

                // 查询其有无子节点，如果没有子节点，说明可以分配给任务
                List<String> assignSlaveChildNodeList = zooKeeper.getChildren(assignSlaveCurrentPath, false);
                if (CollectionUtils.isNotEmpty(assignSlaveChildNodeList)) {
                    LogUtils.printLog("节点路径为 " + assignSlaveCurrentPath + " 的节点已有任务执行");
                    continue;
                }

                // 给assign的对应节点增加一个子节点
                String jobAssignPath = assignSlaveCurrentPath + "/" + initJobPath;
                zooKeeper.create(jobAssignPath, JSON.toJSONString(initJobMessage).getBytes(), OPEN_ACL_UNSAFE , CreateMode.PERSISTENT);

                LogUtils.printLog("分配jobMessage成功，path为  " + jobAssignPath
                        + " ，数据为 " + JSON.toJSONString(initJobMessage));
                assignSuccess = true;
                break;
            }
            LogUtils.printLog("分配  " + (assignSuccess ? "成功" : "失败"));

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }



    }

}
