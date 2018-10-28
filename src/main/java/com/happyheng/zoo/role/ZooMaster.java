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

            // 然后遍历assign节点，找到节点里面子节点为空的，然后将JobMessage的节点设置进去
            List<String> assignNodePathList = zooKeeper.getChildren("/assign", false);
            if (CollectionUtils.isEmpty(assignNodePathList)) {
                LogUtils.printLog("没有可分配任务的assignNode");
                return;
            }

            boolean assignSuccess = false;
            for (String assignNodePath : assignNodePathList) {
                String assignNodeCurrentPath = "/assign/" + assignNodePath;

                // 找到子节点为空的
                List<String> childNodeList = zooKeeper.getChildren(assignNodeCurrentPath, false);

                if (CollectionUtils.isNotEmpty(childNodeList)) {

                    // 如果不为空，说明此slave已分配，那么把它的第一个job打印出来（目前暂时只支持一个slave同时只分配一个job）
                    String firstJobPath = assignNodeCurrentPath + "/" + childNodeList.get(0);
                    LogUtils.printLog("path为  " + assignNodeCurrentPath + " 的已分配，数据为" + new String(zooKeeper.getData(firstJobPath,false, null)));
                } else {

                    // 将jobMessage设置进去，注意应该是添加一个子节点job，然后子节点中的数据就是对应job中的数据的副本
                    // 注意不能让子节点随时去job中取，job可能修改数据
                    String jobAssignPath = assignNodeCurrentPath + "/" + initJobPath;
                    zooKeeper.create(jobAssignPath, JSON.toJSONString(initJobMessage).getBytes(), OPEN_ACL_UNSAFE , CreateMode.PERSISTENT);

                    // zooKeeper.setData(assignNodeCurrentPath, JSON.toJSONString(initJobMessage).getBytes(), -1);
                    LogUtils.printLog("分配jobMessage成功，path为  " + assignNodeCurrentPath
                            + " ，数据为 " + JSON.toJSONString(initJobMessage));
                    assignSuccess = true;

                    break;
                }
            }

            LogUtils.printLog("分配  " + (assignSuccess ? "成功" : "失败"));

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }



    }

}
