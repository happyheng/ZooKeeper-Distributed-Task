package com.happyheng.zoo.role;

import com.alibaba.fastjson.JSON;
import com.happyheng.utils.LogUtils;
import com.happyheng.zoo.ZooService;
import com.happyheng.zoo.bean.JobMessage;
import com.happyheng.zoo.support.ZooSupport;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 *
 * Created by happyheng on 2018/9/9.
 */
public class ZooSlave implements ZooRole{

    @Override
    public void doJob() {

        ZooKeeper zooKeeper = ZooService.getZooKeeper();

        LogUtils.printLog("doSlaveJob");

        try {
            // 创建slave的顺序节点，并拿到自己节点的名称
            String slaveNodeNameAutclPath = zooKeeper.create("/slave/slave", "".getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            // 根据绝对路径找到自己的路径
            String[] nodePathArray = slaveNodeNameAutclPath.split("[/]");
            String slaveNodeName = nodePathArray[nodePathArray.length - 1];

            //  创建/assign下的节点下的节点，子节点为自己的slave节点名称
            String assignNodeName = zooKeeper.create("/assign/" + slaveNodeName, "".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            ZooSupport.loopGetChild(zooKeeper, assignNodeName,e->{

                // 需要限定必须为子节点更新
                if (Watcher.Event.EventType.NodeChildrenChanged != e.getType()) {
                    return;
                }

                // 获取第一个子节点的全路径以及里面的数据快照
                try {
                    List<String> jobChildNodeList = zooKeeper.getChildren(assignNodeName, false);
                    if (CollectionUtils.isEmpty(jobChildNodeList)) {
                        LogUtils.printLog("assignNodeName ---  job节点为空 "+ assignNodeName)  ;
                    }

                    // 获取里面的数据
                    String jobChildNodePath = assignNodeName + "/" + jobChildNodeList.get(0);
                    byte[] jobMessageByte = zooKeeper.getData(jobChildNodePath, false , null);
                    JobMessage jobMessage = JSON.parseObject(new String(jobMessageByte), JobMessage.class);

                    if (jobMessage == null || JobMessage.StatusEnum.INIT.getValue() != jobMessage.getStatus()) {
                        LogUtils.printLog("状态错误" + jobChildNodePath);
                        return;
                    }

                    // 执行任务之前将状态置为正在执行
                    jobMessage.setStatus(JobMessage.StatusEnum.WORKING.getValue());
                    String message = jobMessage.getJobData();
                    LogUtils.printLog("节点执行的message为 " + message);
                    String jobMessageRunBeforeData = JSON.toJSONString(jobMessage);
                    zooKeeper.setData(jobChildNodePath, jobMessageRunBeforeData.getBytes(), -1);

                    // 开始执行任务
                    Thread.sleep(5000);

                    // 执行任务结束后，将对应节点状态置为成功
                    jobMessage.setStatus(JobMessage.StatusEnum.WORK_FINISH.getValue());
                    String jobMessageRunAfterData = JSON.toJSONString(jobMessage);
                    String jobCurrentPath = "/job/" + jobChildNodeList.get(0);
                    zooKeeper.setData(jobCurrentPath, jobMessageRunAfterData.getBytes(), -1);

                    // 删除自己的子任务节点
                    zooKeeper.delete(jobChildNodePath, -1);

                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

            }, null);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
