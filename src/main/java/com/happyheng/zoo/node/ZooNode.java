package com.happyheng.zoo.node;

import com.happyheng.zoo.ZooService;
import com.happyheng.zoo.role.ZooMaster;
import com.happyheng.zoo.role.ZooRole;
import com.happyheng.zoo.role.ZooSlave;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 *
 * Created by happyheng on 2018/9/9.
 */
public class ZooNode {


    private ZooKeeper zooKeeper;

    private String masterId;

    public boolean isMaster;

    private ZooRole zooRole;

    public ZooNode() {

        this.zooKeeper = ZooService.getZooKeeper();
        if (this.zooKeeper == null) {
            throw new NullPointerException();
        }
    }

    /**
     * 成为主节点
     */
    public void createMaster() throws InterruptedException{
        Random random = new Random();
        String randomMasterId = Integer.toHexString(random.nextInt()) ;
        masterId = randomMasterId;
        try {
            zooKeeper.create("/master", randomMasterId.getBytes(), OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL);
            isMaster = true;
        } catch (KeeperException.NodeExistsException e) {
            isMaster = false;
        } catch (KeeperException.ConnectionLossException e) {
            // 连接失败异常，我们不知道到底是因为什么异常导致的loss，可能请求已经发过去，我们就是master，所以要检查
            checkMaster();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    private void checkMaster() {
        while (true) {
            try {
                byte[] masterIdBytes = zooKeeper.getData("/master", false, new Stat());
                String currentMasterId = new String(masterIdBytes);
                isMaster = masterId.equals(currentMasterId);
                return;
            } catch (KeeperException.ConnectionLossException e) {
                e.printStackTrace();
            }
            catch (KeeperException e) {
                return;
            }
            catch (InterruptedException e) {
                return;
            }
        }
    }


    public void doJob(){

        if (isMaster) {
            zooRole = new ZooMaster();
        } else {
            zooRole = new ZooSlave();
        }

        zooRole.doJob();
    }


}
