package com.happyheng.zoo.support;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

/**
 * 能提供loop循环监听的方法，因为zooKeeper默认都是只监听一次
 * Created by happyheng on 2018/10/2.
 */
public class ZooSupport {


    /**
     * 循环监听Get的方法
     * @param zooKeeper
     * @param path
     * @param watcher
     * @param cb
     * @param ctx
     */
    public static void loopGet(ZooKeeper zooKeeper, final String path, Watcher watcher,
                        AsyncCallback.DataCallback cb, Object ctx) {
        zooKeeper.getData(path, event -> {
            if (watcher != null) {
                watcher.process(event);
            }

            loopGet(zooKeeper, path, watcher, cb, ctx);
        }, cb, ctx);
    }


    public static void loopGetChild(ZooKeeper zooKeeper,final String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {

        zooKeeper.getChildren(path, event -> {
            if (watcher != null) {
                watcher.process(event);
            }

            try {
                loopGetChild(zooKeeper, path, watcher, stat);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, stat);

    }

}
