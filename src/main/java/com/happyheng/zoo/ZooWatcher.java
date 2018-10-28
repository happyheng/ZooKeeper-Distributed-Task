package com.happyheng.zoo;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 *
 * Created by happyheng on 2018/8/24.
 */
public class ZooWatcher implements Watcher{

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }
}
