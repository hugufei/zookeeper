package hugufei.learn.client.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class TestWatcher {

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 20000,null);

        //测试Watcher机制
        String ans2 = new String(zk.getData("/testWatcher", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("testWatcher " + watchedEvent);
            }
        }, null));

    }

}
