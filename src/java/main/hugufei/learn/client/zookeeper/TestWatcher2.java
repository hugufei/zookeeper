package hugufei.learn.client.zookeeper;

import org.apache.zookeeper.*;

public class TestWatcher2 {

    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 20000,  new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
        zk.setData("/testWatcher", "1".getBytes(), -1);
        System.in.read();
    }

}
