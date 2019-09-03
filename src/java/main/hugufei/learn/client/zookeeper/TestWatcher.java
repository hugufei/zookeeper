package hugufei.learn.client.zookeeper;

import org.apache.zookeeper.*;

public class TestWatcher {

    public static void main(String[] args) throws Exception {

        ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 20000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });


        //测试Watcher机制
        String ans2 = new String(zk.getData("/testWatcher", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("testWatcher " + watchedEvent);
            }
        }, null));


        System.in.read();
    }

}
