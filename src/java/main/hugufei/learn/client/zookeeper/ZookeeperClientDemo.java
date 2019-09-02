package hugufei.learn.client.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperClientDemo {

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static ZooKeeper zk;

    private static final String ZOOKEEPER_ZNODE_NAME = "zookeeper";

    //1、开启ZK
    public static void startZK() throws Exception {
        System.out.println("1、startZK begin ----------------------");
        //确保server确实已经开启了，这里是创建client到server的session
        zk = new ZooKeeper("127.0.0.1:2181", 20000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("process " + watchedEvent);
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    countDownLatch.countDown();
                }
            }
        });
        try {
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("state is " + zk.getState());
        System.out.println("1、startZK end----------------------");
        System.out.println();
    }

    //清除全部数据
    public static void clear() throws Exception {
        List<String> childList = zk.getChildren("/", false);
        for(String s : childList) {
            if(s.equals(ZOOKEEPER_ZNODE_NAME))
                continue;
            zk.delete("/" + s, -1);
        }
    }


    //2、同步创建临时节点
    public static void syncCreateNode() {
        System.out.println("2、syncCreateNode begin----------------------");
        try {
            String path1 = zk.create("/test1", "znode1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            String path2 = zk.create("/test2", "znode2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("path1 = " + path1);
            System.out.println("path2 = " + path2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("2、syncCreateNode end----------------------");
        System.out.println();
    }

    //3、异步创建临时节点，这里等待它执行完看processResult
    public static void asnycCreateNode() throws Exception {
        System.out.println("3、asnycCreateNode begin----------------------");
        zk.create("/test3", "znode3".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new IStringCallBack(), "context");
        Thread.sleep(2000);
        System.out.println("3、asnycCreateNode end----------------------");
        System.out.println();
    }

    //异步回调
    static class IStringCallBack implements AsyncCallback.StringCallback {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            System.out.println("3、Create path result " + rc + " " + path + " " + ctx + " " + name);
        }
    }

    //4、获取子节点列表，在子节点列表变更时触发
    public static void getChildren() throws Exception {
        System.out.println();
        System.out.println("4、getChildren----------------------");
        List<String> childList = zk.getChildren("/", new Watcher() {
            @Override
            //这个znode的子节点变化的时候会收到通知
            public void process(WatchedEvent watchedEvent) {
                System.out.println("getChildren " + watchedEvent);
            }
        });
        System.out.println("childList " + childList);
        System.out.println("4、getChildren end----------------------");
    }

    //5、节点是否存在，watch监听节点的创建，删除以及更新
    public static void exists() throws Exception {
        System.out.println();
        System.out.println("5、exists----------------------");
        Stat stat = zk.exists("/test2", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("exists " + watchedEvent);
            }
        });
        System.out.println("stat is " + stat);
        System.out.println("5、exists end----------------------");
    }

    // 6、获取数据，注册监听器，在znode内容被改变时触发
    public static void getData() throws Exception {
        System.out.println();
        System.out.println("6、getData----------------------");
        String ans1 = new String(zk.getData("/test1", false, null));
        String ans2 = new String(zk.getData("/test2", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("getData " + watchedEvent);
            }
        }, null));
        System.out.println("znode /test1 content is " + ans1);
        System.out.println("znode /test2 content is " + ans2);
        System.out.println("6、getData end----------------------");
    }

    //7、更新内容，会触发对应znode的watch事件
    public static void setData() throws Exception {
        System.out.println();
        System.out.println("7、setData----------------------");
        String data = "zNode22";
        zk.setData("/test2", data.getBytes(), -1);
        String ans2 = new String(zk.getData("/test2", false, null));
        System.out.println("setData to " + ans2);
        System.out.println("7、setData end----------------------");
    }

    //8、删除
    public static void delete() throws Exception {
        System.out.println();
        System.out.println("8、delete----------------------");
        zk.delete("/test2", -1);
        zk.delete("/test1", -1);
        System.out.println("8、delete end----------------------");
    }


    public static void main(String[] args) throws Exception {
        startZK();
        clear();
        syncCreateNode();
        asnycCreateNode();
        getChildren();
        exists();
        getData();
        setData();
        delete();
    }


}
