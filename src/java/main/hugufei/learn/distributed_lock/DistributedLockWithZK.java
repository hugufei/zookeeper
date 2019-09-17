package hugufei.learn.distributed_lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class DistributedLockWithZK implements Lock, Watcher {

    private ZooKeeper zooKeeper = null;
    private String ROOT_LOCKS = "/LOCK";  //定义根节点
    private String WAIT_LOCK;//等待前一个锁
    private String CURRENT_LOCK; //表示当前的锁
    private CountDownLatch countDownLatch;

    public DistributedLockWithZK() {
        try {
            zooKeeper = new ZooKeeper("localhost:2181", 40000, this);
            //判断根节点是否存在
            Stat stat = zooKeeper.exists(ROOT_LOCKS, false);
            if (stat == null) {
                zooKeeper.create(ROOT_LOCKS, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void lock() {
        try {
            if (this.tryLock()) {
                System.out.println(Thread.currentThread().getName() + ">" + "获得锁成功!");
            }else{
                waitForLock(WAIT_LOCK);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {
        try {
            CURRENT_LOCK = zooKeeper.create(ROOT_LOCKS + "/zk_", "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            //System.out.println(Thread.currentThread().getName() + ">" + CURRENT_LOCK + "," + "尝试竞争锁!!");
            //获得所有节点下面的子节点;
            List<String> childrens = zooKeeper.getChildren(ROOT_LOCKS, false);
            //定义一个集合进行排序
            SortedSet<String> sortedSet = new TreeSet<>();
            for (String children : childrens) {
                sortedSet.add(ROOT_LOCKS + "/" + children);
            }
            //获得当前节点中的最小的子节点;
            String firstNode = sortedSet.first();
            SortedSet<String> lessThenMe = ((TreeSet<String>) sortedSet).headSet(CURRENT_LOCK);
            //通过当前节点与最小节点进行比较 , 如果相等则获取所成功;
            if (CURRENT_LOCK.equals(firstNode)) {
                return true;
            }
            ///获得比当前节点更小的最后一个节点，设置给WAIT_LOCK
            if (!lessThenMe.isEmpty()) {
                WAIT_LOCK = lessThenMe.last();
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    private boolean waitForLock(String prev) throws KeeperException, InterruptedException {
        // 监听上一个节点
        Thread.sleep(1000);
        Stat stat = zooKeeper.exists(prev, true);
        // 如果节点存在，才等待,否则认为获取到了锁
        if (stat != null) {
            //System.out.println(Thread.currentThread().getName() + ">节点为---" + CURRENT_LOCK + ", 等待" + prev + "释放锁!");
            countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
            //todo 当watcher 触发后还需要再次判断当前等待的节点时候是最小的
            System.out.println(Thread.currentThread().getName() + ">被唤醒");
        }else{
            System.out.println(Thread.currentThread().getName() + ">等待的节点不存在了");
        }
        return true;
    }

    @Override
    public void unlock() {
        //System.out.println(Thread.currentThread().getName()+"释放锁"+CURRENT_LOCK);
        try {
            //设置version为-1 不管什么情况都要删除
            zooKeeper.delete(CURRENT_LOCK,-1);
            CURRENT_LOCK=null;
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    @Override
    public void process(WatchedEvent event) {
        if (countDownLatch!=null){
            countDownLatch.countDown();
        }
    }
}
