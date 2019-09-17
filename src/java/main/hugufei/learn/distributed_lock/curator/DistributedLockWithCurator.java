package hugufei.learn.distributed_lock.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class DistributedLockWithCurator {

    private CuratorFramework client;
    private InterProcessMutex mutex;

    public DistributedLockWithCurator(){
        rebuild();
    }

    private void rebuild(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        if(client!=null){
            client.close();
        }
        client = CuratorFrameworkFactory.newClient("localhost:2181",retryPolicy);
        client.start();
        mutex = new InterProcessMutex(client,"/curator/lock");
    }

    public void lock() {
        while(true){
            try {
                mutex.acquire();
                return;
            }catch (Exception e){
                rebuild();
            }
        }
    }

    public void unlock() {
        while(true){
            try {
                mutex.release();
                return;
            }catch (Exception e){
                rebuild();
            }
        }
    }

}
