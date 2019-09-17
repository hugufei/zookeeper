package hugufei.learn.distributed_lock;

import hugufei.learn.distributed_lock.curator.DistributedLockWithCurator;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class DistributedLockWithZKDemo {

    private static final AtomicInteger success = new AtomicInteger();
    private static final AtomicInteger fail = new AtomicInteger();

    public static class ReduceStock implements Runnable{

        private final Stock stock;
        private final CountDownLatch countDownLatch;

        public ReduceStock(Stock stock,CountDownLatch countDownLatch) {
            this.stock = stock;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                countDownLatch.await();
                //DistributedLockWithZK lock = new DistributedLockWithZK();
                DistributedLockWithCurator lock = new DistributedLockWithCurator();
                lock.lock();
                boolean result = stock.reduceStock();
                lock.unlock();
                if (result) {
                    success.incrementAndGet();
                } else {
                    fail.incrementAndGet();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, IOException, InterruptedException {
        while(true){
            System.out.println("begin..................");
            success.set(0);
            fail.set(0);
            CountDownLatch countDownLatch = new CountDownLatch(10);
            Stock stock = new Stock();
            for (int i = 0; i < 10; i++) {
                new Thread(new ReduceStock(stock, countDownLatch),"Thread-"+i).start();
                countDownLatch.countDown();
            }
            Thread.sleep(2000);
            System.out.println("成功数:"+success.get());
            System.out.println("失败数:"+fail.get());
            if(success.get()==2){
                continue;
            }else{
                break;
            }
        }
    }
}
