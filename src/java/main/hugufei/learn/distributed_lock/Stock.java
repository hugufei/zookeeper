package hugufei.learn.distributed_lock;

public class Stock {

    private Integer COUNT = 2;

    public boolean reduceStock() {
        if (COUNT > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            COUNT--;
            return true;
        }
        return false;
    }
}
