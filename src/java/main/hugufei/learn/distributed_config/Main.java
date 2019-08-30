package hugufei.learn.distributed_config;

import java.util.concurrent.TimeUnit;


public class Main {

    public static void main(String[] args) throws InterruptedException {
        Config config = new Config("localhost:2181");

        config.save("timeout", "1");
        for (int i=0; i<100; i++) {
            System.out.println("====="+config.get("timeout"));
            System.out.println("====="+config.get("grade"));
            TimeUnit.SECONDS.sleep(5);
        }
    }
}
