package main;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    public static class ZooKeeperConnection extends Thread{
        private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperConnection.class);
        // declare zookeeper instance to access ZooKeeper ensemble
        private String host;
        private int serial;
        private String dir;
        private int count;

        private ZooKeeper zoo;
        final CountDownLatch connectedSignal = new CountDownLatch(1);

        public ZooKeeperConnection(String _host) {
            super("thread: masterConnection");
            this.host = _host;
            this.dir = null;

            try {
                this.zoo = this.connect();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                this.zoo = null;
            }
        }

        public ZooKeeperConnection(String _host, int _serial, String _dir, int _count) {
            super("slave thread: "+ _serial);
            this.host = _host;
            this.serial = _serial;
            this.dir = _dir;
            this.count = _count;

            try {
                this.zoo = this.connect();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                this.zoo = null;
            }
        }

        @Override
        public void run() {
            if (dir == null) {
                System.out.println("dir is not set.");
            } else {

                for ( int i = 0; i < this.count; i ++) {
                    String path = String.format("%s/%d_%d", this.dir, serial, i);
                    this.create(path, String.valueOf(i).getBytes(), CreateMode.PERSISTENT);

                }
            } // else
        }

        // Method to connect zookeeper ensemble.
        private ZooKeeper connect() throws IOException,InterruptedException {

            zoo = new ZooKeeper(this.host,5000,new Watcher() {

                public void process(WatchedEvent we) {
                    if (we.getState() == Event.KeeperState.SyncConnected) {
                        connectedSignal.countDown();
                    }
                }
            });

            connectedSignal.await();
            return zoo;
        }

        // Method to disconnect from zookeeper server
        private void close() throws InterruptedException {
            zoo.close();
        }

        public boolean dirPreCheck(String dir) {
            Stat result = this.znode_exists(dir);
            if (result == null) {
                return this.create(dir, "homeRoot".getBytes(), CreateMode.PERSISTENT);
            } else {
                return false;
            }
        }

        private boolean create(String path, byte[] data, CreateMode _mode) {
            try {
                zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, _mode);
                System.out.format("%s Node Created: %s by Thread: %s\n", (new Timestamp(System.currentTimeMillis())), path, super.getName());
                return true;
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }

        // Method to check existence of znode and its status, if znode is available.
        private Stat znode_exists(String path) {
            try {
                return zoo.exists(path, true);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }

            return null;
        }

        public void removeRoot(String path) {
            try {
                zoo.delete(path, this.znode_exists(path).getVersion());
                System.out.format("%s Node delete: %s by Thread: %s\n", (new Timestamp(System.currentTimeMillis())), path, super.getName());
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static CommandLine getArguments(String[] args) {
        Options options = new Options();

        Option dir = new Option("d", "dir", true, "name of directory");
        dir.setRequired(true);
        options.addOption(dir);


        Option address = new Option("a", "address", true, "location of the zk server, ex. 127.0.0.1:2181");
        address.setRequired(true);
        options.addOption(address);

        Option num = new Option("n", "num", true, "number of the request");
        num.setRequired(true);
        options.addOption(num);

        Option thread = new Option("t", "thread", true, "Number of threads");
        thread.setRequired(false);
        options.addOption(thread);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }

        return cmd;
    }

    public static void main(String[] args) throws InterruptedException {

        CommandLine cmd = getArguments(args);
        String dir = "/".concat(cmd.getOptionValue("d"));
        String address = cmd.getOptionValue("a");
        int count = 0;
        int threads = 1;
        try {
            count = Integer.parseInt(cmd.getOptionValue("n"));
            threads = Integer.parseInt(cmd.getOptionValue("t", "1"));
        } catch ( NumberFormatException e ) {
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.println(String.format("Create count: %d nodes per thread:%d under /%s at ZK:%s\n", count, threads, dir, address));

        ZooKeeperConnection masterConnection = null;
        ArrayList<ZooKeeperConnection> connectionsPool = new ArrayList<>();
        try {
            masterConnection = new ZooKeeperConnection(address);
            if (!masterConnection.dirPreCheck(dir)) {
                System.out.format("dir: %s pre-check fails\n", dir);
                System.exit(-1);
            }

            connectionsPool = new ArrayList<>();
            for ( int i = 0; i < threads; i ++) {
                ZooKeeperConnection zkSlave = new ZooKeeperConnection(address, i, dir, count);
                zkSlave.start();
                connectionsPool.add(zkSlave);
            }


        } finally {
            for ( ZooKeeperConnection zkCon : connectionsPool) {
                System.out.println("wait for finish ".concat(zkCon.getName()));
                zkCon.join();
                zkCon.close();
                System.out.println("close for ".concat(zkCon.getName()));
            }

            Thread.sleep(45000);

            assert masterConnection != null;
            masterConnection.removeRoot(dir);
            masterConnection.close();

        }










    }
}
