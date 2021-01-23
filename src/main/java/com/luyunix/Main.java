package com.luyunix;

import com.luyunix.hdfs.HdfsOp;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Main {
    static String oldVersion = "";
    static private String path = "/user/maybach/niyulu/xgboost/";
    public static void main(String[] args) throws IOException {
        new Thread(new loadModel()).start();
        System.in.read();
    }
    static String getLastestModelVerison() {
        String pattern = "";
        FileStatus lastestFileStatus = HdfsOp.getLastestFileStatus(path);
        Path path = lastestFileStatus.getPath();
        String pathName = path.getName();
        pattern = pathName.substring(pathName.lastIndexOf("/") + 1);
        return pattern;
    }
    static boolean canLoadModel() {
        return oldVersion.compareTo(getLastestModelVerison()) < 0;
    }
    static class loadModel implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    System.out.println("路径检查 ---- ");
                    if (canLoadModel()) {
                        oldVersion = getLastestModelVerison();
                        System.out.println("有更新了，我在执行加载逻辑，加载的分区是 -》" + oldVersion);
                    }
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
