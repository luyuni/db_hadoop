package com.luyunix.hdfs;

import com.vdian.bigdata.guoyu.Kerb;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Java代码操作HDFS
 * 文件操作：上传文件，下载文件，删除文件
 *
 */
public class HdfsOp {
    static FileSystem getFileSystem() {

        //获取操作HDFS的对象
        FileSystem fileSystem = null;
        try {
            //创建一个配置对象
            Configuration conf = new Configuration();
            //指定HDFS的地址
            conf.set("fs.defaultFS","hdfs://bigdata01:9000");
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileSystem;
    }

    /**
     * 删除文件或者目录
     * @param fileSystem
     * @throws IOException
     */
    private static void delete(FileSystem fileSystem) throws IOException {
        //删除文件，目录也可以删除
        //如果要递归删除目录，则第二个参数需要设置为true
        //如果删除的是文件或者空目录，第二个参数会被忽略
        boolean flag = fileSystem.delete(new Path("/LICENSE.txt"),true);
        if(flag){
            System.out.println("删除成功!");
        }else{
            System.out.println("删除失败!");
        }
    }
    public static boolean download(String hdfsPath, String localPath) {
        if (StringUtils.isBlank(hdfsPath) || StringUtils.isBlank(localPath)) {
            return false;
        }
        return copyToLocal(false, hdfsPath, localPath, true);
    }
    private static boolean copyToLocal(boolean delSrc, String hdfsPath, String localPath, boolean useRawLocalFileSystem) {
        if (StringUtils.isBlank(hdfsPath) || StringUtils.isBlank(localPath)) {
            return false;
        }

        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            fileSystem.copyToLocalFile(delSrc, new Path(hdfsPath), new Path(localPath), useRawLocalFileSystem);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }
    public static boolean upload(String source, String dist){
        FileSystem fileSystem = getFileSystem();
        Path sourcePath = new Path(source);
        Path distPath = new Path(dist);
        try {
            fileSystem.copyFromLocalFile(sourcePath, distPath);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
    public static FileStatus getLastestFileStatus(String path) {
        FileSystem fileSystem = getFileSystem();
        FileStatus[] fileStatuses = null;
        List<FileStatus> dirs = new ArrayList<>();
        try {
            fileStatuses = fileSystem.listStatus(new Path(path));
            for (int i = 0; i < fileStatuses.length; i++) {
                if (fileStatuses[i].isDirectory()) {
                    dirs.add(fileStatuses[i]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dirs.get(dirs.size() - 1);
    }

    public static boolean copy(String srcPath, String dstPath) {
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();
            return FileUtil.copy(fileSystem, new Path(srcPath), fileSystem, new Path(dstPath), false, true, fileSystem.getConf());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) {
        FileStatus lastestPath = getLastestFileStatus("/user/maybach/niyulu/xgbtest");
        System.out.println("lastest diris " + lastestPath);
    }
}
