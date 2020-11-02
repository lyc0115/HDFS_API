package com.lyc.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Hadoop3.1.3
 * HDFS API 操作
 * @author lyc
 * @create 2020--11--01 16:44
 */
public class HdfsUtils {
    private static FileSystem fileSystem;
    static {
        try {
            //URI:统一资源定位符，确定你要访问的hdfs， 8020：filesystem默认端口号 hadoop：以哪个用户登录
            fileSystem = FileSystem.get(URI.create("hdfs://hadoop106:8020"), new Configuration(), "hadoop");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取文件
     * @param path 要读取文件的路径
     * @param filePath 读取后写入本地的文件路径
     * @return
     * @throws IOException
     */
    public static boolean readFile(String path, String filePath) throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(path));

        FileOutputStream fileOutputStream = new FileOutputStream(filePath);
        IOUtils.copyBytes(fsDataInputStream,fileOutputStream,1024,false);
        IOUtils.closeStream(fsDataInputStream);
        fileOutputStream.close();
        return true;
    }

    /**
     * 删除文件
     * @param path  指定删除文件路径
     * @return
     * @throws IOException
     */
    public static boolean deleteFile(String path) throws IOException {
        boolean flag = fileSystem.delete(new Path(path), false);
        return flag;
    }

    /**
     * 删除目录
     * @param path 指定删除文件夹路径
     * @return
     * @throws IOException
     */
    public static boolean deleteDir(String path) throws IOException {
        //当第二个参数为true时，递归删除
        boolean flag = fileSystem.delete(new Path(path), true);
        return flag;
    }

    /**
     * 上传文件
     * @param sourcePath  上传源文件路径
     * @param targetPath  上传至目标路径
     * @return
     * @throws IOException
     */
    public static boolean uploadFile(String sourcePath, String targetPath) throws IOException {
        fileSystem.copyFromLocalFile(new Path(sourcePath), new Path(targetPath));
        return true;
    }

    /**
     * 创建文件
     * @param context   文件内容
     * @param path      指定创建文件的路径
     * @return
     * @throws IOException
     */
    public static boolean createFile(String context, String path) throws IOException {
        FSDataOutputStream outputStream = fileSystem.create(new Path(path));
        byte[] buff = context.getBytes();
        outputStream.write(buff, 0, buff.length);
        return true;
    }

    /**
     * 创建目录
     * @param path  指定创建文件夹的路径
     * @return
     * @throws IOException
     */
    public static boolean createDir(String path) throws IOException {
        boolean flag = fileSystem.mkdirs(new Path(path));
        return flag;
    }

    /**
     * 文件重命名
     * @param sourceName  源文件路径
     * @param targetName  修改后文件路径
     * @return
     * @throws IOException
     */
    public static boolean fileRename(String sourceName, String targetName) throws IOException {
        boolean flag = fileSystem.rename(new Path(sourceName), new Path(targetName));
        return flag;
    }


    /**
     * 判断文件是否存在
     * @param path 指定文件路径
     * @return
     * @throws IOException
     */
    public static boolean isFileExsits(String path) throws IOException {
        boolean flag = fileSystem.exists(new Path(path));
        return flag;
    }

    /**
     * 获取文件的最后修改时间
     * @param path 指定文件的路径
     * @return
     * @throws IOException
     */
    public static long fileLastModified(String path) throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(path));
        long modificationTime = fileStatus.getModificationTime();
        return modificationTime;
    }

    /**
     *获取文件的存储位置
     * @param path 指定文件路径
     * @return
     * @throws IOException
     */
    public static HashMap<String, ArrayList<String>> fileLocation(String path) throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(path));
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
        ArrayList<String> list = new ArrayList<String>();
        int i = 0;
        String indexName = "block_";
        String name = null;
        for (BlockLocation blockLocation : blockLocations) {
            String[] hosts = blockLocation.getHosts();
            name = indexName + i;
            for (String host : hosts) {
                list.add(host);
            }
            map.put(name,list);
            i++;
        }
        return map;
    }


    /**
     * 获取文件节点信息
     * @return
     * @throws IOException
     */
    public static HashMap<String, String> nodeList() throws IOException {
        DistributedFileSystem hdfs = (DistributedFileSystem) HdfsUtils.fileSystem;
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
        String indexName = "DataNode_";
        String lastName = "_Name";
        int i = 0;
        String name = null;
        HashMap<String, String> map = new HashMap<String, String>();
        for (DatanodeInfo dataNodeStat : dataNodeStats) {
            name = indexName + i + lastName;
            map.put(name, dataNodeStat.getHostName());
            i++;
        }
        return map;
    }


    /**
     * 关闭流
     * @throws IOException
     */
    public static void close() throws IOException {
        fileSystem.close();
    }

}
