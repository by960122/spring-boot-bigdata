package com.example.service;

import java.util.List;

import org.apache.hadoop.fs.FileStatus;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description: HDFS 请求demo
 */
public interface HdfsService {

    boolean mkdir(String hdfsDir);

    /**
     * 检查文件是否存在
     *
     * @param hdfsFullPathName 文件名
     * @return true: 存在
     */
    public boolean checkDirExists(String hdfsFullPathName);

    /**
     * 删除文件
     *
     * @param hdfsFullPathName 文件名
     * @return true: 成功
     */
    public boolean deleteFile(String hdfsFullPathName);

    /**
     * 获取路径下所有文件对象
     *
     * @param hdfsDir hdfs 路径
     * @return 对象集合
     */
    List<FileStatus> listPathFiles(String hdfsDir);

    /**
     * 重命名文件
     *
     * @param oldFullPathName 源文件名
     * @param newFullPathName 目标文件名
     * @return true: 成功
     */
    public boolean rename(String oldFullPathName, String newFullPathName);

    /**
     * 获取文件修改时间
     *
     * @param hdfsFullPathName 文件名
     * @return 修改时间
     */
    public String getFileModifyTime(String hdfsFullPathName);

    /**
     * 上传文件
     *
     * @param localFullPathName 本地文件
     * @param hdfsFullPathName 目标路径
     */
    public void putFile(String localFullPathName, String hdfsFullPathName);

    /**
     * 编辑文件
     *
     * @param hdfsFullPathName 文件名
     * @param content 内容
     */
    public void createFileByContent(String hdfsFullPathName, String content);

}