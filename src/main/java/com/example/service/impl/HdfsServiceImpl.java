package com.example.service.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.example.service.HdfsService;
import com.example.tools.TimeTool;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description:
 */
@Service
public class HdfsServiceImpl implements HdfsService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsServiceImpl.class);

    private FileSystem fs;

    public HdfsServiceImpl(FileSystem fs) {
        this.fs = fs;
    }

    @Override
    public boolean mkdir(String hdfsDir) {
        boolean isMkdir = false;
        try {
            isMkdir = fs.mkdirs(new Path(hdfsDir));
        } catch (IOException e) {
            LOGGER.error("hdfd mkdir error: ", e);
        }
        return isMkdir;
    }

    public boolean checkDirExists(String hdfsFullPathName) {
        boolean isExists = false;
        try {
            isExists = fs.exists(new Path(hdfsFullPathName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isExists;
    }

    @Override
    public boolean deleteFile(String hdfsFullPathName) {
        boolean isSuccess = false;
        try {
            isSuccess = fs.delete(new Path(hdfsFullPathName), false);
        } catch (IOException e) {
            LOGGER.error("hdfs delete fime error: ", e);
        }
        return isSuccess;
    }

    @Override
    public List<FileStatus> listPathFiles(String hdfsDir) {
        try {
            FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsDir));
            return Arrays.stream(fileStatuses).collect(Collectors.toList());

        } catch (IOException e) {
            LOGGER.info("hdfs query path files error: ", e);
        }
        return Collections.emptyList();
    }

    @Override
    public boolean rename(String oldFullPathName, String newFullPathName) {
        boolean isSuccess = false;
        try {
            isSuccess = fs.rename(new Path(oldFullPathName), new Path(newFullPathName));
        } catch (IOException e) {
            LOGGER.error("hdfs rename file error: ", e);
        }
        return isSuccess;
    }

    @Override
    public String getFileModifyTime(String hdfsFullPathName) {
        FileStatus fileStatus = null;
        try {
            fileStatus = fs.getFileStatus(new Path(hdfsFullPathName));
        } catch (IOException e) {
            LOGGER.error("hdfs query file modify time error: {}", e);
        }
        return TimeTool.convertStandardTime(fileStatus.getModificationTime());
    }

    @Override
    public void putFile(String localFullPathName, String hdfsFullPathName) {
        try {
            fs.copyFromLocalFile(new Path(localFullPathName), new Path(hdfsFullPathName));
        } catch (IOException e) {
            LOGGER.error("hdfs put file error: {}", e);
        }
    }

    @Override
    public void createFileByContent(String hdfsFullPathName, String content) {
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = fs.create(new Path(hdfsFullPathName));
            fsDataOutputStream.writeUTF(content);
        } catch (IOException e) {
            LOGGER.error("hdfs create file error: ", e);
        } finally {
            try {
                fsDataOutputStream.flush();
                fsDataOutputStream.close();
            } catch (IOException e) {
                LOGGER.error("hdfs create file after close stream error: ", e);
            }
        }
    }
}
