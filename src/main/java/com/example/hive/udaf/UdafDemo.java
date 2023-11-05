package com.example.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.LongWritable;

class UdafDemo implements UDAFEvaluator {
    private LongWritable result;

    public void init() {
        result = null;
    }

    private boolean iterator(LongWritable value) {
        if (value == null)
            return false;
        if (result == null)
            result = new LongWritable(value.get());
        else
            result.set(Math.max(result.get(), value.get()));
        return true;
    }

    // hive需要部分比较结果的时候会调用此方法,返回一个封装了当前状态的对象
    public LongWritable terminatePartial() {
        return result;
    }

    // 合并两个部分比较结果时会调用该方法
    public boolean merge(LongWritable other) {
        return iterator(other);
    }

    // hive需要最终比较结果的时候调用该方法
    public LongWritable terminate() {
        return result;
    }
}
