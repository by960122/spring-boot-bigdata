package com.example.hadoop.yarn;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Tool tool;
        if ("wordcount".equals(args[0])) {
            tool = new WordCount();
        } else {
            throw new RuntimeException("no such tool " + args[0]);
        }
        int run = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));
        System.exit(run);
    }
}
