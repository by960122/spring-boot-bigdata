package com.example.hadoop.mapreduce.application;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description: 用户: 该用户拥有的好友们 user1: frined1,friend2,friend3…… user2: frined1,friend2,friend3…… user3:
 *               frined1,friend2,friend3…… user4: frined1,friend2,friend3…… ……要求传出的格式为: 两个用户: 两个用户的共同好友 user1-user2:
 *               friend1,friend2,friend3…… user1-user3: friend1,friend2,friend3…… user1-user4: friend1,friend2,friend3……
 *               user2-user3: friend1,friend2,friend3…… user2-user4: friend1,friend2,friend3…… user3-user4:
 *               friend1,friend2,friend3…… ……我们可以先将传入数据格式转换成: 所有用户的好友们: 拥有该好友的用户 friend1: user1,user2,user3,user4……
 *               friend2: user1,user2,user3,user4…… friend3: user1,user2,user3,user4…… ……再转换成: 两个用户: 两个用户的共同好友
 *               user1-user2: friend1,friend2,friend3…… user1-user3: friend1,friend2,friend3…… user1-user4:
 *               friend1,friend2,friend3…… user2-user3: friend1,friend2,friend3…… user2-user4: friend1,friend2,friend3……
 *               user3-user4: friend1,friend2,friend3……
 */
public class FindFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
        // 传进来的 value: A:B,C,D,F,E,O
        // 将 value 转换成字符串,line 表示行数据.为"A:B,C,D,F,E,O"
        // 分割字符串,得到用户和好友们.用userAndFriends表示.为{"A","B,C,D,F,E,O"}
        String line = value.toString();
        String[] userAndFriends = line.split(":");
        // userAndFriends 0 位置为用户.user 为"A"
        String user = userAndFriends[0];
        // userAndFriends 1 位置为好友们.这里以","分割分割字符串.得到每一个好友 friend 为{"B","C","D","F","E","O"}
        String[] friends = userAndFriends[1].split(",");
        // 循环遍历.以<B,A>,<C,A>,<D,A>......的形式传给 reducer
        for (String friend : friends) {
            context.write(new Text(friend), new Text(user));
        }
    }
}
