package com.example.hadoop.mapreduce.application;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description:
 */
public class FindFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text> {
    protected void reduce(Text user_user, Iterable<Text> friends, Context context)
        throws IOException, InterruptedException {
        // 传进来的数据 <用户1- 用户2,好友们>
        StringBuilder stringBuffer = new StringBuilder();
        // 遍历所有的好友,并将这些好友放在 stringBuffer 中,以" "分隔
        for (Text friend : friends) {
            stringBuffer.append(friend).append(" ");
        }
        // 以好友为 key,用户们为 value 传给下一个 mapper
        context.write(user_user, new Text(stringBuffer.toString()));
    }
}
