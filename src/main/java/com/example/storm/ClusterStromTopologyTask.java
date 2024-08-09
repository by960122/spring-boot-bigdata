package com.example.storm;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * @author: BYDylan
 * @date: 2023/11/5
 * @description: 设置 exectutor 使用 task 的数量,默认为1
 */
public class ClusterStromTopologyTask {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spoutid", new MySpout());
        // 设置task数,默认为1
        builder.setBolt("bolt-1", new SumBolt()).setNumTasks(2).shuffleGrouping("spoutid");
        StormTopology createTopology = builder.createTopology();
        String topologyName = ClusterStromTopologyTask.class.getSimpleName();
        Config config = new Config();
        try {
            StormSubmitter.submitTopology(topologyName, config, createTopology);
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
            e.printStackTrace();
        }
    }

    public static class MySpout extends BaseRichSpout {

        private static final long serialVersionUID = 1L;
        int i = 1;
        private SpoutOutputCollector collector;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            System.out.println("spout:" + i);
            this.collector.emit(new Values(i++));
            Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }

    }

    public static class SumBolt extends BaseRichBolt {

        private static final long serialVersionUID = 1L;
        int sum = 0;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {}

        @Override
        public void execute(Tuple input) {
            Integer num = input.getIntegerByField("num");
            sum += num;
            System.out.println("和为：" + sum);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    }
}
