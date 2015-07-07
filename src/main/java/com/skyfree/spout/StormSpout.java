package com.skyfree.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class StormSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    private Random random;

    private SpoutOutputCollector spoutOutputCollector;

    // 制造一些数据,模拟数据源
    private static final Map<Integer, String> map = new HashMap<Integer, String>();

    static {
        map.put(0, "google");
        map.put(1, "facebook");
        map.put(2, "twitter");
        map.put(3, "youtube");
        map.put(4, "linkedin");
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 定义数据流,这里一定要进行概念上的区分,这里的数据流,是元组的数据流,所以要定义元组的schema
        outputFieldsDeclarer.declare(new Fields("site"));
    }


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // 一些初始化的操作
        this.spoutOutputCollector = spoutOutputCollector;
        this.random = new Random();
    }


    public void nextTuple() {
        // 不停的释放元组
        this.spoutOutputCollector.emit(new Values(map.get(random.nextInt(5))));
    }
}
