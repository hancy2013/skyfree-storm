package com.skyfree.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class StormBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1L;

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String site = tuple.getStringByField("site");
        System.out.println("==========================Name of input site is:" + site);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
