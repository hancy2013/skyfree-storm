package com.skyfree.topology;

import backtype.storm.Config;

// 这是提交到远程拓扑的类
import backtype.storm.StormSubmitter;

// 这是本地提交拓扑的类
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.skyfree.bolt.StormBolt;
import com.skyfree.spout.StormSpout;

public class StormTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("storm_spout", new StormSpout(), 2);
        builder.setBolt("storm_bolt", new StormBolt(), 2).shuffleGrouping("storm_spout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);
        System.out.println("****************");
        System.out.println(args[0]);
        if (args[0].equals("local")) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm_topology", conf, builder.createTopology());
            try {
                Thread.sleep(200000);
            } catch (InterruptedException e) {
                System.out.println("Thread interrupted exception:" + e);
            }
            cluster.killTopology("storm_topology");
            cluster.shutdown();
        } else if (args[0].equals("server")) {
            try {
                StormSubmitter.submitTopology("storm_topology", conf, builder.createTopology());
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }
}
