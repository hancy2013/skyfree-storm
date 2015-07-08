package com.skyfree.monitor;

import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.RebalanceOptions;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/8 15:27
 */
public class TopologyControl {
    public static void kill(String topologyId) {
        try {
            Client client = ThriftClient.getClient();
            client.killTopology(topologyId);
            ThriftClient.close();
        } catch (Exception e) {
            throw new RuntimeException("Error occurred while killing the topology : " + e);
        }
    }

    public static void activate(String topologyId) {
        try {
            Client client = ThriftClient.getClient();
            client.activate(topologyId);
            ThriftClient.close();
        } catch (Exception e) {
            throw new RuntimeException("Error occurred while activate the topology : " + e);
        }
    }

    public static void deactivate(String topologyId) {
        try {
            Client client = ThriftClient.getClient();
            client.deactivate(topologyId);
            ThriftClient.close();
        } catch (Exception e) {
            throw new RuntimeException("Error occurred while deactivate the topology : " + e);
        }
    }

    public static void rebalance(String topologyId, RebalanceOptions options) {
        try {
            Client client = ThriftClient.getClient();
            client.rebalance(topologyId, options);
            ThriftClient.close();
        } catch (Exception e) {
            throw new RuntimeException("Error occurred while deactivate the topology : " + e);
        }
    }

    public static void main(String[] args) {
        TopologyControl.activate("storm_topology");
    }
}
