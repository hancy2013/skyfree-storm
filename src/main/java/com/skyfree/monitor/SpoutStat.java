package com.skyfree.monitor;

import backtype.storm.generated.*;
import backtype.storm.generated.Nimbus.Client;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.skyfree.monitor.Utils.printLine;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/8 15:00
 */
public class SpoutStat {
    private static final String DEFAULT = "default";
    private static final String ALL_TIME = ":all-time";

    public static void printSpoutStat(String topologyId) {
        try {
            Client client = ThriftClient.getClient();
            TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);

            Iterator<ExecutorSummary> executorSummaryIterator = topologyInfo.get_executors_iterator();
            while (executorSummaryIterator.hasNext()) {
                ExecutorSummary executorSummary = executorSummaryIterator.next();
                ExecutorStats executorStats = executorSummary.get_stats();
                if (executorStats != null) {                    
                    ExecutorSpecificStats executorSpecificStats = executorStats.get_specific();
                    String componentId = executorSummary.get_component_id();

                    if (executorSpecificStats.is_set_spout()) {
                        printLine();
                        SpoutStats spoutStats = executorSpecificStats.get_spout();
                        System.out.println("component id of spout : " + componentId);
                        System.out.println("transferred : " + getAllTimeStat(executorStats.get_transferred(), ALL_TIME));
                        System.out.println("total tuples emitted : " + getAllTimeStat(executorStats.get_emitted(), ALL_TIME));
                        System.out.println("acked : " + getAllTimeStat(spoutStats.get_acked(), ALL_TIME));
                        System.out.println("failed : " + getAllTimeStat(spoutStats.get_failed(), ALL_TIME));
                        printLine();
                    }                    
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException("Error occurred while fetching the spout information!");
        }
    }

    private static Long getAllTimeStat(Map<String, Map<String, Long>> map, String statName) {
        if (map != null) {
            Long statValue = null;
            Map<String, Long> tempMap = map.get(statName);
            statValue = tempMap.get(DEFAULT);
            return statValue;
        }
        return 0L;
    }

    public static void main(String[] args) throws IOException {
        SpoutStat.printSpoutStat("storm_topology-1-1436326975");
        ThriftClient.close();
    }
}
