package com.skyfree.monitor;

import backtype.storm.generated.*;
import backtype.storm.generated.Nimbus.Client;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.skyfree.monitor.Utils.printLine;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/8 15:00
 */
public class BoltStat {
    private static final String DEFAULT = "default";
    private static final String ALL_TIME = ":all-time";

    public static void printBoltStat(String topologyId) {
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

                    if (executorSpecificStats.is_set_bolt()) {
                        printLine();
                        BoltStats boltStats = executorSpecificStats.get_bolt();
                        System.out.println("component id of bolt : " + componentId);
                        System.out.println("transferred : " + getAllTimeStat(executorStats.get_transferred(), ALL_TIME));
                        System.out.println("total tuples emitted : " + getAllTimeStat(executorStats.get_emitted(), ALL_TIME));
                        System.out.println("acked : " + getBoltStats(boltStats.get_acked(), ALL_TIME));
                        System.out.println("failed : " + getBoltStats(boltStats.get_failed(), ALL_TIME));
                        System.out.println("executed : " + getBoltStats(boltStats.get_executed(), ALL_TIME));
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

    private static Long getBoltStats(Map<String, Map<GlobalStreamId, Long>> map, String statName) {
        if (map != null) {
            Long statValue = null;
            Map<GlobalStreamId, Long> tempMap = map.get(statName);
            Set<GlobalStreamId> key = tempMap.keySet();

            if (key.size() > 0) {
                Iterator<GlobalStreamId> iterator = key.iterator();
                statValue = tempMap.get(iterator.next());
            }
            return statValue;
        }

        return 0L;
    }

    public static void main(String[] args) throws IOException {
        BoltStat.printBoltStat("storm_topology-1-1436326975");
        ThriftClient.close();
    }
}
