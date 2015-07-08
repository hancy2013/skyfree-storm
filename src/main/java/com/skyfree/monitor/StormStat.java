package com.skyfree.monitor;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologySummary;

import java.io.IOException;
import java.util.Iterator;

import static com.skyfree.monitor.Utils.*;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/8 12:13
 */
public class StormStat {

    public static void printNimbusStats() {
        try {
            Client client = ThriftClient.getClient();
            String nimbusConfiguration = client.getNimbusConf();

            printLine();
            System.out.println("Nimbus Configuration:" + nimbusConfiguration);
            printLine();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void printSupervisorStat() {
        try {
            Client client = ThriftClient.getClient();
            ClusterSummary clusterSummary = client.getClusterInfo();

            Iterator<SupervisorSummary> supervisorSummaryIterator = clusterSummary.get_supervisors_iterator();

            while (supervisorSummaryIterator.hasNext()) {
                SupervisorSummary ss = supervisorSummaryIterator.next();
                printLine();
                System.out.println("Supervisor Host IP: " + ss.get_host());
                System.out.println("Number of used workers: " + ss.get_num_used_workers());
                System.out.println("Number of  workers: " + ss.get_num_workers());
                System.out.println("Supervisor_id : " + ss.get_supervisor_id());
                System.out.println("Supervisor uptime in sec: " + ss.get_uptime_secs());
                printLine();
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void printTopologyStat() {
        try {
            Client client = ThriftClient.getClient();
            ClusterSummary clusterSummary = client.getClusterInfo();

            Iterator<TopologySummary> topologiesIterator = clusterSummary.get_topologies_iterator();

            while (topologiesIterator.hasNext()) {
                TopologySummary ts = topologiesIterator.next();
                printLine();
                System.out.println("topology id : " + ts.get_id());
                System.out.println("topology name : " + ts.get_name());
                System.out.println("number of executors : " + ts.get_num_executors());
                System.out.println("number of tasks : " + ts.get_num_tasks());
                System.out.println("number of workers : " + ts.get_num_workers());
                System.out.println("status : " + ts.get_status());
                System.out.println("topology uptime in sec : " + ts.get_uptime_secs());
                printLine();
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        StormStat.printNimbusStats();
        StormStat.printSupervisorStat();
        StormStat.printTopologyStat();

        ThriftClient.close();
    }
}
