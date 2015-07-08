package com.skyfree.monitor;

import backtype.storm.generated.Nimbus.Client;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;

import java.io.Closeable;
import java.io.IOException;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/8 14:54
 */
public class ThriftClient {
    private static final String storm_url = "storm";

    private static Client client;
    private static TFramedTransport transport;

    static {
        TSocket socket = new TSocket(storm_url, 6627);
        transport = new TFramedTransport(socket);
        TProtocol tBinaryProtocol = new TBinaryProtocol(transport);

        client = new Client(tBinaryProtocol);

        try {
            transport.open();
        } catch (Exception exception) {
            throw new RuntimeException("Error occured while making conneciton with nimbus thrift server");
        }

    }

    public static Client getClient() {
        return client;
    }

    public static void close() throws IOException {
        if (transport != null) {
            transport.close();
        }
    }
}
