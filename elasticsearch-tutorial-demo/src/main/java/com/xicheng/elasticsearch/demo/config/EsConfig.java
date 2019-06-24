package com.xicheng.elasticsearch.demo.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;

@Configuration
public class EsConfig {

    private static final String HOST = "192.168.143.112";
    private static final int PORT = 9300;

    /**
     * 连接es
     * @return
     * @throws Exception
     */
    public TransportClient getConnection() throws Exception{
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT));

        System.out.println("连接成功");
        return client;
    }

}
