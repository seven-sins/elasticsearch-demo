package com.es.esdemo.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class EsUtils {

	@SuppressWarnings("resource")
	public static TransportClient getClient() {
		// 设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-es").build();
        // 创建client
        try {
			return new PreBuiltTransportClient(settings) .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.0.205"), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
        return null;
	}
}
