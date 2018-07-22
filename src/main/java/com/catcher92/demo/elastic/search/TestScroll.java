package com.catcher92.demo.elastic.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class TestScroll {

    public static void main(String[] args) {
        TransportClient client;
        try {
            client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        SearchResponse scrollResponse = client.prepareSearch("testIndex")     // index
                .setTypes("testType")                                                   // type
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)                // 官方推荐使用_doc 排序
                .setScroll(TimeValue.timeValueMinutes(10))                              // 处理一个批次的时间
                .setQuery(QueryBuilders.matchAllQuery())                                // 查询全部数据
                .setSize(100)                                                           // 一个shade返回的数量
                .get();
        try {
            SearchHit[] searchHits;
            do {
                searchHits = scrollResponse.getHits().getHits();
                for (SearchHit searchHit : searchHits) {
                    System.out.println(searchHit.getSourceAsString());
                }
                // 获取下一个批次数据
                scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId())
                        .setScroll(TimeValue.timeValueMinutes(10)).get();
            } while (searchHits.length != 0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.prepareClearScroll();
            client.close();
        }
    }

}
