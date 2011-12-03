package org.elasticsearch.demo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class BasicTest extends AbstractTest {

    private byte[] createSource(String room, String color) throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .field("room", room)
            .field("color", color)
            .endObject()
            .copiedBytes();
    }

    @Test
    public void simple() throws IOException {

        // For unit tests it is recommended to use local node.
        // This is to ensure that your node will never join existing cluster on the network.
        Node node = NodeBuilder.nodeBuilder()
                .settings(settings)
                .local(true)
                .node();

        Client client = node.client();

        client.prepareIndex("house", "room", "1").setSource(createSource("livingroom","red")).execute().actionGet();
        client.prepareIndex("house", "room", "2").setSource(createSource("familyroom","white")).execute().actionGet();
        client.prepareIndex("house", "room", "3").setSource(createSource("kitchen","blue")).execute().actionGet();
        client.prepareIndex("house", "room", "4").setSource(createSource("bathroom","white")).execute().actionGet();
        client.prepareIndex("house", "room", "5").setSource(createSource("garage","blue")).execute().actionGet();

        client.admin().indices().refresh(Requests.refreshRequest("_all")).actionGet();

        QueryBuilder queryBuilder = QueryBuilders.termQuery("color", "white");

        SearchResponse resp = client.prepareSearch("house")
                .setTypes("room")
//                .setSearchType(SearchType.DEFAULT)
                .setQuery(queryBuilder)
//                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();

        List<String> expected = new ArrayList<String>();
        expected.add("2");
        expected.add("4");

        assertEquals(expected.size(), resp.hits().getTotalHits());

        for (SearchHit hit : resp.hits().getHits()) {
            assertTrue(expected.contains(hit.id()));
        }

        client.close();
        node.close();

    }
}
