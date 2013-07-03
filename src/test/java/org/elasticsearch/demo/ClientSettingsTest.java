/*
 * Copyright 2013 Lukas Vlcek and contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.demo;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ClientSettingsTest extends BaseTestSupport {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private String tempFolderName;
    private Settings settings;

    @Before
    public void prepare() throws IOException {
        tempFolderName = testFolder.newFolder(IndexAndSearchTest.class.getName()).getCanonicalPath();
//        tempFolderName = "/Users/lukas/projects/lukas-vlcek/elasticsearch.demo/tmp";
        System.out.println(tempFolderName);
        settings = settingsBuilder()
                // ---------------------------------------------
                // "mmapfs" is platform dependent, let's use the default instead, in any case it can not be "memory"
                // .put("index.store.type", "mmapfs")
                // ---------------------------------------------
                // use "local" gateway type to get physical files on fs,
                // thus we can check what is the difference between CFS and non-CFS index
                .put("gateway.type", "local")
                .put("path.data", tempFolderName)
                .put("index.number_of_shards", 1)
                .put("index.compound_format", false) // [ 0,"0",false ] are all equal - meaning CFS is used
                .build();
    }

    @Test public void shouldUseCFS() throws IOException {

        // For unit tests it is recommended to use local node.
        // This is to ensure that your node will never join existing cluster on the network.
        Node node = NodeBuilder.nodeBuilder()
                .settings(settings)
                .local(true)
                .node();

        // get Client
        Client client = node.client();

        String non_cfsIndex = "noncfs";
        String cfsIndex = "cfs";
        String clusterName = client.admin().cluster().prepareHealth().execute().actionGet().getClusterName();

        // Index some data
        client.prepareIndex(non_cfsIndex, "room", "1").setSource(createSource("livingroom", "red")).execute().actionGet();
        client.admin().indices().prepareFlush(non_cfsIndex).execute().actionGet();
        client.prepareIndex(non_cfsIndex, "room", "2").setSource(createSource("familyroom", "white")).execute().actionGet();
        client.admin().indices().prepareFlush(non_cfsIndex).execute().actionGet();
        client.prepareIndex(non_cfsIndex, "room", "3").setSource(createSource("kitchen", "blue")).execute().actionGet();
        client.admin().indices().prepareFlush(non_cfsIndex).execute().actionGet();

//        File file = new File(tempFolderName);

        client.admin().indices()
                .prepareCreate(cfsIndex)
                .setSettings(settingsBuilder().put("index.compound_format", true).build())
                .execute().actionGet();

        client.prepareIndex(cfsIndex, "room", "1").setSource(createSource("livingroom", "red")).execute().actionGet();
        client.admin().indices().prepareFlush(cfsIndex).execute().actionGet();
        client.prepareIndex(cfsIndex, "room", "2").setSource(createSource("familyroom", "white")).execute().actionGet();
        client.admin().indices().prepareFlush(cfsIndex).execute().actionGet();
        client.prepareIndex(cfsIndex, "room", "3").setSource(createSource("kitchen", "blue")).execute().actionGet();
        client.admin().indices().prepareFlush(cfsIndex).execute().actionGet();

        assertFalse(
                client.admin().indices().prepareSegments(non_cfsIndex).execute().actionGet()
                        .getIndices().get(non_cfsIndex)
                        .getShards().get(0).getShards()[0]
                        .getSegments().get(0)
                        .compound
        );

        assertTrue(
                client.admin().indices().prepareSegments(cfsIndex).execute().actionGet()
                        .getIndices().get(cfsIndex)
                        .getShards().get(0).getShards()[0]
                        .getSegments().get(0)
                        .compound
        );

        // cleanup
        client.close();
        node.close();

        assertTrue(node.isClosed());
    }
}
