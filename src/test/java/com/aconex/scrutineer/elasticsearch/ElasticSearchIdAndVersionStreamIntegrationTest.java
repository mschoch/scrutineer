package com.aconex.scrutineer.elasticsearch;

import static com.aconex.scrutineer.HasIdAndVersionMatcher.hasIdAndVersion;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.SystemUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aconex.scrutineer.IdAndVersion;
import com.fasterxml.sort.DataReaderFactory;
import com.fasterxml.sort.DataWriterFactory;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;
import com.fasterxml.sort.util.NaturalComparator;

public class ElasticSearchIdAndVersionStreamIntegrationTest {

    private static final String INDEX_NAME = "local";
    private Client client;
    private ElasticSearchTestHelper elasticSearchTestHelper;

    @Before
    public void setup() throws IOException {
        Node node = nodeBuilder().local(true).node();
        client = node.client();
        deleteIndexIfExists();

        indexIdAndVersion("1", 1);
        indexIdAndVersion("3", 3);
        indexIdAndVersion("2", 2);

        client.admin().indices().prepareFlush(INDEX_NAME).execute().actionGet();
    }

    @After
    public void teardown() {
        client.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldGetStreamFromElasticSearch() {

        SortConfig sortConfig = new SortConfig().withMaxMemoryUsage(256*1024*1024);
        DataReaderFactory<IdAndVersion> dataReaderFactory = new IdAndVersionDataReaderFactory();
        DataWriterFactory<IdAndVersion> dataWriterFactory = new IdAndVersionDataWriterFactory();
        Sorter sorter = new Sorter(sortConfig, dataReaderFactory, dataWriterFactory, new NaturalComparator<IdAndVersion>());
        ElasticSearchDownloader elasticSearchDownloader = new ElasticSearchDownloader(client, INDEX_NAME, "_type:idandversion", "_rev");
        ElasticSearchIdAndVersionStream elasticSearchIdAndVersionStream =
                new ElasticSearchIdAndVersionStream(elasticSearchDownloader, new ElasticSearchSorter(sorter), new IteratorFactory(), SystemUtils.getJavaIoTmpDir().getAbsolutePath());

        elasticSearchIdAndVersionStream.open();
        Iterator<IdAndVersion> iterator = elasticSearchIdAndVersionStream.iterator();

        assertThat(iterator.next(), hasIdAndVersion("1","1"));
        assertThat(iterator.next(), hasIdAndVersion("2","2"));
        assertThat(iterator.next(), hasIdAndVersion("3","3"));

        elasticSearchIdAndVersionStream.close();
    }

    private void deleteIndexIfExists() {
        elasticSearchTestHelper = new ElasticSearchTestHelper(client);
        elasticSearchTestHelper.deleteIndexIfItExists(INDEX_NAME);
    }

    private void indexIdAndVersion(String id, long version) throws IOException {
        client.prepareIndex(INDEX_NAME,"idandversion").setId(id).setOperationThreaded(false).setVersion(version+10).setVersionType(VersionType.EXTERNAL).setSource(jsonBuilder().startObject().field("_rev", new Long(version).toString()).endObject()).execute().actionGet();
    }

}
