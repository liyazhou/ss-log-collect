package es;

//import com.fasterxml.jackson.databind.*;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by liyazhou on 2017/2/23.
 */
public class EsClient {
//    curl -XGET 'localhost:9200/_cat/indices?v&pretty'

    // on startup
    public static final String host1 = "localhost";

    public static void main(String[] args) {
//        Settings settings = Settings.builder()
//                .put("cluster.name", "lyzcluster").build();

        TransportClient client = null;
        try {
            client = new PreBuiltTransportClient(Settings.EMPTY)
//            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
//                .addTransportAddress(new InetSocketTransportAd dress(InetAddress.getByName("host2"), 9300));
//            buildIndex(client);
            getIndex(client);
//            deleteIndex(client);
//            deleteByQuery(client);
//            deleteByQueryAsy(client);
//            update(client);
//            updateInsert(client);
            multiGet(client);

        } catch (UnknownHostException e) {
            e.printStackTrace();
//            log.error(e);
        } finally {
//        on shutdown
            client.close();
        }
    }

    private static void multiGet(TransportClient client) {
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("twitter", "tweet", "1")
                .add("twitter", "tweet", "1")
//                .add("twitter", "tweet", "2", "3", "4")
//                .add("bank", "account", "foo")
                .get();

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println(json);
            }
        }


    }

    private static void updateInsert(TransportClient client) {
        try {


            IndexRequest indexRequest = null;
            indexRequest = new IndexRequest("index", "type", "1")
                    .source(jsonBuilder()
                            .startObject()
                            .field("name", "Joe Smith")
                            .field("gender", "male")
                            .endObject());
            GetResponse response = client.prepareGet("index", "type", "1").setOperationThreaded(false).get();
            System.out.println(response.getSourceAsString());
            UpdateRequest updateRequest = new UpdateRequest("index", "type", "1")
                    .doc(jsonBuilder()
                            .startObject()
                            .field("gender", "lyz")
                            .endObject())
                    .upsert(indexRequest);
            try {
                client.update(updateRequest).get();
                response = client.prepareGet("index", "type", "1").setOperationThreaded(false).get();
                System.out.println(response.getSourceAsString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void update(TransportClient client) {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("twitter");
        updateRequest.type("tweet");
        updateRequest.id("1");

        try {
            updateRequest.doc(jsonBuilder()
                    .startObject()
                    .field("user", "male")
                    .endObject());
            client.update(updateRequest).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
//
//        client.prepareUpdate("ttl", "doc", "1")
//                .setScript(new Script("ctx._source.gender = \"male\""  , ScriptService.ScriptType.INLINE, null, null))
//                .get();
//
//        try {
//            client.prepareUpdate("ttl", "doc", "1")
//                    .setDoc(jsonBuilder()
//                            .startObject()
//                            .field("gender", "male")
//                            .endObject())
//                    .get();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        UpdateRequest updateRequest2 = new UpdateRequest("twitter", "tweet", "1")
                .script(new Script("ctx._source.user = \"female\""));
        try {
            client.update(updateRequest2).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        UpdateRequest updateRequest3 = null;
        try {
            updateRequest3 = new UpdateRequest("twitter", "tweet", "1")
                    .doc(jsonBuilder()
                            .startObject()
                            .field("user", "mid")
                            .endObject());
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            client.update(updateRequest3).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void deleteBydQueryAsy(TransportClient client) {
//        fix yml
//        "thread_pool": {
//            "force_merge": {
//                "type": "fixed",
//                        "min": 1,
//                        "max": 1,
//                        "queue_size": -1
//            },
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("user", "kimchy"))
                .source("twitter")
                .execute(new ActionListener<BulkIndexByScrollResponse>() {
                    @Override
                    public void onResponse(BulkIndexByScrollResponse response) {
                        long deleted = response.getDeleted();
//                        System.out.println(response.toString());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // Handle the exception
                        System.out.println(e);
                    }
                });
    }

    private static void deleteByQuery(TransportClient client) {
        BulkIndexByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                        .filter(QueryBuilders.matchQuery("user", "kimchy"))
                        .source("twitter")
                        .get();

        long deleted = response.getDeleted();
        System.out.println(response.toString());
    }

    private static void deleteIndex(TransportClient client) {
        DeleteResponse response = client.prepareDelete("twitter", "tweet", "1").get();
//        DeleteResponse response = client.prepareDelete("twitter", "tweet", "1")
//                .setOperationThreaded(false)
//                .get();
        System.out.println(response.toString());

    }

    private static void getIndex(TransportClient client) {
//        GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
        GetResponse response = client.prepareGet("twitter", "tweet", "1").setOperationThreaded(false).get();
        System.out.println(response.getSourceAsString());


    }

    private static void buildIndex(TransportClient client) throws UnknownHostException {

        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        IndexResponse response = client.prepareIndex("index", "type")
                .setSource(json)
                .get();
        // Index name
        String _index = response.getIndex();
// Type name
        String _type = response.getType();
// Document ID (generated or not)
        String _id = response.getId();
// Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();
// status has stored current instance statement.
        RestStatus status = response.status();
//
//            Map<String, Object> json = new HashMap<String, Object>();
//            json.put("user","kimchy");
//            json.put("postDate",new Date());
//            json.put("message","trying out Elasticsearch");

        // instance a json mapper
        Msg msg = new Msg();
        msg.setUser("liyazhou");
        msg.setPostDate(new Date());
        msg.setMessage("hello wolrd");
//            ObjectMapper mapper = new ObjectMapper(); // create once, reuse
//
//// generate json
//            byte[] json = mapper.writeValueAsBytes(msg);
//

        try {
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("user", "kimchy")
                    .field("postDate", new Date())
                    .field("message", "trying out Elasticsearch")
                    .endObject();
            String jsonStr = builder.string();


            IndexResponse response2 = client.prepareIndex("twitter", "tweet", "1")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("user", "kimchy")
                            .field("postDate", new Date())
                            .field("message", "trying out Elasticsearch")
                            .endObject()
                    )
                    .get();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
