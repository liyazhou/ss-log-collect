package es;

//import com.fasterxml.jackson.databind.*;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by liyazhou on 2017/2/23.
 */
public class EsClient {

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

            String json = "{" +
                    "\"user\":\"kimchy\"," +
                    "\"postDate\":\"2013-01-30\"," +
                    "\"message\":\"trying out Elasticsearch\"" +
                    "}";
            IndexResponse response = client.prepareIndex("twitter", "tweet")
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


        } catch (UnknownHostException e) {
            e.printStackTrace();
//            log.error(e);
        } finally {
//        on shutdown
            client.close();
        }
    }
}
