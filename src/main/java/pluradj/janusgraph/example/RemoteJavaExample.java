package pluradj.janusgraph.example;

import java.util.List;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteJavaExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteJavaExample.class);

    public static void main(String[] args) {
        // https://tinkerpop.apache.org/docs/current/reference/#connecting-via-java
        String contactPoint = "localhost";
        int port = 8182;
        GryoMapper.Builder builder = GryoMapper.build().addRegistry(JanusGraphIoRegistry.getInstance());
        MessageSerializer serializer = new GryoMessageSerializerV3d0(builder);

        Cluster cluster = Cluster.build().
            addContactPoint(contactPoint).
            port(port).
            serializer(serializer).
            create();
        Client client = cluster.connect().init();

        // demonstrate Java driver passing Gremlin as a string to be executed on the remote server
        LOGGER.info("create a new edge");
        String gremlin = "g.addV('a').as('a').addV('b').addE('to').from('a').next()";
        ResultSet results = client.submit(gremlin);
        Edge edge = results.one().getEdge();
        LOGGER.info(edge.toString());

        // demonstrate Java GLV using Gremlin natively in app and executed on the remote server
        Graph graph = EmptyGraph.instance();
        try {
            GraphTraversalSource g = graph.traversal().withRemote("conf/remote-graph.properties");
            List list = g.V().valueMap(true).toList();
            list.forEach((v) -> LOGGER.info(v.toString()));
            list = g.E().valueMap(true).toList();
            list.forEach((e) -> LOGGER.info(e.toString()));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

        LOGGER.info("drop all");
        gremlin = "g.V().drop().iterate(); g.V().count().next()";
        results = client.submit(gremlin);
        LOGGER.info(""+results.one().getLong());

        client.close();
        cluster.close();
    }
}
