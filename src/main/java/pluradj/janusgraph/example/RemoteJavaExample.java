package pluradj.janusgraph.example;

import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.janusgraph.core.ConfiguredGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.management.ConfigurationManagementGraph;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteJavaExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteJavaExample.class);
    private static final String GRAPH_NAME = "janus_test";

    public static void main(String[] args) throws ConfigurationException {
        // https://tinkerpop.apache.org/docs/current/reference/#connecting-via-java
        String contactPoint = "localhost";
        int port = 8182;
        GryoMapper.Builder builder = GryoMapper.build().addRegistry(JanusGraphIoRegistry.getInstance());
        MessageSerializer serializer = new GryoMessageSerializerV1d0(builder);

        Cluster cluster = Cluster.build().
            addContactPoint(contactPoint).
            port(port).
            serializer(serializer).
            create();
        Client client = cluster.connect().init();
        
        
        JanusGraphManager gm = new JanusGraphManager(new Settings());
        PropertiesConfiguration graphConf = new PropertiesConfiguration("conf/remote-graph.properties");

     // ConfiguredGraphFactory needs a graph configuration to store configurations
        StandardJanusGraph graph =
                new StandardJanusGraph(
                		new GraphDatabaseConfiguration(
                				new CommonsConfiguration(graphConf)
                				)
                		);
        new ConfigurationManagementGraph(graph);
        
        
     // Creating template configuration or updating if one already exists
        if (null == ConfiguredGraphFactory.getTemplateConfiguration()) {
            Configuration templaceConfiguration = ConfigurationUtils.cloneConfiguration(graphConf);
            templaceConfiguration.clearProperty("graph.graphname");
            ConfiguredGraphFactory.createTemplateConfiguration(templaceConfiguration);
        } else {
            ConfiguredGraphFactory.updateConfiguration(graphConf.getString("graph.graphname"), graphConf);
        }
        
        
        if (!ConfiguredGraphFactory.getGraphNames().contains(GRAPH_NAME)) {
            ConfiguredGraphFactory.create(GRAPH_NAME);
        }
        
        JanusGraph janusgraph1 = ConfiguredGraphFactory.open(GRAPH_NAME);
        
     // Mandatory to have "alias" defined in here "graph1_traversal". Alias should be graphname + "_traversal"
        GraphTraversalSource g = janusgraph1.traversal().withRemote(DriverRemoteConnection.using(cluster, "graph1_traversal"));
        
        
        
        
        
        // demonstrate Java driver passing Gremlin as a string to be executed on the remote server
        /*LOGGER.info("create a new edge");
        String gremlin = "g.addV('a').as('a').addV('b').addE('to').from('a').next()";
        ResultSet results = client.submit(gremlin);
        Edge edge = results.one().getEdge();
        LOGGER.info(edge.toString());*/
        

        // demonstrate Java GLV using Gremlin natively in app and executed on the remote server
       // Graph graph = EmptyGraph.instance();
        
        
        try {
           // GraphTraversalSource g = graph.traversal().withRemote("conf/remote-graph.properties");
            List list = g.V().valueMap(true).toList();
            list.forEach((v) -> LOGGER.info(v.toString()));
            list = g.E().valueMap(true).toList();
            list.forEach((e) -> LOGGER.info(e.toString()));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

       // LOGGER.info("drop all");
       // gremlin = "g.V().drop().iterate(); g.V().count().next()";
       // results = client.submit(gremlin);
       // LOGGER.info(""+results.one().getLong());

        client.close();
        cluster.close();
    }
}
