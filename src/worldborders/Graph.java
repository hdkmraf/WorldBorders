/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package worldborders;

import java.io.File;
import java.io.IOException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;


/**
 *
 * @author rafael
 */
public class Graph {
    
    private String DB_PATH;
    String greeting;
    private GraphDatabaseService graphDb;
    private Index<Node> nameIndex;
    private Index<Node> codeIndex;
    private String NAME_KEY = "name";
    private String WEIGHT_KEY = "weight";
    private String DAYS_KEY = "days";
    private String CODE_KEY = "code";
    private String LATITUDE_KEY = "latitude";
    private String LONGITUDE_KEY = "longitude";
    private String DIR;
    
    
    public Graph(String dir, String db_path, boolean newDb){
        DIR = dir;
        DB_PATH = db_path;
        if(newDb){
            clearDb();
        }
        // START SNIPPET: startDb
        //graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        graphDb = new EmbeddedGraphDatabase( DB_PATH );
        nameIndex = graphDb.index().forNodes(NAME_KEY);
        codeIndex = graphDb.index().forNodes(CODE_KEY);
        registerShutdownHook( graphDb );
        // END SNIPPET: startDb
    }
    
    
   private enum worldRelationshipTypes implements RelationshipType{
       allowed_stay
   }
    
    
   private void clearDb()
    {
        try
        {
            FileUtils.deleteRecursively( new File( DB_PATH ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
   
   void shutDown()
    {
        System.out.println();
        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();
        // END SNIPPET: shutdownServer
    }
    
   // START SNIPPET: shutdownHook
   private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
    // END SNIPPET: shutdownHook
    
    public void createRelationship(Country from, Country to, Float weight, Float days){
         Transaction tx = graphDb.beginTx();
         Node fromNode = null;
         Node toNode = null;
         Relationship relationship;
         try{
            fromNode = codeIndex.get(CODE_KEY, from.CODE).getSingle();
            if(fromNode == null){
                fromNode = createAndIndexNode(from);
            }
            toNode = codeIndex.get(CODE_KEY, to.CODE).getSingle();
            if(toNode == null){
                toNode = createAndIndexNode(to);
            }
            relationship = fromNode.createRelationshipTo(toNode, worldRelationshipTypes.allowed_stay);
            relationship.setProperty(WEIGHT_KEY, weight);
            relationship.setProperty(DAYS_KEY, days);
            tx.success();
         } catch (Exception ex){
             //System.out.println("Problem with graph relationship:"+ex);
             ex.getMessage();
         }
           finally {
             tx.finish();
         }              
    }
    
     private Node createAndIndexNode(Country country){
        Node node = graphDb.createNode();
        node.setProperty(NAME_KEY, country.NAME);
        node.setProperty(CODE_KEY, country.CODE);
        node.setProperty(LATITUDE_KEY, country.LATITUDE);
        node.setProperty(LONGITUDE_KEY, country.LONGITUDE);
        
        nameIndex.add(node, NAME_KEY, country.NAME);
        codeIndex.add(node, CODE_KEY, country.CODE);
        return node;
        
    }        
}
