/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package worldborders;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;


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
         Node fromNode = null;
         Node toNode = null;
         Relationship relationship;
         if ("".equals(from.NAME) || "".equals(to.NAME) || from.NAME==null || to.NAME==null){
            return;
         }
         Transaction tx = graphDb.beginTx();
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
     
    public void graphToGoogleGeochart(){
        String script = "";        
        String NL = System.getProperty("line.separator");
        
        script+= "google.load('visualization', '1', {'packages':['geochart']});"+NL;
        script+= "googleSetOnLoadCallback(drawRegionsMap);"+NL;
        
        script+= "function drawVisualization() {"+NL;
        script+= "var data = {};"+NL;
        
        
        //get data here        
        GlobalGraphOperations graphOperations = GlobalGraphOperations.at(graphDb);
        Iterable<Node> nodes = graphOperations.getAllNodes();
        int i=1;
        String dataZero = "data['main'] = google.visualization.arrayToDataTable([";
        dataZero+= "['Country','Test'],";
        
        for(Node node: nodes){
            Iterable<Relationship> relationships;
            String fromCountry;
            try{
                relationships = node.getRelationships(Direction.OUTGOING);
                fromCountry = (String) node.getProperty(NAME_KEY);                
                fromCountry = fromCountry.replace("'", "\\'").replaceAll("[^\\p{ASCII}]", "");                
                if (fromCountry.contains("China"))
                    fromCountry = "China";
                dataZero+= "['"+fromCountry+"',0],";
            }
            catch(Exception ex){
                continue;
            }
            String currentData = "";
            int relCount = 0;
            for (Relationship relationship : relationships){
                try{
                    String toCountry = (String) relationship.getEndNode().getProperty(NAME_KEY);
                    toCountry = toCountry.replace("'", "\\'").replaceAll("[^\\p{ASCII}]", "");
                    if (toCountry.contains("China"))
                        toCountry = "China";
                    currentData+= "['"+toCountry+"',100],";
                    relCount++;
                }
                catch(Exception ex){
                    continue;
                }
            }
            if(relCount>0){
                currentData= currentData.substring(0,currentData.length()-1);
                currentData= "data['"+fromCountry+"'] = google.visualization.arrayToDataTable([['Country','Test'],"+currentData;            
                currentData+= "]);";
            }
            else{
                currentData= "data['"+fromCountry+"'] = google.visualization.arrayToDataTable([['Country','Test'],['"+fromCountry+"',100]]);";
            }
            script += currentData+NL;
            i++;
        }
        dataZero = dataZero.substring(0,dataZero.length()-1);
        dataZero+= "]);"+NL;
        
        script += dataZero+NL;
            
        script+= "var index = 'main';"+NL;
        script+= "var options = {width: 556, height: 347};"+NL;

        script+= "var geochart = new google.visualization.GeoChart(document.getElementById('visualization'));"+NL;
        script+= "geochart.draw(data[index], options);"+NL;   
        
        script+= "google.visualization.events.addListener(geochart,'select',myClickHandler);"+NL;
        
        script+= "function myClickHandler(){"+NL;
        script+= "var select = geochart.getSelection();"+NL;
        script+= "var newIndex = data[index].getValue(select[0].row,0);"+NL;
        script+= "geochart.draw(data[newIndex], options);"+NL;
        script+= "index = newIndex;}"+NL;
        script+= "}";
        
        Helper.writeToFile(DIR+"googleGeochart.js", script.toString());
    }
}
