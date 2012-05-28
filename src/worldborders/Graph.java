/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package worldborders;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.MongoException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Hashtable;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.Traversal;
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
    private Node startupNode;
    private Node userNode;
    private Relationship relationship;
    private Index<Node> nameIndex;
    private Index<Node> typeIndex;
    private Index<Node> idIndex;
    private Index<Relationship> roleIndex;
    private String NAME_KEY = "name";
    private String TYPE_KEY = "type";
    private String ROLE_KEY = "role";
    private String ID_KEY = "angel_id";
    private String MONGO_DB = "angelmatch";
    private String MARKETS_KEY = "markets";
    private String LOCATIONS_KEY = "locations";
    private String WEIGHT_KEY = "weight";
    private Hashtable roleTypes;
    private Integer startupId;
    private String startupName;
    private String DIR;
    private Hashtable weightsTable;
    StopEvaluator stopEvaluator;
    Integer maxDepth = 5;
    
    
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
        typeIndex = graphDb.index().forNodes(TYPE_KEY);
        idIndex = graphDb.index().forNodes(ID_KEY);
        roleIndex = graphDb.index().forRelationships(ROLE_KEY);
        roleTypes = new Hashtable();
        registerShutdownHook( graphDb );
        // END SNIPPET: startDb
        //Initialize weights hashtable
        weightsTable = new Hashtable();
        weightsTable.put("past_investor",   new Integer(8));
        weightsTable.put("incubator",       new Integer(7));
        weightsTable.put("advisor",         new Integer(6));
        weightsTable.put("referer",         new Integer(5));
        weightsTable.put("founder",         new Integer(4));
        weightsTable.put("employee",        new Integer(3));
        weightsTable.put("LocationTag",     new Integer(2));
        weightsTable.put("MarketTag",       new Integer(1));
        stopEvaluator = new StopEvaluator(){
            @Override
            public boolean isStopNode(TraversalPosition tp) {
                /*if (tp.depth()>(maxDepth-1) && tp.lastRelationshipTraversed().isType(angelRelationshipTypes.past_investor)){
                    String nodeType = (String) tp.currentNode().getProperty(TYPE_KEY, null);
                    if ("user".equals(nodeType)){
                        return true;
                    }
                } else */
                if (tp.depth()>maxDepth){
                    return true;
                }
                return false;
            }
        
        };
    }
    
    
   private enum angelRelationshipTypes implements RelationshipType{
       LocationTag, MarketTag, past_investor, advisor, incubator, referer, founder, employee
   }
    
    public void createDb(){
        Mongo m;
        DB db;
        DBCollection startupRoles;
        DBCollection startups = null;
        DBCursor roleCursor = null;
        DBCursor startupCursor = null;
        BasicDBObject query = new BasicDBObject();
                
        try {
            m = new Mongo();
            db = m.getDB(MONGO_DB);
            //get all the startup_roles from mongo
            startupRoles = db.getCollection("startup_roles");
            //get all the startups
            startups = db.getCollection("startups");
            roleCursor = startupRoles.find();
            roleCursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
            roleCursor.batchSize(10);
        } catch (UnknownHostException ex) {
            Logger.getLogger(Dump.class.getName()).log(Level.SEVERE, null, ex);
        } catch (MongoException ex) {
            Logger.getLogger(Dump.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        //iterate through all roles
        while(roleCursor.hasNext()) {
            DBObject roleObject = roleCursor.next();
            startupId = (Integer) roleObject.get("startup_id"); 
            
            //Get the specific startup for this role
            query.put("id", startupId);
            startupCursor = startups.find(query);
            DBObject startupObject = startupCursor.next();
            startupName = (String) startupObject.get(NAME_KEY); 
            

            startupNode = null;
            
            //iterate through roles beloinging to this startup
            ArrayList roles = (ArrayList) roleObject.get("startup_roles");
            for(int i=0; i<roles.size();i++){
                String role = null;
                Integer userId = null;
                String userName = null;
                //Get user data
                try{
                    DBObject roleEntry = (BasicDBObject) roles.get(i);
                    DBObject user = (DBObject) roleEntry.get("user");
                    role = (String) roleEntry.get(ROLE_KEY);
                    userId = (Integer) user.get("id");
                    userName = (String) user.get(NAME_KEY);
                    DynamicRelationshipType type = DynamicRelationshipType.withName(role);
                    roleTypes.put(role, type);      
                } catch(Exception ex) {
                    continue;
                }
                       
                Transaction tx = graphDb.beginTx();
                try{
                    startupNode = nameIndex.get(NAME_KEY, startupName).getSingle();
                    if(startupNode==null){
                        startupNode = createAndIndexNode(startupId,startupName,"startup");
                    }
                    userNode = null;
                    userNode = nameIndex.get(NAME_KEY, userName).getSingle();
                    if(userNode == null)
                        userNode = createAndIndexNode(userId,userName,"user");
                    relationship = userNode.createRelationshipTo(startupNode, (DynamicRelationshipType)roleTypes.get(role));
                    relationship.setProperty(WEIGHT_KEY, weightsTable.get(role));
                    roleIndex.add(relationship, ROLE_KEY, role);
                    tx.success();
                } catch (Exception ex){
                    System.out.println(ex.getMessage());
                    continue;
                } finally {
                    tx.finish();
                }
            }
            
            //iterate through marketTags
            ArrayList marketTags = (ArrayList) startupObject.get(MARKETS_KEY);
            makeRelationships(marketTags, "market_tag");
            
             //iterate through location Tags
            ArrayList locationTags = (ArrayList) startupObject.get(LOCATIONS_KEY);
            makeRelationships(locationTags, "location_tag");
        }
    }
    
    private void makeRelationships(ArrayList fromArray, String nodeType){
        Node node;
        for(int i=0; i<fromArray.size(); i++){
            String role = null;
            Integer id;
            String name;
            try{
                DBObject object = (BasicDBObject) fromArray.get(i);
                id = (Integer) object.get("id");
                name = (String) object.get(NAME_KEY);
                role = (String) object.get("tag_type");
                DynamicRelationshipType type = DynamicRelationshipType.withName(role);
                roleTypes.put(role, type);      
            } catch(Exception ex) {
                continue;
            }
            Transaction tx = graphDb.beginTx();
            try{
                startupNode = nameIndex.get(NAME_KEY, startupName).getSingle();
                if(startupNode==null){
                    startupNode = createAndIndexNode(startupId,startupName,"startup");
                }
                node = null;
                node = nameIndex.get(NAME_KEY, name).getSingle();
                if(node == null){
                    node = createAndIndexNode(id,name, nodeType);
                }
                relationship = node.createRelationshipTo(startupNode, (DynamicRelationshipType)roleTypes.get(role));
                relationship.setProperty(WEIGHT_KEY, weightsTable.get(role));
                roleIndex.add(relationship, ROLE_KEY, role);
                tx.success();
            } catch (Exception ex){
                System.out.println(ex.getMessage());
                continue;
            } finally {
                tx.finish();
            }
        } 
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
    
    private Node createAndIndexNode(int id, String name, String type){
        Node node = graphDb.createNode();
        node.setProperty(ID_KEY, id);
        node.setProperty(TYPE_KEY, type);
        node.setProperty(NAME_KEY, name);
        nameIndex.add(node, NAME_KEY, name);
        typeIndex.add(node, TYPE_KEY, type);
        idIndex.add(node, ID_KEY, id);
        return node;
        
    }
    
    public void findBestStartupUserMatches(){
        GlobalGraphOperations graphOperations = GlobalGraphOperations.at(graphDb);
        Iterable<Node> nodes = graphOperations.getAllNodes();
        String type1;
        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                Traversal.expanderForTypes(
                    angelRelationshipTypes.advisor,
                    Direction.BOTH,
                    angelRelationshipTypes.incubator,
                    Direction.BOTH,
                    angelRelationshipTypes.past_investor,
                    Direction.BOTH,
                    angelRelationshipTypes.referer,
                    Direction.BOTH,
                    angelRelationshipTypes.employee,
                    Direction.BOTH,
                    angelRelationshipTypes.founder,
                    Direction.BOTH),
                maxDepth); 
        for(Iterator<Node> itSNodes = nodes.iterator(); itSNodes.hasNext();){
            String finalLine = null;
            Node sNode = itSNodes.next();
            try{
                type1 = (String) sNode.getProperty(TYPE_KEY);
                startupName = (String) sNode.getProperty(NAME_KEY);
                startupId = (Integer) sNode.getProperty(ID_KEY);
            } catch (Exception ex){
//                itSNodes.remove();
                continue;
            }
            Double maxNetWeight = 0.0;
            Double maxRelCount = 0.0;
            Double maxPathCount = 0.0;
            String maxUserName = null;
            Double firstUser = 0.0;
            int maxUserId = 0;
            if ("startup".equals(type1)){   
                Traverser uNodes = sNode.traverse(
                        Order.DEPTH_FIRST,
                        stopEvaluator,
                        ReturnableEvaluator.ALL_BUT_START_NODE,
                        angelRelationshipTypes.advisor,
                        Direction.BOTH,
                        angelRelationshipTypes.incubator,
                        Direction.BOTH,
                        angelRelationshipTypes.past_investor,
                        Direction.BOTH,
                        angelRelationshipTypes.referer,
                        Direction.BOTH,
                        angelRelationshipTypes.employee,
                        Direction.BOTH,
                        angelRelationshipTypes.founder,
                        Direction.BOTH
                        );
                String type2 = null;
                for(Node uNode : uNodes){
                    String userName;
                    int userId;                  
                    try{
                        type2 = (String) uNode.getProperty(TYPE_KEY);
                        userName = (String) uNode.getProperty(NAME_KEY);
                        userId = (Integer) uNode.getProperty(ID_KEY);                      
                    } catch (Exception ex){
                        continue;
                    }
                    if (type2.equals("user") &&
                        uNodes.currentPosition().lastRelationshipTraversed().isType(angelRelationshipTypes.past_investor)){                          
                        Double netWeight = 0.0;
                        Double relCount = 0.0;
                        Double pathCount = 0.0;
                        Iterable<Path> paths = finder.findAllPaths(sNode, uNode);
                        for(Path path : paths){
                            Iterable<Relationship> relationships = path.relationships();
                            for(Relationship rel : relationships){
                                try{
                                    //netWeight += (Integer)rel.getProperty(WEIGHT_KEY);
                                    netWeight += ((Integer) weightsTable.get(rel.getType().name()))*firstUser;
                                    firstUser = 1.0;
                                    relCount++;
                                } catch (Exception ex){
                                    System.out.println(ex.getMessage());
                                    continue; 
                                }
                            }
                            pathCount++;
                        }
                        netWeight /= relCount;
                        if(netWeight >= maxNetWeight && relCount > maxRelCount){
                            maxNetWeight = netWeight;
                            maxRelCount = relCount;
                            maxUserName = userName;
                            maxPathCount = pathCount;
                            maxUserId = userId;
                            finalLine = "{\"startup_id\":"+startupId+",\"startup_name\":\""+startupName+"\",\"user_id\":"+maxUserId+",\"user_name\":\""+maxUserName+"\",\"relationship\":"+maxNetWeight+",\"edges\":"+maxRelCount+",\"paths\":"+maxPathCount+"}\n";
                            System.out.print(finalLine);
                            //System.out.println("{\"startup_id\":"+startupId+",\"startup_name\":\""+startupName+"\",\"user_id\":"+maxUserId+",\"user_name\":\""+maxUserName+"\",\"relationship\":"+maxNetWeight+",\"edges\":"+maxRelCount+"}\n");
                        }
                        
                    }
                }
            }
            if (maxNetWeight>0){                
                Helper.writeToFile(DIR+"matches.json",finalLine);        
                System.out.println();
            }            
        }            
    }
    
   
}
