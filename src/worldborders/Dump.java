/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package worldborders;
import org.neo4j.shell.util.json.JSONException;
import worldborders.Helper;
import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.MongoException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.*;
import java.io.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.neo4j.shell.util.json.JSONObject;

/**
 *
 * @author rafael
 */
public class Dump {
    
    private String proxy;
    private int port;
    private String dir;
    private int maxRequestsPerHour;
    
    public Dump(String dir, int maxRequests){
        this.proxy = null;
        this.port = -1;
        this.dir = dir;
        this.maxRequestsPerHour = maxRequests;
    }
    
    public Dump(String dir, int maxRequests, String proxy, int port){
        this.proxy = proxy;
        this.port = port;
        this.dir = dir;
        this.maxRequestsPerHour = maxRequests;
    }
    
    
    //select="users" or select="startups"
    public void getCountries(){
        Pattern countryPattern = Pattern.compile(".*\\{\\{flagcountry\\|(\\w+)\\}\\}");
        Pattern durationPattern = Pattern.compile("\\|\\{\\{yes\\|(\\d+\\s\\w+)\\}\\}.*");
        String [] countryLines = getWikiRevision("Template:Visa_requirements").split("\n");
        for (String countryLine : countryLines){
            if(countryLine.matches(".*Visa requirements for.*")){
                countryLine = countryLine.replaceAll("(\\* \\[\\[)|(\\|.*)", "");
                String [] revisionLines = getWikiRevision(countryLine).split("\n");
                for(int i=0; i<revisionLines.length; i++){
                    Matcher countryMatcher = countryPattern.matcher(revisionLines[i]);
                    if(countryMatcher.matches()){
                       String country = countryMatcher.group(1); 
                       String duration;
                       System.out.println(country);
                       i++;
                       Matcher durationMatcher = durationPattern.matcher(revisionLines[i]);
                       if(durationMatcher.matches()){
                           duration = durationMatcher.group(1);
                           System.out.println(duration); 
                       }                       
                    }
                }
            }            
        }       
        System.out.println("Finished");
    }
    
    
    private String getWikiRevision(String request){
        String wikiRequest = "http://en.wikipedia.org/w/api.php?format=json&action=query&prop=revisions&rvprop=content&titles="; 
        String response = Helper.makeRequest(wikiRequest+request.replaceAll("\\s", "_"), proxy, port);
        try {
            JSONObject jsonObject = new JSONObject(response);
            JSONObject page = (JSONObject) jsonObject.getJSONObject("query").get("pages");
            Iterator pagesKeys = page.keys();
            String pageKey = (String) pagesKeys.next();
            return page.getJSONObject(pageKey).getJSONArray("revisions").getJSONObject(0).getString("*");
        } catch (JSONException ex) {
            Logger.getLogger(Dump.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }
    
    
    
    public void getStartupRoles(){
        Mongo m;
        DB db;
        DBCollection startups;
        DBCursor cur = null;
        BasicDBObject query = new BasicDBObject();
        query.put("hidden", false);
        
        try {
            m = new Mongo();
            db = m.getDB("angelmatch");
            startups = db.getCollection("startups");
            cur = startups.find(query);
            cur.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
            cur.batchSize(10);
        } catch (UnknownHostException ex) {
            Logger.getLogger(Dump.class.getName()).log(Level.SEVERE, null, ex);
        } catch (MongoException ex) {
            Logger.getLogger(Dump.class.getName()).log(Level.SEVERE, null, ex);
        }
        String response = "";
        int j=0;
        String currentFile = "startup_roles_1.json";
        String request = "http://en.wikipedia.org/w/api.php?format=xml&action=query&prop=revisions&rvprop=content&titles=Main%20Page"; 
        String startup_id = "{\"startup_id\":";
        String delim = "";
        while(cur.hasNext()) {
            Integer id = (Integer) cur.next().get("id");
            if (j>=maxRequestsPerHour){
                j=0;
                currentFile = "startup_roles_"+id+".json";
                Helper.waitSeconds(3600);
            }  
            response = null; 
            do{
                response = Helper.makeRequest(request+id, proxy, port);  
                if ("400".equals(response)){
                  Helper.waitSeconds(3600);
                }
            } while (response==null || "500".equals(response));
                
            response = response.substring(1);
            if(!"".equals(response)){
                response = delim+startup_id+id+","+response;
                System.out.println(response);
                Helper.writeToFile(dir+currentFile, response);
                delim = "\n";
            }
            j++;      
        } 
        System.out.println("Finished Startup Roles");
    }
    
        
     
     //select must be users, startups or startup_roles
     public void mongoImportAll(String select){
         String drop = " --drop";
         String command = "mongoimport --host localhost --db angelmatch --collection "+select+" --type json --file ";
         File folder = new File(dir);
         File[] files = folder.listFiles();
         for(int i=0; i<files.length; i++){
             if(files[i].isFile()){
                 String fileName = files[i].getName();
                 if (fileName.matches(select+"_\\d+\\.json")){
                   String cmd = command+dir+fileName+drop;
                   drop = "";
                   Runtime run = Runtime.getRuntime();
                   Process pr;
                   try {
                        pr = run.exec(cmd);
                        pr.waitFor();      
                        System.out.println(cmd);
                    } catch (IOException ex) {
                        Logger.getLogger(Dump.class.getName()).log(Level.SEVERE, null, ex);
                    }catch (InterruptedException ex) {
                        Logger.getLogger(Dump.class.getName()).log(Level.SEVERE, null, ex);
                    }                       
                 }    
             }
         }
     }
}
