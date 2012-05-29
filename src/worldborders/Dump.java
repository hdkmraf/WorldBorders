/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package worldborders;
import org.neo4j.shell.util.json.JSONException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.*;
import java.util.Hashtable;
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
    private Graph graph;
    private Hashtable nationalities;
    private String NATIONALITIES_FILE = "src/worldborders/nationalities.csv";
    
    public Dump(String dir, int maxRequests){
        this.proxy = null;
        this.port = -1;
        this.dir = dir;
        graph = new Graph(dir, dir+"graph.db", true);
        readNationalities();
    }
    
    public Dump(String dir, int maxRequests, String proxy, int port){
        this.proxy = proxy;
        this.port = port;
        this.dir = dir;
        graph = new Graph(dir, dir+"graph.db", true);
        readNationalities();
    }
    
    
    //select="users" or select="startups"
    public void getCountries(){
        Pattern country1Pattern = Pattern.compile(".*(Visa requirements for [\\w\\s]+ citizens)\\s*\\|([\\w\\s]+).*");
        Pattern country2Pattern = Pattern.compile(".*\\{\\{flag.*\\|([\\w\\s]+)\\}\\}");
        Pattern daysPattern = Pattern.compile(".*yes\\|(\\d+) day.*");
        Pattern monthsPattern = Pattern.compile(".*yes\\|(\\d+) month.*");
        String [] countryLines = getWikiRevision("Template:Visa_requirements").split("\n");
        
        for (String countryLine : countryLines){
            Matcher country1Matcher = country1Pattern.matcher(countryLine);
            if(country1Matcher.matches()){
                String wikiPage = country1Matcher.group(1);
                String nationality = country1Matcher.group(2);
                String country1 = (String) nationalities.get(nationality);
                if (country1 == null){
                    country1 = nationality;
                }
                String response = getWikiRevision(wikiPage);
                if(response != null){
                    String [] revisionLines = response.split("\n");
                    for(int i=0; i<revisionLines.length; i++){
                        Matcher country2Matcher = country2Pattern.matcher(revisionLines[i]);
                        if(country2Matcher.matches()){
                           String country2 = country2Matcher.group(1); 
                           Double duration = 0.0;
                           i++;
                           Matcher daysMatcher = daysPattern.matcher(revisionLines[i]);
                           Matcher monthsMatcher = monthsPattern.matcher(revisionLines[i]);
                           if(daysMatcher.matches()){
                               duration = Double.valueOf(daysMatcher.group(1));
                           } else if (monthsMatcher.matches()){
                               duration = Double.valueOf(monthsMatcher.group(1))*30;
                           }                      
                           if (duration >0){
                               graph.createRelationship(proxy, country2, duration/100, duration);
                               System.out.println(country1+","+country2+","+duration/100+","+duration);                            
                           }
                        }
                    }
                }
            }            
        }   
        graph.shutDown();
        System.out.println("Finished");
    }
    
    
    private String getWikiRevision(String request){
        String wikiRequest = "http://en.wikipedia.org/w/api.php?format=json&action=query&prop=revisions&rvprop=content&titles="; 
        String response = Helper.makeRequest(wikiRequest+request.replaceAll("\\s", "_"), proxy, port);
        String revisions = null;
        try {
            JSONObject jsonObject = new JSONObject(response);
            JSONObject page = (JSONObject) jsonObject.getJSONObject("query").get("pages");
            Iterator pagesKeys = page.keys();
            String pageKey = (String) pagesKeys.next();
            revisions = page.getJSONObject(pageKey).getJSONArray("revisions").getJSONObject(0).getString("*");
        } catch (JSONException ex) {
            //Logger.getLogger(Dump.class.getName()).log(Level.SEVERE, null, ex);
            return revisions;
        }
        return revisions;
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
     
     private void readNationalities(){
         nationalities = new Hashtable();
         String[] lines = Helper.readFile(NATIONALITIES_FILE).split(System.getProperty("line.separator"));
         for(String line:lines){
             String[] country = line.split(":");
             nationalities.put(country[1], country[0]);
         }
     }
}
