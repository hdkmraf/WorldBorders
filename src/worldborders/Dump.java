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
    private Hashtable<String, String> nationalities;
    private Hashtable<String, String> countryCodes;
    private String NATIONALITIES_FILE = "src/worldborders/country_codes.csv";
    
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
        Pattern country1Pattern = Pattern.compile(".*(Visa requirements for [\\w\\s]+ citizens)\\s*\\|\\s*([\\w\\s]+).*");
        
        Pattern country2Pattern = Pattern.compile(".*\\{\\{flag.*\\|\\s*([\\w\\s]+)\\}\\}");        
        Pattern daysPattern = Pattern.compile(".*yes\\|(\\d+) day.*");
        Pattern monthsPattern = Pattern.compile(".*yes\\|(\\d+) month.*");
        Pattern freedomPattern = Pattern.compile(".*yes.*Freedom of movement.*");
        Pattern codePattern = Pattern.compile(".*\\{\\{.*\\|?([A-Z]{3})\\}\\}\\s+(\\d+)\\s+(day|month)s?");
        Pattern namePattern = Pattern.compile(".*?\\{\\{.*?\\|??\\s*?([\\w\\s]+)\\}\\}\\s+?(\\d+)\\s+?(day|month)s?");
        
        String [] countryLines = getWikiRevision("Template:Visa_requirements").split("\n");
        
        for (String countryLine : countryLines){
            Matcher country1Matcher = country1Pattern.matcher(countryLine);
            if(country1Matcher.matches()){
                String wikiPage = country1Matcher.group(1);
                String nationality = country1Matcher.group(2);
                String country1 = nationalities.get(nationality);
                if (country1 == null){
                    country1 = nationality;
                }
                String response = getWikiRevision(wikiPage);
                if(response != null){
                    String [] revisionLines = response.split("\n");
                    
                    for(int i=0; i<revisionLines.length; i++){
                        Matcher country2Matcher = country2Pattern.matcher(revisionLines[i]);
                        float duration = 0;
                        String country2 = "";
                        if(country2Matcher.matches()){
                           country2 = country2Matcher.group(1);                           
                           i++;
                           Matcher daysMatcher = daysPattern.matcher(revisionLines[i]);                                                      
                           if(daysMatcher.matches()){
                               duration = Float.valueOf(daysMatcher.group(1));
                           }
                           else {
                               Matcher monthsMatcher = monthsPattern.matcher(revisionLines[i]);
                               if (monthsMatcher.matches()){
                                    duration = Float.valueOf(monthsMatcher.group(1))*30;
                               }
                               else {
                                   Matcher freedomMatcher = freedomPattern.matcher(revisionLines[i]);
                                   if(freedomMatcher.matches()){
                                        duration = 360;                               
                                   }
                               }     
                           }                                                  
                        }
                        
                        else {
                            Matcher codeMatcher = codePattern.matcher(revisionLines[i]);
                            if (codeMatcher.matches()){
                                country2 = countryCodes.get(codeMatcher.group(1));
                                duration = Float.valueOf(codeMatcher.group(2));
                                if("month".equals(codeMatcher.group(3))){
                                    duration *= 30;
                                }
                            }
                            else{
                                Matcher nameMatcher = namePattern.matcher(revisionLines[i]);
                                if(nameMatcher.matches()){
                                    country2 = nameMatcher.group(1);
                                    duration = Float.valueOf(nameMatcher.group(2));
                                    if("month".equals(nameMatcher.group(3))){
                                        duration *= 30;
                                    }   
                                }
                            }
                        }
                        
                        if (duration >0){
                            graph.createRelationship(country1, country2, duration/100, duration);
                            System.out.println(country1+","+country2+","+duration/100+","+duration);    
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
    
     
     private void readNationalities(){
         nationalities = new Hashtable<String, String>();
         countryCodes = new Hashtable<String, String>();
         String[] lines = Helper.readFile(NATIONALITIES_FILE).split(System.getProperty("line.separator"));
         for(String line:lines){
             String[] country = line.split(",");
             nationalities.put(country[2], country[1]);
             countryCodes.put(country[0], country[1]);
         }
     }
}
