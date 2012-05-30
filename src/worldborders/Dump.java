/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package worldborders;
import java.io.File;
import java.util.ArrayList;
import org.neo4j.shell.util.json.JSONException;
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
    private String NATIONALITIES_FILE;
    private Pattern country1Pattern;     
    private Pattern durationPattern;
    private Pattern freedomPattern;
    private Pattern codePattern;
    private Pattern namePattern;
    private Pattern visaPattern;
    
    public Dump(String dir, int maxRequests){
        this.proxy = null;
        this.port = -1;
        this.dir = dir;   
        init();
    }
    
    public Dump(String dir, int maxRequests, String proxy, int port){
        this.proxy = proxy;
        this.port = port;
        this.dir = dir;    
        init();
    }
    
    private void init(){
        NATIONALITIES_FILE = System.getProperty("user.dir")+"/src/worldborders/country_codes.csv";
        graph = new Graph(dir, dir+"graph.db", true);
        readNationalities();
        compilePatterns();     
    }
    
    private void compilePatterns(){
        country1Pattern = Pattern.compile(".*(Visa requirements for [a-zA-Z\\s]+ citizens)\\s*\\|\\s*([a-zA-Z\\s]+).*");
        freedomPattern = Pattern.compile(".*(Free|free|FREE|Unlimited|unlimited|UNLIMITED|Freedom|freedom).*");
        
        durationPattern = Pattern.compile(".*?(\\d{1,3})\\s+(day|week|month).*");
        
        codePattern = Pattern.compile(".*?\\{\\{.*?\\|?([A-Z]{3})\\}\\}(.*)");
        namePattern = Pattern.compile(".*?\\{\\{.*?\\|?\\s*([A-Z][a-z][a-zA-Z\\s]+)\\}\\}(.*)");
        visaPattern = Pattern.compile(".*(arrival|Arrival|issued|Issued).*");
    }
    
    public void dumpToFiles(){
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
                        Helper.writeToFile(dir+country1, response);
                    }             
                }
        }
    }
    
    
    //select="users" or select="startups"
    public void getCountries(){ 
        File folder = new File(dir);
        File[] listOfFiles = folder.listFiles();
        int count = 0;
        for (File file : listOfFiles){
            boolean found = false;
            String country1 = "";
            if(file.isFile()){
                country1 = getCountryShortName(file.getName());
                String response = Helper.readFile(file.getPath());
                String [] revisionLines = response.split("\n"); 
                for(int i=0; i<revisionLines.length; i++){
                    Matcher nameMatcher = namePattern.matcher(revisionLines[i]);
                    Matcher codeMatcher = codePattern.matcher(revisionLines[i]);
                    Float duration = null;
                    String country2 = null;                    
                    if (codeMatcher.matches()){
                        //System.out.println(codeMatcher.group(1));
                        country2 = countryCodes.get(codeMatcher.group(1));
                        if (country2 == null){
                            continue;
                        }
                        if (codeMatcher.group(2).length()>0){
                            duration = getDuration(revisionLines[i]);
                        }
                        else {
                            if (i>=revisionLines.length-1){
                                duration = 30F;
                            }
                            else {
                                duration = getDuration(revisionLines[i+1]);
                                if (duration == null){
                                    duration = 30F;
                                }
                                else{
                                    i++;
                                }
                            }
                        }
                    }   
                    else if (nameMatcher.matches()){                        
                        country2 = getCountryShortName(nameMatcher.group(1));
                        if (nameMatcher.group(2).length()>0){
                            duration = getDuration(revisionLines[i]);
                        }
                        else {
                            if (i>=revisionLines.length-1){
                                duration = 30F;
                            }
                            else {
                                duration = getDuration(revisionLines[i+1]);
                                if (duration == null){
                                    duration = 30F;
                                }
                                else{
                                    i++                                    ;
                                }                                    
                            }
                        }                        
                    }                                           
                    if (duration != null && duration > 0){
                        graph.createRelationship(country1, country2, duration/100, duration);
                        System.out.println(country1+","+country2+","+duration/100+","+duration);    
                        found = true;
                    }
                }               
            }    
            if(found){
                count++;
            }
            else{
                System.out.println("Problem:"+country1+", Size:"+file.length());
            }                
        }                                                                                                              
        graph.shutDown();
        System.out.println("Finished:"+count+":"+listOfFiles.length);
    }
    
 
    private Float getDuration(String line){
        Float duration = null;
        Matcher freedomMatcher = freedomPattern.matcher(line);
        Matcher durationMatcher = durationPattern.matcher(line);
        Matcher visaMatcher = visaPattern.matcher(line);
        if (visaMatcher.matches()){
            return 0F;                               
        }
        if (freedomMatcher.matches()){
            return 360F;                               
        }
        if (durationMatcher.matches()){
            duration = Float.valueOf(durationMatcher.group(1));
            if ("month".equals(durationMatcher.group(2))){
                return duration * 30;
            } 
            if ("week".equals(durationMatcher.group(2))){
                return duration * 7;
            }
        }
        if (line.contains("yes")){
            return 30F;
        }
        return duration;
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
     
     private String  getCountryShortName(String country){
         String lowerCountry = country.toLowerCase();
         for(String name : nationalities.values()){
             if(lowerCountry.contains(name.toLowerCase())){
                 return name;
             }
             if(name.toLowerCase().contains(lowerCountry)){
                 return name;
             }
         }

         for(String denonym : nationalities.keySet()){
             if(denonym.toLowerCase().contains(country)){
                 return nationalities.get(denonym);
             }             
         }         
         return country;
     }
}
