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
    private Hashtable<String, Country> countries;
    private String NATIONALITIES_FILE;
    private Pattern country1Pattern;     
    private Pattern durationPattern;
    private Pattern freedomPattern;
    private Pattern codePattern;
    private Pattern namePattern;
    private Pattern visaPattern;
    private Pattern EUPattern;
    private Pattern continentPattern;
    private Pattern sectionPattern;
    
    public Dump(String dir, int maxRequests, Graph graph){
        this.proxy = null;
        this.port = -1;
        this.dir = dir;   
        init(graph);
    }
    
    public Dump(String dir, int maxRequests, Graph graph, String proxy, int port){
        this.proxy = proxy;
        this.port = port;
        this.dir = dir;    
        init(graph);
    }
    
    
    private void init(Graph graph){
        NATIONALITIES_FILE = System.getProperty("user.dir")+"/src/worldborders/country_codes.csv";
        this.graph = graph;
        readNationalities();
        compilePatterns();     
    }
    
    private enum EUCodes{
       AUT, BEL, BGR, CYP, CZE, DNK, EST, FIN, FRA, DEU, GBR, GRC, HUN, IRL, ITA, LVA, LTU, LUX, MLT, NLD, POL, PRT, ROU, SVK, SVN, ESP, SWE;
   }
    
    private void compilePatterns(){
        country1Pattern = Pattern.compile(".*(Visa requirements for [a-zA-Z\\s]+ citizens)\\s*\\|\\s*([a-zA-Z\\s]+).*");
        freedomPattern = Pattern.compile(".*([Ff]ree|[Uu]nlimited).*");
        
        durationPattern = Pattern.compile(".*?(\\d{1,3})\\s+(day|week|month).*");
        
        EUPattern = Pattern.compile(".*?\\{\\{.*?\\|?([Ee][Uu]|[Ee]uropean [Uu]nion|[Ss]chengen [Aa]rea)\\}\\}(.*)");
        codePattern = Pattern.compile(".*?\\{\\{.*?\\|?([A-Z]{3})\\}\\}(.*)");
        namePattern = Pattern.compile(".*?\\{\\{.*?\\|?\\s*([A-Z][a-zA-Z\\s]+)\\}\\}(.*)");
        visaPattern = Pattern.compile(".*(VOA|[Aa]rrival|[Ii]ssue|[Ss]ingle|[Hh]old|[Tt]ransit|[Ee]lectronic|[Aa]long|[Vv]alid).*");
        sectionPattern = Pattern.compile(".*?={2,}.*?={2,}.*");
        continentPattern = Pattern.compile(".*(America|Africa|Asia|Europe|Oceania|CARICOM|Pacific|Caribbean|ASEAN|EU).*");
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
                        Helper.writeToFile(dir+country1, response, false);
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
            Country country1 = null;
            if(file.isFile()){
                String code1 = getCountryCode(file.getName());
                if (code1==null){
                    continue;
                }
                System.out.println(file.getName()+","+code1);
                country1 = countries.get(code1);
                String response = Helper.readFile(file.getPath());
                String [] revisionLines = response.split("\n"); 
                boolean inContinent = false;
                for(int i=0; i<revisionLines.length; i++){
                    boolean foundEU = false;
                    Matcher sectionMatcher = sectionPattern.matcher(revisionLines[i]);
                    if(sectionMatcher.matches()){
                        inContinent = false;
                        Matcher continentMatcher = continentPattern.matcher(revisionLines[i]);
                        if(continentMatcher.matches()){
                            inContinent = true;
                        }                        
                    }
                    if(!inContinent){
                        continue;
                    }                    
                    Matcher nameMatcher = namePattern.matcher(revisionLines[i]);
                    Matcher codeMatcher = codePattern.matcher(revisionLines[i]);
                    Matcher EUMatcher = EUPattern.matcher(revisionLines[i]);
                    Float duration = null;
                    Country country2 = null;
                    if (codeMatcher.matches()){
                        //System.out.println(codeMatcher.group(1));
                        country2 = countries.get(codeMatcher.group(1));
                        if (country2 == null){
                            continue;
                        }
                        duration = getDuration(revisionLines[i]);                        
                        if (duration == null) {
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
                    else if(EUMatcher.matches()){                    
                        duration = getDuration(revisionLines[i]);
                        if (duration == null){
                            duration = getDuration(revisionLines[i+1]);
                            if (duration == null){
                                duration = 30F;
                            }
                            else{
                                i++;
                            }                                    
                        }
                        if (duration != null && duration > 0){                            
                            for (EUCodes euCode : EUCodes.values()){
                                country2 = countries.get(euCode.toString());                                 
                                graph.createRelationship(country1, country2, duration/100, duration);
                                System.out.println(country1.NAME+","+country2.NAME+","+duration/100+","+duration);                                    
                            }
                            found = true;                            
                            foundEU = true;
                        }                            
                    }  
                    else if (nameMatcher.matches()){ 
                        String code2 = getCountryCode(nameMatcher.group(1));
                        if (code2 == null){
                            continue;
                        }
                        country2 = countries.get(code2);                      
                        duration = getDuration(revisionLines[i]);                        
                        if (duration == null) {
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
                    if (duration != null && duration > 0 && !foundEU){
                        graph.createRelationship(country1, country2, duration/100, duration);
                        System.out.println(country1.NAME+","+country2.NAME+","+duration/100+","+duration);    
                        found = true;                       
                    }
                }               
            }    
            if(found){
                count++;
            }
            else{
                System.out.println("Problem:"+file.getName()+", Size:"+file.length());
            }                
        }                                                                                                              
        System.out.println("Finished:"+count+":"+listOfFiles.length);
    }
    
 
    private Float getDuration(String line){
        Float duration = null;
        Matcher freedomMatcher = freedomPattern.matcher(line);
        Matcher durationMatcher = durationPattern.matcher(line);
        Matcher visaMatcher = visaPattern.matcher(line);
        if (line.contains("{{yes") || line.contains("{{Yes")){
            if (durationMatcher.matches()){
                duration = Float.valueOf(durationMatcher.group(1));
                if ("month".equals(durationMatcher.group(2))){
                    return duration * 30;
                } 
                if ("week".equals(durationMatcher.group(2))){
                    return duration * 7;
                }
            }    
            return 90F;
        }
        if (visaMatcher.matches() || line.contains("{{No") || line.contains("{{no")){
            return 0F;                               
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
        if (freedomMatcher.matches()){
            return 90F;                               
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
         countries = new Hashtable<String, Country>();
         String[] lines = Helper.readFile(NATIONALITIES_FILE).split(System.getProperty("line.separator"));
         for (int i=1; i<lines.length; i++){
             String[] line = lines[i].split(",");
             //System.out.println(i+","+lines[i]);
             if (!"".equals(line[0])){                            
                Country country = new Country(line[0], line[1], line[2], Float.valueOf(line[6]), Float.valueOf(line[7]));
                countries.put(line[0], country);
                nationalities.put(line[2], line[0]);
                countryCodes.put(line[1], line[0]);
             }
         }
     }
     
     private String  getCountryCode(String country){
         String lowerCountry = country.toLowerCase();         
         for(String name : countryCodes.keySet()){
             if(name.toLowerCase().contains(lowerCountry) || lowerCountry.contains(name.toLowerCase())){
                 return countryCodes.get(name);
             }
         }         
         return null;
     }
}
