/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package worldborders;

/**
 *
 * @author rafael
 */
public class WorldBorders {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String proxy = "proxy-west.uec.ac.jp";
        String dir = "/home/rafael/world_borders_dump/";
        int port = 8080;
        int maxRequestsPerHour = 1000;
        
        boolean newDump = false;        
        boolean newGraph = false;
        
        Graph graph = new Graph(dir, dir+"graph.db", newGraph);
        
        Dump dump = new Dump(dir, maxRequestsPerHour, graph, proxy, port);
        
        //Get files from wikipedia and save the locally
        if(newDump)
            dump.dumpToFiles();
        
        //Get countries and make graph from relationships
        if(newGraph)
            dump.getCountries();

        //Export graph to google chart
        graph.graphToGoogleGeochart();
        
        //Shut down...
        graph.shutDown();
        
    }
}
