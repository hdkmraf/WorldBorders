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
        
        Dump dump = new Dump(dir, maxRequestsPerHour, proxy, port);
        dump.getCountries();
    }
}
