/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package worldborders;

/**
 *
 * @author rafael
 */
public class Country {
    
    public String NAME;
    public String CODE;
    public String DENONYM;
    public Float LATITUDE;
    public Float LONGITUDE;
    
    public Country(String code, String name, String denonym, Float latitude, Float longitude){
        NAME = name;
        CODE = code;
        DENONYM = denonym;
        LATITUDE = latitude;
        LONGITUDE = longitude;
    }
    
}
