package yago.datagraph;

import java.lang.reflect.*;
import java.util.*;

/** 
 * This class is part of the YAGO distribution. Its use is subject to the
 * licence agreement at http://mpii.de/yago
 * 
 * This class represents a YAGO class.<P>
 * 
 * Yago already imports tons of classes from WordNet. Some of them are of
 * particular importance for the Extractors/Generators and are thus represented
 * explicitly as YagoClasses. This class also defines YagoClasses that are not
 * in WordNet.
 * Relationjava converts these classes to the text tables.  <P>
 * 
 * Relationjava does not need to be adjusted if a YagoClass is added.
 * This class does need to be adjusted if a new version of WordNet is used! 
 * (currently 3.0)
 *
 * @author Fabian M. Suchanek*/

public class YagoClass implements Comparable<YagoClass> {
  public static final YagoClass PERSON=new YagoClass(wordNetId("person", 100007846));   
  public static final YagoClass UNIT=new YagoClass(wordNetId("unit_of_measurement", 113583724));
  public static final YagoClass COMPANY=new YagoClass(wordNetId("company", 108058098));   
  public static final YagoClass CITY=new YagoClass(wordNetId("city", 108524735));
  public static final YagoClass GEOPOLITICAL=new YagoClass(wordNetId("location", 100027167));
  public static final YagoClass COUNTRY=new YagoClass(wordNetId("country", 108544813));
  public static final YagoClass PHYSICAL=new YagoClass(wordNetId("physical_object", 100002684));
  public static final YagoClass MOVIE=new YagoClass(wordNetId("movie", 106613686));
  public static final YagoClass ENTITY=new YagoClass(wordNetId("entity", 100001740));
  public static final YagoClass COMPUTERSYSTEM=new YagoClass(wordNetId("computer_system", 103085915));
  public static final YagoClass COMPUTERSCIENTIST=new YagoClass(wordNetId("computer_scientist", 109951070));
  public static final YagoClass PRIZE=new YagoClass(wordNetId("award", 106696483));  
  public static final YagoClass LANGUAGE=new YagoClass(wordNetId("language", 106282651));
  public static final YagoClass CLASS=new YagoClass("yagoClass",ENTITY);  
  public static final YagoClass RELATION=new YagoClass("yagoRelation",ENTITY);  
  public static final YagoClass TRANSITIVERELATION=new YagoClass("yagoTransitiveRelation",RELATION);  
  public static final YagoClass SYMMETRICRELATION=new YagoClass("yagoSymmetricRelation",RELATION);  
  public static final YagoClass FUNCTION=new YagoClass("yagoFunction",RELATION);
  public static final YagoClass FACT=new YagoClass("yagoFact",ENTITY);
  public static final YagoClass LITERAL=new YagoClass("yagoLiteral",ENTITY);  
  public static final YagoClass BOOLEAN=new YagoClass("yagoBoolean",LITERAL);  
  public static final YagoClass NUMBER=new YagoClass("yagoNumber", LITERAL);  
  public static final YagoClass RATIONAL=new YagoClass("yagoRationalNumber",NUMBER);  
  public static final YagoClass INTEGER=new YagoClass("yagoInteger",RATIONAL);  
  public static final YagoClass NONNEGATIVEINTEGER=new YagoClass("yagoNonNegativeInteger",INTEGER);  
  public static final YagoClass STRING=new YagoClass("yagoString",LITERAL);  
  public static final YagoClass WORD=new YagoClass("yagoWord",STRING);  
  public static final YagoClass URL=new YagoClass("yagoURL",STRING);  
  public static final YagoClass TLD=new YagoClass("yagoTLD",STRING);
  public static final YagoClass CHAR=new YagoClass("yagoChar",STRING);  
  public static final YagoClass TIMEINTERVAL=new YagoClass("yagoTimeInterval",LITERAL);  
  public static final YagoClass TIMEPOINT=new YagoClass("yagoTimePoint",TIMEINTERVAL);  
  public static final YagoClass DATE=new YagoClass("yagoDate",TIMEINTERVAL);  
  public static final YagoClass YEAR=new YagoClass("yagoYear",TIMEINTERVAL);  
  public static final YagoClass QUANTITY=new YagoClass("yagoQuantity",LITERAL);
  public static final YagoClass LENGTH=new YagoClass("yagoLength",QUANTITY);
  public static final YagoClass AREA=new YagoClass("yagoArea",QUANTITY);
  public static final YagoClass WEIGHT=new YagoClass("yagoWeight",QUANTITY);
  public static final YagoClass DENSITYPERAREA=new YagoClass("yagoDensityPerArea",QUANTITY);
  public static final YagoClass DURATION=new YagoClass("yagoDuration",QUANTITY);  
  public static final YagoClass MONETARYVALUE=new YagoClass("yagoMonetaryValue",QUANTITY);
  public static final YagoClass IDENTIFIER=new YagoClass("yagoIdentifier",STRING);
  public static final YagoClass CALLINGCODE=new YagoClass("yagoCallingCode",NONNEGATIVEINTEGER);
  public static final YagoClass ISBN=new YagoClass("yagoISBN",IDENTIFIER);
  public static final YagoClass PROPORTION=new YagoClass("yagoProportion",RATIONAL);
  
  /** Holds the name of the class */
  public String name;
  /** Holds the superclasses */
  public YagoClass[] superclass;
  
  /** Constructor with full data*/
  public YagoClass(String name, YagoClass... superclass) {
    this.name=name;
    this.superclass=superclass;
  }
  
  /** Constructor with full data*/
  public YagoClass(String name) {
    this(name, new YagoClass[0]);
  }
  
  /**Returns the name */
  public String getName() {
    return name;
  }

  /** Returns the superclass */
  public YagoClass[] getSuperclasses() {
    return superclass;
  }
 
  /** Tells whether this class is a subclass of another (only within the YagoClasses)*/
  public boolean isSubClassOf(YagoClass s) {
    if(this==s) return(true);
    if(superclass.length==0) return(false);
    for(YagoClass y : superclass) {
      if(y.isSubClassOf(s)) return(true);
    }
    return(false);
  }

  /** Holds all classes */
  protected static List<YagoClass> values=null;

  /** Returns all relations */
  public static List<YagoClass> values() {
    if (values == null) {
      values = new ArrayList<YagoClass>(20);
      for (Field f : YagoClass.class.getFields()) {
        if (!f.getDeclaringClass().equals(yago.datagraph.YagoClass.class)) continue;
        try {
          values.add((YagoClass) f.get(null));
        } catch (Exception e) {
          // This won't happen, because we're inside the YagoClass class
        }
      }
    }
    return (values);
  }

  /** Maps a String to a Class or NULL */
  public static YagoClass valueOf(String s) {
    for(YagoClass c : values()) {
      if(c.name.equals(s)) return(c);
    }
    return(null);
  }
  
  /** Returns the name of this class */
  public String toString() {
    return(name);
  }

  /** returns a wordnet identifier*/
  public static String wordNetId(String s,int id) {
    return("wordnet_"+(s.replace(' ','_'))+"_"+id);
  }

  @Override
  public int hashCode() {    
    return name.hashCode();
  }
  
  public int compareTo(YagoClass o) {    
    return this.name.compareTo(o.name);
  }

}
