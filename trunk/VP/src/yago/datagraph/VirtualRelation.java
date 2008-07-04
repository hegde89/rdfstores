package yago.datagraph;
import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.regex.*;

import yago.javatools.*;

/** 
 * This class is part of the YAGO distribution. Its use is subject to the
 * licence agreement at http://mpii.de/yago
 * 
 * This class represents relations that are not stored in the database, but 
 * computed on the fly. Examples are smallerThan, greaterThan etc.
 * 
 * These relations have a method "holds", which computes whether the relation holds
 * between two entities (given as Strings).
 * 
 * @author Fabian M. Suchanek
 */
public abstract class VirtualRelation extends Relation {
  public static final VirtualRelation GREATERTHAN = new VirtualRelation(">",YagoClass.NUMBER,YagoClass.NUMBER,false,false) {
    public boolean holds(String x, String y) {
      try {
        return(NumberParser.toDouble(x)>NumberParser.toDouble(y));
      }
      catch(NumberFormatException e) {
        return(false);
      }
    }    
  };
  public static final VirtualRelation SMALLERTHAN = new VirtualRelation ("<",YagoClass.LITERAL,YagoClass.LITERAL,false,false) {
    public boolean holds(String x, String y) {
      try{
        return(NumberParser.toDouble(x)<NumberParser.toDouble(y));
      }catch(Exception e) {
        return(false);
      }
    }    
  };  
  public static final VirtualRelation BEFORE = new VirtualRelation ("before",YagoClass.TIMEINTERVAL,YagoClass.TIMEINTERVAL,false,false) {
    public boolean holds(String x, String y) {       
      try{
        Calendar xc=DateParser.asCalendar(DateParser.normalize(x));
        Calendar yc=DateParser.asCalendar(DateParser.normalize(y));
        return(xc.before(yc));
      }catch(Exception e) {
        return(false);
      }
    }    
  };
  public static final VirtualRelation AFTER= new VirtualRelation ("after",YagoClass.TIMEINTERVAL,YagoClass.TIMEINTERVAL,false,false) {
    public boolean holds(String x, String y) {       
      try{
        Calendar xc=DateParser.asCalendar(DateParser.normalize(x));
        Calendar yc=DateParser.asCalendar(DateParser.normalize(y));
        return(yc.before(xc));
      }catch(Exception e) {
        return(false);
      }
    }    
    };

  public VirtualRelation(String name, YagoClass domain, YagoClass range, boolean isTransitive, boolean isFunction) {
    super(name, domain, range, isTransitive, isFunction);  
  }
  
  /** Tells whether this relation holds between two Strings */
  public abstract boolean holds(String x, String y);
  
  /** List of all relations declared in this class*/
  public static List<Relation> virtualValues=new ArrayList<Relation>(20);
  static {
    for(Field f : VirtualRelation.class.getFields()) {      
      if(!f.getDeclaringClass().equals(yago.datagraph.VirtualRelation.class)) continue;
      try {
        virtualValues.add((VirtualRelation)f.get(null));
      }
      catch(Exception e) {
        // This won't happen, because we're inside the Relation class
      }          
    }
  }

  /** List of all virtual relations declared in this class.
   * Cannot be a list of VirtualRelations, because the superclass says "List of Relation"*/    
  public static List<Relation> values() {
    return(virtualValues);
  }

  /** Maps a String to a Relation (or to null)*/
  public static VirtualRelation valueOf(String s) {
    for(Relation r : values()) {
      if(r.name().equalsIgnoreCase(s)) return((VirtualRelation)r);
    }
    return(null);
  }
}
