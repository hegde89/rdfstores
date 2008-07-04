package yago.datagraph;
import yago.javatools.*;

/**
 * This class is part of the YAGO distribution. Its use is subject to the
 * licence agreement at http://mpii.de/yago
 * 
 * This class represents an Entity
 * 
 * @author Fabian M. Suchanek and Maya Ramanath
 *
 */
public class Entity implements Comparable<Entity> {
  /** Holds the name of the entity*/
	protected String name;

  public Entity(String n) {
    this.name=n;
  }
  
	public String name () { return name; }
	
  /** Compares by name*/
	public boolean equals (Object o){
    if(!(o instanceof Entity)) return(false);
		return (((Entity)o).name.equals(name)); 
  }
	
  /** Compares by name*/
	public int compareTo (Entity e) {
    return(this.name.compareTo(e.name));
	}
	
  /** Hashes by name*/
  public int hashCode() {  
    return name.hashCode();
  }
  
  /** Returns name*/
	public String toString () { return name; }
  
  /** Returns the name as HTML */
  public String toHTMLString() {
    String s=Char.toHTML(name());
    if(s.startsWith("wikicategory_")) s=s.substring(s.indexOf('_')+1);
    if(s.startsWith("wordnet_")) s=s.substring(s.indexOf('_')+1);    
    return(s);
  }

}
