package yago.datagraph;

import java.io.*;
import java.sql.*;

import yago.configuration.DBConfig;
import yago.javatools.*;

/** 
 * This class is part of the YAGO distribution. Its use is subject to the
 * licence agreement at http://mpii.de/yago
 * 
 * Represents a Fact 
 * 
 * @author Fabian M. Suchanek
 * */
public class Fact extends WeightedLabeledEdge<Entity, Relation, Float> implements Comparable<Fact> {
	
	/**  For the construction of a new fact */
	public Fact (Entity e1, Entity e2, Relation r, Float w) {
		super (e1, e2, r, w);
	}
	
  public int compareTo(Fact o) {
    if(n1.compareTo(o.n1)!=0) return(n1.compareTo(o.n1));
    if(n2.compareTo(o.n2)!=0) return(n2.compareTo(o.n2));    
    return(label.compareTo(o.label));
  }
  
  public void toHTMLString(Writer out) throws IOException {
    out.write(n1.toHTMLString());
    out.write(" &nbsp;&nbsp;&mdash;"+label.toString()+"&mdash;> &nbsp;&nbsp;");
    out.write(n2.toHTMLString()+"\n");
  }
}
