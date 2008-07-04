package yago.datagraph;
import yago.javatools.*;

/**
* This class is part of the YAGO distribution. Its use is subject to the
* licence agreement at http://mpii.de/yago
* 
* This class represents a labeled edge in a graph 
* 
* @author  Maya Ramanath
* 
* */
public class LabeledEdge<T,L> extends Edge<T> {
  /** Holds the label*/
	protected L label;
	
	public LabeledEdge () {
		
	}
	public LabeledEdge (T n1, T n2, L l) {
		super (n1, n2);
		label = l;
	}
	
	public L label () { return label; }
	
  public boolean equals(Object obj) {
    if(obj == null || !(obj instanceof LabeledEdge)) return(false);
    LabeledEdge other=(LabeledEdge)obj;    
    return other.label.equals(this.label) && other.n1.equals(this.n1) && other.n2.equals(this.n2) ;
  }
  
  public int hashCode() {  
    return n1.hashCode()^n2.hashCode()^label.hashCode();
  }
    
	public String toString () {
		return (n1+ " --" + label + "--> " + n2);
	}
}
