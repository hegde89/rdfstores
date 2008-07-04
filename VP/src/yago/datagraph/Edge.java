package yago.datagraph;

/**
 * This class is part of the YAGO distribution. Its use is subject to the
 * licence agreement at http://mpii.de/yago
 * 
 * Represents an edge between two nodes
 * 
 * @author Maya Ramanath
 *
 */
public class Edge<T> {
	T n1, n2;
	
	public Edge () { }
	
	public Edge (T n1, T n2) {
		this.n1 = n1;
		this.n2 = n2;
	}
			
	public T source () { return n1; }
	public T destination () { return n2; }
	
  public int hashCode() {  
    return n1.hashCode()^n2.hashCode();
  }
    
	public boolean equals(Object e) {
    if(e==null || !(e instanceof Edge)) return(false);
    return(n1.equals(((Edge)e).n1) && n2.equals(((Edge)e).n2));
	}

  public String toString() {
    return(n1.toString()+"--"+n2.toString());
  }
}
