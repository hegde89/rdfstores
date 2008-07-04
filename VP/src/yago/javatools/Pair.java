package yago.javatools;

/** This class is part of the
<A HREF=http://www.mpi-inf.mpg.de/~suchanek/downloads/javatools target=_blank>
          Java Tools
</A> by <A HREF=http://www.mpi-inf.mpg.de/~suchanek target=_blank>
          Fabian M. Suchanek</A>
  You may use this class if (1) it is not for commercial purposes,
  (2) you give credit to the author and (3) you use the class at your own risk.
  If you use the class for scientific purposes, please cite our paper
  "Combining Linguistic and Statistical Analysis to Extract Relations from Web Documents"
  (<A HREF=http://www.mpi-inf.mpg.de/~suchanek/publications/kdd2006.pdf target=_blank>pdf</A>,
  <A HREF=http://www.mpi-inf.mpg.de/~suchanek/publications/kdd2006.bib target=_blank>bib</A>,
  <A HREF=http://www.mpi-inf.mpg.de/~suchanek/publications/kdd2006.ppt target=_blank>ppt</A>
  ). If you would like to use the class for commercial purposes, please contact
  <A HREF=http://www.mpi-inf.de/~suchanek>Fabian M. Suchanek</A><P>

This class provides the simple datatype of a pair */ 
public class Pair<F,S> implements Comparable<Pair<F,S>> {
  /** Holds the first component */
  public F first;
  /** Holds the second component */  
  public S second;
  /** Returns the first */
  public F first() {
    return first;
  }
  /** Sets the first */
  public void setFirst(F first) {
    this.first=first;
  }
  /** Returns the second */
  public S second() {
    return second;
  }
  /** Sets the second */
  public void setSecond(S second) {
    this.second=second;
  }
  
  /** Constructs a Pair*/
  public Pair(F first, S second) {
    super();
    this.first=first;
    this.second=second;
  }  

  public int hashCode() {
    return(first.hashCode()^second.hashCode());
  }
  /** Returns "first/second"*/
  public String toString() {
    return first+"/"+second;
  }
  
  @SuppressWarnings("unchecked")
  public int compareTo(Pair<F, S> o) {
    int firstCompared=((Comparable<F>)first).compareTo(o.first());
    if(firstCompared!=0) return(firstCompared);
    return(((Comparable<S>)second).compareTo(o.second()));
  }
}
