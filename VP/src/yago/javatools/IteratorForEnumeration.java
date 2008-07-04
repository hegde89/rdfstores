package yago.javatools;

import java.util.*;

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
  
  This class provides a simple converter from Enumerations to Iterators.
  It accepts untyped Enumerations, but yields always typed Iterators.
  It can also convert an untyped Enumeration to a list.<BR>
  Example:<BR>
  <PRE>
    for(String s : new IteratorForEnumeration&lt;String>(someUntypedEnumeration)) {
      System.out.println(s);
    }
  </PRE>
  */
public class IteratorForEnumeration<T> implements Iterator<T>, Iterable<T> {

  /** Holds the enumeration object*/
  public Enumeration<T> enumerator;
    
  @SuppressWarnings("unchecked")
  public IteratorForEnumeration(Enumeration enumerator) {   
    this.enumerator=enumerator;
  }

  public boolean hasNext() {
    return(enumerator.hasMoreElements());
  }

  public T next() {
    return(enumerator.nextElement());
  }

  public void remove() {
   throw new UnsupportedOperationException("Remove on IteratirForEnumeration");
  }

  public Iterator<T> iterator() {    
    return this;
  }
  
  /** Returns the rest of the enumeration as a list*/
  public List<T> asList() {
    return Collections.list(enumerator);
  }

}
