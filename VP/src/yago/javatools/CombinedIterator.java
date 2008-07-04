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
  
  The class combines multiple iterators to one iterator.
  The nice thing about it:  The object is an Iterator as well as an Iterable,
  i.e. it can be used in a for-each-loop.<BR>
  Example:
   <PRE>
         for(Object o : new CombinedIterator(list1.iterator(),list2.iterator()))
               process(o);
   </PRE>
  */
public class CombinedIterator<T> implements  Iterator<T>, Iterable<T> {
  /** Holds the queue of iterators */
  private Queue<Iterator<? extends T>> iterators=new LinkedList<Iterator<? extends T>>();
  /** Creates an empty CombinedIterator */
  public CombinedIterator() {
  }
  /** Creates a CombinedIterator two iterators */
  public CombinedIterator(Iterator<? extends T> i1, Iterator<? extends T> i2) {
    iterators.offer(i1);
    iterators.offer(i2);
  }  
  /** Creates a CombinedIterator from some iterators */
  public CombinedIterator(Iterator<? extends T>... its) {
    for(Iterator<? extends T> i : its) iterators.offer(i);
  }
  /** Adds an iterator */
  public CombinedIterator<T> add(Iterator<? extends T> i) {
    iterators.offer(i);
    return(this);
  }
  /** TRUE if there are more elements */
  public boolean hasNext() {
    if(iterators.peek()==null) return(false);
    if(iterators.peek().hasNext()) return(true);
    iterators.remove();
    return(hasNext());
  }
  /** Returns next */
  public T next() {
    if(!hasNext()) return(null);
    return(iterators.peek().next());
  }
  /** Returns this */
  public Iterator<T> iterator() {
    return(this);
  }
  /** Does nothing */
  public void remove(){}
}
