package yago.javatools;
import java.util.Iterator;

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

This class allows to use an iterator in a for-each-loop.<BR>
Example:
<PRE>
   for(String s : new Scanner("Scan this string")) {
      // Compiletime error, because Scanner is an Iterator but not Iterable
   }
   
   for(String s : new IterableForIterator&lt;String>(new Scanner("Scan this string"))) {
     // works fine
   }
   
</PRE>
 */ 
public class IterableForIterator<T> implements Iterable<T> {
  public Iterator<T> iterator;
  
  public IterableForIterator(Iterator<T> iterator) {
    this.iterator=iterator;
  }

  public Iterator<T> iterator() {
    return iterator;
  }

}
