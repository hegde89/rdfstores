package yago.javatools;
import java.io.*;
import java.util.*;

import yago.javatools.*;

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

This class provides a stack for simple datatypes (int, float, boolean, double etc.).
It functions without wrapping/unwrapping. 
Example:
<PRE>
   SmallStack s=new SmallStack();
   s.push(7);
   D.p(s.popInt());
</PRE>
*/
public class SmallStack  {
  protected long[] values=new long[10];
  protected int nextFree=0;
  
  public SmallStack() {
   
  }
  public SmallStack(double d) {
    this();
    push(d);
  }
  public SmallStack(long d) {
    this();
    push(d);
  }
  public SmallStack(float d) {
    this();
    push(d);
  }
  
  public long push(long l) {
    if(nextFree==values.length) {
      long[] oldval=values;
      values=new long[values.length+10];
      System.arraycopy(oldval, 0, values, 0, nextFree);
    }
    return(values[nextFree++]=l);
  }
  public boolean push(boolean b) {
    push(b?1:0);
    return(b);
  }
  public double push(double d) {
    push(Double.doubleToRawLongBits(d));
    return(d);
  }
  public double push(float d) {
    push(Float.floatToIntBits(d));
    return(d);
  }
  
  public long peekLong() {
    if(nextFree==0) throw new NoSuchElementException("SmallStack is empty");
    return(values[nextFree]);
  }

  public boolean peekBoolean() {
    return(peekLong()==1);
  }

  public int peekInt() {
    return((int)peekLong());
  }

  public float peekFloat() {
    return(Float.intBitsToFloat((int)peekLong()));
  }

  public double peekDouble() {
    return(Double.longBitsToDouble(peekLong()));
  }

  public long popLong() {
    if(nextFree==0) throw new NoSuchElementException("SmallStack is empty");
    return(values[--nextFree]);
  }

  public boolean popBoolean() {
    return(popLong()==1);
  }

  public int popInt() {
    return((int)popLong());
  }

  public float popFloat() {
    return(Float.intBitsToFloat((int)popLong()));
  }

  public double popDouble() {
    return(Double.longBitsToDouble(popLong()));
  }

  public int size() {
    return(nextFree);
  }
  
  public boolean empty() {
    return(size()==0);
  }
  
  public int search(long l) {
    for(int i=0;i<nextFree;i++) if(values[i]==l) return(i);
    return(-1);
  }

  public int search(double d) {
    return(search(Double.doubleToRawLongBits(d)));
  }

  public int search(boolean d) {
    return(search(d?1:0));
  }
  
  public boolean equals(Object o) {
    if(!(o instanceof SmallStack)) return(false);
    SmallStack other=(SmallStack)o;
    if(nextFree!=other.nextFree) return(false);
    for(int i=0;i<nextFree;i++) {
      if(values[i]!=other.values[i]) return(false);
    }
    return(true);
  }
  
  public int hashCode() {
    int result=0;
    for(int i=0;i<nextFree;i++) {
      result^=values[i];
    }
    return(result);
  }

}
