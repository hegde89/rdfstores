package yago.javatools;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.io.*;

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
  
  This class wraps a ResultSet into an Iterator over a given Class.
  It requires a method that can wrap a row of a ResultSet into an object 
  of the given class.<BR>
  Example:
  <PRE>
  // We need this class to define how to construct an Employer from a table row
  public static class EmployerWrapper implements ResultWrapper&lt;Employer> {
  
     // Wraps the current row in a ResultSet into an Employer
     public Employer wrap(ResultSet r) {  
       return(new Employer(r.getString(1),r.getDouble(2)); 
     }
     
  }
  
  Database d=new OracleDatabase("scott","tiger");
  for(Employer e : d.query("SELECT * FROM employers WHERE salary>1000",
                           new EmployerConstructor())) {
     System.out.println(e);
  }
  </PRE>
 */ 
public class ResultIterator<T> implements Iterator<T>, Iterable<T>, Closeable{  
      /** Wraps the current row in a ResultSet into a T*/
      public static interface ResultWrapper<T> {
        /** Wraps the current row in a ResultSet into a T*/
        public T wrap(ResultSet r) throws Exception ;
      }
      /** Three-valued boolean variable of true, false and null (uncertain) */
      protected Boolean hasNext=null;
      /** Holds the resultSet*/
      protected ResultSet resultSet;
      /** Holds the constructor to be used for each row */
      protected ResultWrapper<T> constructor;
      /** Creates a FactList from a ResultSet*/
      public ResultIterator(ResultSet s, ResultWrapper<T> cons) {
        resultSet=s;
        constructor=cons;
      }      
      /** Empty constructor for subclasses */
      protected ResultIterator() {        
      }
      /** Returns this */
      public Iterator<T> iterator() {
        return(this);
      }
      public boolean hasNext() {        
        // If we're uncertain, ask the resultSet to switch to the next
        if(hasNext==null) {
          try{
            // Use the Java 5.0 auto-boxing here to convert boolean to Boolean
            hasNext=resultSet.next();
            if(!hasNext) close();
          } catch(SQLException e) {
            throw new RuntimeException(e);
          }
        }  
        // Here, hasNext has a value. Either the one from before
        // or the one from resultSet.next()
        return(hasNext);
      }

      @SuppressWarnings("unchecked")
      public T next() {
        if(!hasNext()) throw new NoSuchElementException();
        try {
          // Tell hasNext() that we're uncertain about future elements
          hasNext=null;
          return(constructor.wrap(resultSet));
        } catch(Exception e) {
          throw new RuntimeException(e);
        }
      }

      /** Closes the resultset and the underlying statement*/
      public void close() {
        Database.close(resultSet);
      }

      /** Closes the resultset */
      public void finalize() {
        close();
      }

      /** Unsupported*/
      public void remove() {
        throw new UnsupportedOperationException();
      }
     
      /** ResultWrapper for a single String column */
      public static final ResultWrapper<String> StringWrapper=new ResultWrapper<String>() {
        public String wrap(ResultSet r) throws SQLException {
          return(r.getString(1));
        }
      };

      /** ResultWrapper for String columns */
      public static final ResultWrapper<String[]> StringsWrapper=new ResultWrapper<String[]>() {
        public String[] wrap(ResultSet r) throws SQLException {          
          String[] result=new String[r.getMetaData().getColumnCount()];
          for(int i=0;i<result.length;i++) result[i]=r.getString(i+1);
          return(result);
        }
      };
      
      /** ResultWrapper for a single Long column */
      public static final ResultWrapper<Long> LongWrapper=new ResultWrapper<Long>() {
        public Long wrap(ResultSet r) throws SQLException {
          return(r.getLong(1));
        }
      };

      /** ResultWrapper for a single Double column */
      public static final ResultWrapper<Double> DoubleWrapper=new ResultWrapper<Double>() {
        public Double wrap(ResultSet r) throws SQLException {
          return(r.getDouble(1));
        }
      };
      
      /** ResultWrapper for a single Integer column */
      public static final ResultWrapper<Integer> IntegerWrapper=new ResultWrapper<Integer>() {
        public Integer wrap(ResultSet r) throws SQLException {
          return(r.getInt(1));
        }
      };
            
      /** Returns an arraylist of an iterator (killing the iterator)*/
      public static<T> List<T> asList(Iterator<T> i) {
        ArrayList<T> l=new ArrayList<T>();
        while(i.hasNext()) l.add(i.next());
        return(l);
      }

      /** Returns an arraylist of this iterator (killing this iterator)*/
      public List<T> asList() {
        return(asList(this));
      }
      
}
