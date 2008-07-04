package yago.javatools;
import java.io.*;
import java.net.URL;
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
  
  This class can read characters from a file that is UTF8 encoded.<BR>
  Example:
  <PRE>
     Reader f=new UTF8Reader(new File("blah.blb"));
     int c;
     while((c=f.read())!=-1) System.out.print(Char.normalize(c));
     f.close();
  </PRE>
*/
public class UTF8Reader extends Reader {

  /** Holds the input Stream */
  protected InputStream in;
  
  /** number of chars for announce */
  protected long numBytesRead=0;
  
  /** tells whether we want a progress bar*/
  protected boolean progressBar=false;
  
  /** Constructs a UTF8Reader from a Reader */
  public UTF8Reader(InputStream s) {
    in=s;
  }

  /** Constructs a UTF8Reader for an URL 
   * @throws IOException */
  public UTF8Reader(URL url) throws IOException {
    this(url.openStream());
  }
  
  /** Constructs a UTF8Reader from a File */
  public UTF8Reader(File f) throws FileNotFoundException {
    this(new FileInputStream(f));
  }

  /** Constructs a UTF8Reader from a File, makes a nice progress bar */
  public UTF8Reader(File f, String message) throws FileNotFoundException {
    this(new FileInputStream(f));
    progressBar=true;
    Announce.progressStart(message, f.length());
  }

  /** Constructs a UTF8Reader from a File */
  public UTF8Reader(String f) throws FileNotFoundException {
    this(new File(f));
  }

  /** Constructs a UTF8Reader from a File, makes a nice progress bar */
  public UTF8Reader(String f, String message) throws FileNotFoundException {    
    this(new File(f), message);
  }

  @Override
  public void close() throws IOException {
    if(in==null) return;
    in.close();
    in=null;
    if(progressBar) Announce.progressDone();
    progressBar=false;     
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    if(in==null) return(-1);
    int c;
    int numRead=0;
    while(numRead<len) {
      c=read();
      if(c==-1) {
        close();
        if(numRead>0) return(numRead);
        else return(-1);
      }      
      cbuf[off++]=(char)c;
      numRead++;
    }
    return numRead;  
  }

  @Override
  public int read() throws IOException {
    if(in==null) return(-1);
    int c=in.read(); 
    if(c==-1) {
      close();      
      return(-1);
    }
    int len=Char.Utf8Length((char)c);
    numBytesRead+=len;
    if(progressBar) Announce.progressAt(numBytesRead);    
    switch(len) {
      case 2: return(Char.eatUtf8(((char)c)+""+((char)in.read()),Char.eatLength));
      case 3: return(Char.eatUtf8(((char)c)+""+((char)in.read())+((char)in.read()),Char.eatLength));
      case 4: return(Char.eatUtf8(((char)c)+""+((char)in.read())+((char)in.read())+((char)in.read()),Char.eatLength));
      default: return(c);
    }    
  }
  
  /** Returns the number of bytes read from the underlying stream*/ 
  public long numBytesRead() {
    return(numBytesRead);
  }
  
  /** Test method
   * @throws IOException   */
  public static void main(String[] args) throws IOException {
    Reader f=new UTF8Reader(new File("blah.blb"));
    int c;
    while((c=f.read())!=-1) System.out.print(Char.normalize(c));
    f.close();
  }

}
