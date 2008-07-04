package yago.javatools;

import java.io.*;
import java.util.regex.*;

import yago.javatools.MatchReader.MyMatchResult;


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
  
Does the same as the MatchReader (see there), but deals with bytes instead of Chars
(see SimpleInputStreamReader).*/
public class ByteMatchReader extends MatchReader {

  /** Holds the stream to read from*/
  protected InputStream input;
  
  /** Constructs a MatchReader from a Reader and a Pattern */
  public ByteMatchReader(InputStream i, Pattern p) {
    input=i;
    pattern=p;
    next();
  }
  
  /** Constructs a MatchReader from a Reader and a Pattern */
  public ByteMatchReader(InputStream i, String p) {
    this(i,Pattern.compile(p));
  }

  /** Constructs a MatchReader that reads from a file, with progress message (main constructor)*/
  public ByteMatchReader(File f, Pattern p, String announceMsg) throws FileNotFoundException {
    if(announceMsg!=null) {
      Announce.progressStart(announceMsg, f.length());
      chars=0;
    }
    input=new BufferedInputStream(new FileInputStream(f));
    pattern=p;
    matcher=p.matcher(buffer);
  }

  /** Constructs a MatchReader that reads from a file, with progress message*/
  public ByteMatchReader(String f, Pattern p, String announceMsg) throws FileNotFoundException {
    this(new File(f), p, announceMsg);
  }

  /** Constructs a MatchReader that reads from a file, with progress message*/
  public ByteMatchReader(String f, String p, String announceMsg) throws FileNotFoundException {
    this(new File(f), Pattern.compile(p), announceMsg);
  }

  /** Constructs a MatchReader that reads from a file, with progress message*/
  public ByteMatchReader(File f, String p, String announceMsg) throws FileNotFoundException {
    this(f, Pattern.compile(p), announceMsg);
  }
  
  /** Constructs a MatchReader that reads from a file */
  public ByteMatchReader(File f, String p) throws FileNotFoundException {
    this(f, Pattern.compile(p),null);
  }

  /** Constructs a MatchReader that reads from a file */
  public ByteMatchReader(String f, String p) throws FileNotFoundException {
    this(new File(f), Pattern.compile(p),null);
  }

  /** Constructs a MatchReader that reads from a file */
  public ByteMatchReader(String f, Pattern p) throws FileNotFoundException {
    this(new File(f), p,null);
  }

  /** Constructs a MatchReader that reads from a file */
  public ByteMatchReader(File f, Pattern p) throws FileNotFoundException {
    this(f, p,null);
  }
  
  /** Reads 1 character */
  protected int read() throws IOException {
    return(input.read());
  }

  /** Closes the reader */
  public void close() {
    try {
      input.close();
    }
    catch(IOException e) {}
  }

    
  
}
