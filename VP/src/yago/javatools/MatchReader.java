package yago.javatools;
import java.io.*;
import java.util.*;
import java.util.regex.*;

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
  
The MatchReader reads MatchResults from a file.
It closes the Reader automatically if hasNext()==false.
The MatchReader buffers always 1000 characters of the file, i.e.
it can work with large files without caching them in total.
<BR>
Example:
<PRE>
  MatchReader matchReader=new MatchReader("some Filename", "some Pattern");
  for(MatchResult matchResult : matchReader) {
    System.out.println(matchResult.group(1));
  }
</PRE>
The file is automatically closed after the last match has been read. If you do
not read all matches, close the iterator manually by the method close().<P>

A single quote (') preceded by a backslash will not match a quote in the pattern.
*/

public class MatchReader extends PeekIterator<MatchResult> implements Closeable {
  /** Number of newly read chars */
  public final int BUFSIZE=1000;
  /** Maximal length of a String matching a pattern */
  public final int MAXPATTERNLENGTH=200;
  /** Holds the Reader */
  protected Reader in;
  /** Holds the current matcher */
  protected Matcher matcher;
  /** Holds the current buffer */
  protected StringBuilder buffer=new StringBuilder(BUFSIZE+MAXPATTERNLENGTH);
  /** Holds the pattern to be found */
  protected Pattern pattern;
  /** Char counter for Announce.progress */
  protected long chars=-1;
  /** Points to the index of the last match*/
  protected int lastMatchEnd=0;
  /** Holds the string that quotes are replaced by*/
  public static final String QUOTE="FabianSuchanek";
  
  /** Constructs a MatchReader from a Reader and a Pattern */
  public MatchReader(Reader i, Pattern p) {
    in=i;
    pattern=p;
    next();
  }
  
  /** Constructs a MatchReader from a Reader and a Pattern */
  public MatchReader(Reader i, String p) {
    this(i,Pattern.compile(p));
  }

  /** Constructs a MatchReader that reads from a file, with progress message (main constructor)*/
  public MatchReader(File f, Pattern p, String announceMsg) throws FileNotFoundException {
    pattern=p;
    matcher=p.matcher(buffer);
    if(announceMsg!=null) {
      Announce.progressStart(announceMsg, f.length());
      chars=0;
    }
    in=new BufferedReader(new FileReader(f));
  }

  /** Constructs a MatchReader that reads from a file, with progress message*/
  public MatchReader(String f, Pattern p, String announceMsg) throws FileNotFoundException {
    this(new File(f), p, announceMsg);
  }

  /** Constructs a MatchReader that reads from a file, with progress message*/
  public MatchReader(String f, String p, String announceMsg) throws FileNotFoundException {
    this(new File(f), Pattern.compile(p), announceMsg);
  }

  /** Constructs a MatchReader that reads from a file, with progress message*/
  public MatchReader(File f, String p, String announceMsg) throws FileNotFoundException {
    this(f, Pattern.compile(p), announceMsg);
  }
  
  /** Constructs a MatchReader that reads from a file */
  public MatchReader(File f, String p) throws FileNotFoundException {
    this(f, Pattern.compile(p),null);
  }

  /** Constructs a MatchReader that reads from a file */
  public MatchReader(String f, String p) throws FileNotFoundException {
    this(new File(f), Pattern.compile(p),null);
  }

  /** Constructs a MatchReader that reads from a file */
  public MatchReader(String f, Pattern p) throws FileNotFoundException {
    this(new File(f), p,null);
  }

  /** Constructs a MatchReader that reads from a file */
  public MatchReader(File f, Pattern p) throws FileNotFoundException {
    this(f, p,null);
  }
  
  /** For subclasses*/
  protected MatchReader(){
  }
  
  /** Reads 1 character (simplification for subclass ByteMatchReader) */
  protected int read() throws IOException {
    return(in.read());
  }
  
  /** Returns the next MatchResult */
  public MatchResult internalNext() {
    while(true) {
      // Try whether there is something in the current buffer
      if(matcher.find()) {
        lastMatchEnd=matcher.end();
        return(new MyMatchResult(matcher));
      }
      // Determine the part of the old buffer which we will keep
      if(lastMatchEnd<buffer.length()-MAXPATTERNLENGTH) lastMatchEnd=buffer.length()-MAXPATTERNLENGTH;
      buffer.delete(0,lastMatchEnd);
      // Read a new buffer
      int len;
      for(len=0;len<BUFSIZE;len++) {
        int c;
        try {
          c=read();
        }
        catch(IOException e) {
          return (null);
        }        
        if(c==-1) break;
        if(c=='\'' && buffer.length()>0 && buffer.charAt(buffer.length()-1)=='\\') {
          buffer.setLength(buffer.length()-1);
          buffer.append(QUOTE);
        }
        else buffer.append((char)c);
      }
      if(chars!=-1) Announce.progressAt(chars+=len);
      // Reached the end of file, no data read, close the file
      if(len==0) {
        close();
        if(chars!=-1) Announce.progressDone();
        return(null);
      }
      matcher=pattern.matcher(buffer);
      lastMatchEnd=0;
    }
  }

  /** Closes the reader */
  public void close() {
    try {
      in.close();
    }
    catch(IOException e) {}
  }

  /** Closes the reader */
  public void finalize() {
    close();
  }   

  /** A MatchResult that undoes the quotes */
  public static class MyMatchResult implements MatchResult {
    public MatchResult inner;
    public MyMatchResult(Matcher m) {
      inner=m.toMatchResult();
    }
    public int end() {
      return(inner.end());
    }

    public int end(int group) {
      return inner.end(group);
    }

    public String group() {
      return(group(0));
    }

    public String group(int group) {
      return inner.group(group).replace(QUOTE, "'");
    }

    public int groupCount() {
      return groupCount();
    }

    public int start() {
      return inner.start();
    }

    public int start(int group) {
      return inner.start(group);
    }
    
  }
  /** Test routine */
  public static void main(String[] args) throws Exception {
    for(MatchResult idAndEntity : new MatchReader("c:\\Fabian\\Data\\yago\\search\\pairings.txt",Pattern.compile("(\\d+)\tu:http://[^\\:\n]*/([^/]+)\n"), "Parsing url mappings")) {  
      D.p(idAndEntity.group(1), idAndEntity.group(2))      ;
      D.r();
    }
  }

}
