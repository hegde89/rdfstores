package yago.javatools;

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

Does the same as FileLines (see there) but reads bytes (see SimpleInputStreamReader).
*/
public class ByteFileLines extends FileLines {

  /** The stream to read the lines from */
  public InputStream in;
  
  /** Constructs FileLines from a filename */
  public ByteFileLines(String f) throws IOException {
    this(f,null);
  }
  /** Constructs FileLines from a file */
  public ByteFileLines(File f) throws IOException {
    this(f,null);
  }
  /** Constructs FileLines from a filename, shows progress bar */
  public ByteFileLines(String f, String announceMsg) throws IOException {
    this(new File(f),announceMsg);
  }
  /** Constructs FileLines from a file, shows progress bar  (main constructor 1) */
  public ByteFileLines(File f, String announceMsg) throws IOException {
    if(announceMsg!=null) {
      Announce.progressStart(announceMsg, f.length());
      announceChars=0;
    }
    in=new BufferedInputStream(new FileInputStream(f));
  }  
  /** Constructs FileLines from a Reader */
  public ByteFileLines(InputStream i)  {
    this(new BufferedInputStream(i));
  }  
  /** Constructs FileLines from a BufferedReader (main constructor 2) */
  public ByteFileLines(BufferedInputStream i) {
    in=i;
  }
  
  @Override
  public String internalNext() {
    StringBuffer next=new StringBuffer(100);
    try {
      int c;
      do{
        if((c=in.read())==-1) {
          close();
          if(announceChars!=-1) Announce.progressDone();
          return(null);
        }
      } while(c==(char)10 || c==(char)13);
      do{
        next.append((char)c);
        c=in.read();
      } while(c!=(char)10 && c!=(char)13 && c!=-1);      
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
    if(announceChars!=-1) Announce.progressAt(announceChars+=next.length());
    return(next.toString()); 
  }
  
  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException e) {}
  }
  
  public static void main(String[] args) throws Exception {
    for(String l : new ByteFileLines("c:\\fabian\\service\\autoexec.bat")) {
      D.p(l);
    }
  }
}
