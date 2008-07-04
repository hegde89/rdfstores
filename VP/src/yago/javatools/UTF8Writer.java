package yago.javatools;
import java.io.*;
import java.util.*;

/** This class is part of the
<A HREF=http://www.mhttp://www.mpi-inf.de/~suchanekpi-inf.mpg.de/~suchanek/downloads/javatools target=_blank>
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

This allows to write characters as UTF8 to a file<BR>
Example:
<PRE>
     Writer w=new UTF8Writer("c:\\blah.blb");
     w.write(Char.decodePercentage("Hall&ouml;chen"));
     w.close();
</PRE>
*/
public class UTF8Writer extends Writer {

  /** The real writer */
  protected OutputStream out;
  
  /** Writes to a file*/
  public UTF8Writer(File f, boolean append) throws IOException{
    this(new FileOutputStream(f,append));
  }
  
  /** Writes to a file*/
  public UTF8Writer(File f) throws IOException{
    this(f,false);
  }

  /** Writes to a file*/
  public UTF8Writer(String f) throws IOException{
    this(new File(f));
  }
  
  /** Writes to a writer*/
  public UTF8Writer(OutputStream f){
    out=f;
  }

  @Override
  public void close() throws IOException {
     out.close();    
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void write(char[] cbuf, int off, int len) throws IOException {
    for(int i=off;i<off+len;i++) write(cbuf[i]);
  }

  @Override
  public void write(int c) throws IOException {
    String s=Char.encodeUTF8(c);
    for(int i=0;i<s.length();i++) out.write((byte)s.charAt(i));
  }
  
  /** Writes a line*/
  public void writeln(String s) throws IOException {
    write(s);
    write("\n");
  }
  
}
