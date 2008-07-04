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
  
A SimpleOutputStreamWriter writes the characters directly as bytes to an output stream
-- regardless of the encoding. See SimpleInputStreamReader for an explanation.
*/
public class SimpleOutputStreamWriter extends Writer {

  /** Holds the underlying OutputStrema*/
  public OutputStream out;
  
  public SimpleOutputStreamWriter(OutputStream o) {
    out=o;
  }

  public SimpleOutputStreamWriter(File f) throws IOException {
    this(new BufferedOutputStream(new FileOutputStream(f)));
  }

  public SimpleOutputStreamWriter(String s) throws IOException {
    this(new File(s));
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
    for(int pos=off;pos<off+len;pos++) write(cbuf[pos]);
  }

  public void write(int c) throws IOException {
    out.write((byte)(c));
  }
}
