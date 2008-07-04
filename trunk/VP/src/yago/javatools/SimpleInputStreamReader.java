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
  
A SimpleInputStreamReader reads the bytes from an InputStream and passes them
on as characters -- regardless of the encoding.
<BR>
Example:
<PRE>
    // It does not work like this
    Reader r=new InputStreamReader(new ByteArrayInputStream(new byte[]{(byte)144}));
    System.out.println(r.read());
    r.close();
     -----> 65533
     
    // But it does like this
    r=new SimpleInputStreamReader(new ByteArrayInputStream(new byte[]{(byte)144}));
    System.out.println(r.read());
    r.close();
     -----> 144
     
</PRE>
*/

public class SimpleInputStreamReader extends Reader {
  public InputStream in;
  
  public SimpleInputStreamReader(InputStream i) {
    in=i;
  }

  public SimpleInputStreamReader(File f) throws FileNotFoundException {
    this(new FileInputStream(f));
  }

  public SimpleInputStreamReader(String f) throws FileNotFoundException {
    this(new File(f));
  }
  
  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    byte[] bbuf=new byte[len];
    int result=in.read(bbuf, 0, len);
    for(int i=0;i<result;i++) cbuf[off+i]=(char)bbuf[i];
    return result;
  }

  public int read() throws IOException {
    return(in.read());
  }
  
  /**  */
  public static void main(String[] args) throws Exception {
    // It does not work like this
    Reader r=new InputStreamReader(new ByteArrayInputStream(new byte[]{(byte)144}));
    System.out.println(r.read());
    r.close();
    
    // But it does like this
    r=new SimpleInputStreamReader(new ByteArrayInputStream(new byte[]{(byte)144}));
    System.out.println(r.read());
    r.close();
  }

}
