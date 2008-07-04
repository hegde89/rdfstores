package yago.javatools;
import java.util.*;
import java.io.*;
import java.net.URL;
import java.nio.*;

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
  
The HTML-Reader reads characters from a HTML-file.<BR>
Example:
   <PRE>
         HTMLReader r=new HTMLReader(new File("index.html"));
         int c;
         while((c=r.read())!=-1) {
           if(c==-2) System.out.print(" TAG:",r.getTag());
           else System.out.print(c);
         }
         
         -->
             This is the HTML-file, with resolved ampersand sequences
             and with -2 returned for tags.
   </PRE>
<P />   
If the file is UTF8-encoded, consider wrapping an UTF8Reader:
   <PRE>
     HTMLReader r=new HTMLReader(new UTF8Reader(new File("index.html")));
   </PRE>  
 */
public class HTMLReader extends Reader {
  
  /** Holds the actual reader */
  protected Reader in;
  
  /** number of chars for announce (or -1) */
  protected long announceChars=-1;
  
  /** Constructs a HTMLReader from a Reader */
  public HTMLReader(Reader s) {
    in=s;
  }

  /** Constructs a HTMLReader for an URL 
   * @throws IOException */
  public HTMLReader(URL url) throws IOException {
    this(new InputStreamReader(url.openStream()));
  }
  
  /** Constructs a HTMLReader from a File */
  public HTMLReader(File f) throws FileNotFoundException {
    this(new BufferedReader(new FileReader(f)));
  }

  /** Constructs a HTMLReader from a File with a progress bar*/
  public HTMLReader(File f, String message) throws FileNotFoundException {
    this(f);
    announceChars=0;
    Announce.progressStart(message, f.length());
  }  
  
  /** Reads a sequence of characters
   * up to the blank following the nth char, ignores tags */
  public String readTextLine(int n) throws IOException {
    StringBuilder l=new StringBuilder();
    int c;
    while((c=read())!=-1 && !(c==' ' && l.length()>=n)) {
      if(c!=-2) l.append((char)c);
    }
    if(c==-1 && l.length()==0) return(null);
    return(l.toString());
  }

  /** Holds the content of the last tag (decoded) if skipTags==true*/
  protected String tagContent=null;
  
  /** Returns the content of the last tag (decoded) if skipTags==true*/
  public String getTagContent() {
    return(tagContent);
  }
  
  /** Holds the last tag (uppercased) */
  protected String tag=null;
  
  /** Returns the last tag (uppercased) if skipTags==true */
  public String getTag() {
    return(tag);
  }   
  
  /** Reads a character, returns -2 for tags*/
  @Override
  public int read() throws IOException {
    int c=in.read();
    if(announceChars!=-1) Announce.progressAt(announceChars++);    
    switch(c) {
      case -1: return(-1);
      case '<': {        
        StringBuilder t=new StringBuilder();
        while((c=in.read())=='/' || c=='!' || Character.isLetterOrDigit(c)) {
          t.append((char)c);       
        }
        if(announceChars!=-1) Announce.progressAt(announceChars+=t.length());
        if(t.length()>0 && t.charAt(t.length()-1)=='/') t.setLength(t.length()-1);
        tag=t.toString().toUpperCase();
        t.setLength(0);
        while(c!=-1 && c!='>') t.append((char)(c=in.read()));
        if(announceChars!=-1) Announce.progressAt(announceChars+=t.length());
        if(t.length()>0) t.setLength(t.length()-1);
        tagContent=Char.decodePercentage(Char.decodeAmpersand(t.toString()));        
        if(tag.equals("SCRIPT")) {
          scrollToTag("/SCRIPT");
          return(read());
        }
        if(tag.equals("STYLE")) {
          scrollToTag("/STYLE");
          return(read());
        }
        return(-2);
      }  
      case '&': {  
        String a="&";        
        while(!Character.isWhitespace((char)(c=in.read())) && c!=';' && c!=-1) a+=(char)c;
        a+=';';
        c=Char.eatAmpersand(a,Char.eatLength);
        if(announceChars!=-1) Announce.progressAt(announceChars+=a.length());
        return(c);
      }
      default: return(c);
    }
  }

  /** Seeks the next tag of name <I>t</I> and returns all text
   * to the terminating tag /<I>t</I>. Nesting is not supported.
   * Returns null if <I>t</I> was not found. */
  public String readTaggedText(String t) throws IOException {
    if(!scrollToTag(t)) return(null);
    t='/'+t.toUpperCase();
    StringBuilder result=new StringBuilder();
    while(true) {
      int c=read();
      if(c==-1) break;
      if(c==-2) {
        if(t.equals(getTag())) break;
        else continue;
      }
      result.append((char)c);
    }
    return(result.toString());
  }

  /** Seeks a specific string and scrolls to it, returns TRUE if found 
   * @throws IOException */
  public boolean scrollTo(String s) throws IOException {
    return(FileLines.find(in,s)!=-1);
  }
  
  /** Seeks a specific tag and scrolls to it, returns TRUE if found 
   * @throws IOException */
  public boolean scrollToTag(String s) throws IOException {   
    s=s.toUpperCase();
    while(true) {
      int c=read();
      if(c==-1) return(false);      
      if(c==-2 && s.equals(getTag())) return(true);      
    }  
  }

  @Override
  public void close() throws IOException {
    in.close();
    if(announceChars!=-1) Announce.progressDone();
    announceChars=-1;
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    int c;
    int numRead=0;
    while(numRead<len) {
      c=read();
      if(c==-1) {
        if(numRead>0) return(numRead);
        else return(-1);
      }
      if(c==-2) continue;
      cbuf[off++]=(char)c;
      numRead++;
    }
    return numRead;
  }

  /** Test routine */
  public static void main(String[] argv) throws Exception {
    D.p("Enter the name of an HTML-file and hit ENTER");
    HTMLReader r=new HTMLReader(new File(D.r()));
    int c;
    while((c=r.read())!=-1) {
      if(c==-2) System.out.print(" TAG:"+r.getTag());
      else System.out.print((char)c);
    }
    r.close();
  }
}
