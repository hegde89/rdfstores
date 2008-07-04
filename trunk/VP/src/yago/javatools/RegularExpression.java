package yago.javatools;

import java.util.*;
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
  
   The class represents a regular expression. You can walk through the regular expression 
   by RegExStates. Each RegExState represents one position in the regular expression.
   Each RegExState knows which other RegExStates are valid sucessors. The regular expression
   itself knows which RegExStates are valid exit points. The regular expression can also be 
   inverted.<BR>
   Example:
   <PRE>
         D.p(RegularExpression.compile("a|(b c*)*").describe());
         -->
            1(a) -> []             // From a, we're done
            2(b) -> [3(c)]         // From b, go to c
            3(c) -> [3(c), 2(b)]   // From c, go either to c or to b
            Valid exits: 1(a), 3(c),  
         D.p(RegularExpression.compile("a|(b c*)*").inverse().describe());            
         -->
            4(a) -> []
            6(c) -> [5(b), 6(c)]
            5(b) -> [6(c)]    
            Valid exits: 4(a), 5(b),       
   </PRE>
*/
public class RegularExpression implements Iterable<List<String>> {

  /** RegExStates with which the RegularExpression starts */
  public List<RegExState> entries=new ArrayList<RegExState>();
  /** Valid exit states of this RegularExpression */    
  public List<RegExState> exits=new ArrayList<RegExState>();
  /** Holds the original regex */
  public String original;
  
  /** returns the original */
  public String getOrginal() {
    return(original);
  }
  
  /** Returns a RegularExpression for a string*/
  public static RegularExpression compile(String regex) {
    regex=regex.replaceAll("([^\\| \\(])\\(","$1 (");
    regex=regex.replaceAll("\\)([^\\| \\)\\*])",") $1");      
    regex=regex.replaceAll("\\*([^\\| \\)])","* $1"); 
    if(regex.indexOf('+')!=-1) throw new RuntimeException("'+' is not supported by RegularExpression.java: "+regex);
    RegularExpression result=parseSimple(new StringTokenizer(regex,"|* ()",true));
    result.original=regex;
    return(result);
  }
  
  public String toString() {
    return(original);
  }
  
  /** Internal constructor*/
  protected RegularExpression(String token) {
    RegExState r=new RegExState(token);
    entries.add(r);
    exits.add(r);
  }

  /** Internal constructor*/
  protected RegularExpression() {
  }
  
  /** Tells whether this RegExChunk is a valid exit*/
  public boolean isExit(RegExState e) {
    return(exits.contains(e));
  }
  
  /** Returns the entry states of this RegularExpression */
  public List<RegExState> getEntries() {
    return(entries);
  }
  
  /** Parses a regex from a StringTokenizer*/
  public static RegularExpression parseSimple(StringTokenizer regex) {
    boolean disjunctMode=false;
    RegularExpression result=null;
    while(regex.hasMoreElements()) {
      String next=regex.nextToken();
      RegularExpression nextRegEx;
      if(next.equals("(")) {
        nextRegEx=parseSimple(regex);
      } else {
        if(next.length()==0 || next.length()==1 && "|* ()".indexOf(next.charAt(0))!=-1) {
          throw new RuntimeException("Invalid token in regular expression "+next);
        }
        nextRegEx=new RegularExpression(next);
      }   
      String delim=regex.hasMoreTokens()?regex.nextToken():")";
      if(delim.equals("*")) {
        for(RegExState c : nextRegEx.exits) c.addSuccessors(nextRegEx.entries);
        delim=regex.hasMoreTokens()?regex.nextToken():")";
      }        
      disjunctMode=disjunctMode||delim.equals("|");
      if(result==null) {
        result=nextRegEx;
      } else {  
        if(disjunctMode) {
          result.entries.addAll(nextRegEx.entries);
          result.exits.addAll(nextRegEx.exits);          
        } else {
          for(RegExState c : result.exits) c.addSuccessors(nextRegEx.entries);
          result.exits=nextRegEx.exits;
        }
      }  
      if(delim.equals(")")) {
        break;
      }
    } 
    return(result);
  }
  
  /** returns a nice String description */
  public String describe() {
    StringBuilder b=new StringBuilder();
    for(RegExState c : entries) b.append(c.describe()).append("\n");
    b.append("Valid exits: ");
    for(RegExState c : exits) b.append(c.toString()).append(", ");
    return(b.toString());
  }    

  /** Returns the set of States (expensive)*/
  public List<RegExState> getStates() {
    List<RegExState> result=new ArrayList<RegExState>();
    result.addAll(getEntries());
    for(int i=0;i<result.size();i++) {
      for(RegExState s : result.get(i).getSuccessors()) {
        if(!result.contains(s)) result.add(s);
      }
    }
    return(result);
  }
  
  /** Returns the inverse of this Regular Expression (expensive) */
  public RegularExpression inverse() {
    RegularExpression result=new RegularExpression();
    List<RegExState> resultStates=new ArrayList<RegExState>();
    List<RegExState> states=getStates();
    for(RegExState s : states) resultStates.add(new RegExState(s.getToken()));
    for(int i=0;i<states.size();i++) {
      RegExState resultState=resultStates.get(i);
      RegExState state=states.get(i);
      for(int j=0;j<states.size();j++) {
        if(states.get(j).getSuccessors().contains(state)) resultState.addSuccessor(resultStates.get(j));
      }
      if(entries.contains(state)) result.exits.add(resultState);
      if(exits.contains(state)) result.entries.add(resultState);
    }
    return(result);
  }
  /** Represents one position in a regular expression*/
  public static class RegExState implements Comparable<RegExState> {
    /** Counts all RegExStates to have an id for each (for toString)*/
    public static int idcounter=0;
    /** Id for toString*/
    public int id=++idcounter;
    /** Holds all positions that can follow from here */
    public List<RegExState> successors=new ArrayList<RegExState>();
    /** Holds the token name at the current position*/
    public String token=null;

    /** Changes the token */
    public void setToken(String t) {
      token=t;
    }
    
    /** Adds one successor */
    public void addSuccessor(RegExState s) {
      successors.add(s);
    }

    /** Adds multiple successors */
    public void addSuccessors(Collection<RegExState> s) {
      successors.addAll(s);
    }

    /** Returns the token */
    public String getToken() {
      return token;
    }

    /** Returns the successors */
    public List<RegExState> getSuccessors() {
      return successors;
    }

    /** Constructs a RegExChunk with a token*/
    public RegExState(String token) {
      this.token=token;
    }    

    /** Returns the id and the token*/
    public String toString() {
      return(id+"("+(token==null?"null":token.toString())+")");
    }

    /** Returns a nice description string*/
    public String describe() {
      StringBuilder b=new StringBuilder();
      Collection<RegExState> s=new ArrayList<RegExState>();
      describe(b,s);
      return(b.toString());
    }
    
    /** Helper method for describe()*/
    protected void describe(StringBuilder b, Collection<RegExState> done) {
      if(done.contains(this)) return;
      b.append(this.toString());
      b.append(" -> [");
      if(successors.size()!=0) {
        for(RegExState c : successors) b.append(c.toString()).append(", ");
        b.setLength(b.length()-2);
      }  
      b.append("]\n");      
      done.add(this);
      for(RegExState c : successors) c.describe(b,done);
    }

    public int compareTo(RegExState o) {            
      return(o.id<id?-1:o.id>id?1:0);
    }

    public boolean equals(Object o) {
      return(o!= null & o instanceof RegExState && ((RegExState)o).id==id);
    }

    public int hashCode() {
      return id;
    }
    
  }
  
  /** Returns an iterator over incarnations of the expression */
  public PeekIterator<List<String>> iterator() {
    return new PeekIterator<List<String>>() {
      Queue<List<RegExState>> queue = new ArrayQueue<List<RegExState>>();
      {
        for(RegExState r : getEntries()) {
          queue.add(Arrays.asList(r));
        }
      }
      public List<String> internalNext() {
        List<RegExState> next;
        RegExState end;
        do {
          if (queue.size() == 0) return (null);
          next = queue.poll();
          end= next.get(next.size() - 1);
          for (RegExState followUp : end.getSuccessors()) {
            List<RegExState> nextPlusFollowUp = new ArrayList<RegExState>(next);
            nextPlusFollowUp.add(followUp);
            queue.add(nextPlusFollowUp);
          }
        } while (!exits.contains(end));
        List<String> result = new ArrayList<String>();
        for (RegExState s : next)
          result.add(s.token);
        return result;
      }
    };
  }
  /** Test routine */
  public static void main(String[] args) {    
    while(true) {
      D.p("Enter a regular expression");
      RegularExpression r=RegularExpression.compile(D.r());
      D.p(r.describe());
      D.p(r.getEntries());
      D.p("Inverse:");
      D.p(r.inverse().describe());
      Iterator<List<String>> it=r.iterator();
      for (int i = 0; i < 10 && it.hasNext(); i++) {
        D.p(it.next());
      }
    }
  }
}
