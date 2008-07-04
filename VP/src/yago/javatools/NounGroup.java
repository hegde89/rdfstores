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

The class NounGroup splits a noun group (given by a String) into its
modifiers and its head.<BR>
Example:
<PRE>
     System.out.println(new NounGroup("the United States of America").description());
     ->
      NounGroup:
        Original: the_United_States_of_America
        Determiner: the
        Head: State
        Plural: true
        preModifiers: United
        Preposition: of
        postModifier:
          NounGroup:
            Original: America
            Determiner:
            Head: America
            Plural: false
            preModifiers:
            Preposition:
            postModifier:
</PRE>
*/
public class NounGroup {

  /** Defines just one function from a String to a boolean */
  public interface String2Boolean {
    /** Function from a String to a boolean */
    boolean apply(String s);
  }

  /** Tells whether a word is an adjective (currently by a simple heuristics */
  public static String2Boolean isAdjective=new String2Boolean() {
     public boolean apply(String s) {
       return(s.length()>0 && Character.isLowerCase(s.charAt(0)) &&
              (s.endsWith("al") || s.endsWith("ed") || s.endsWith("ing")));
     }
  };

  /** Holds an empty String */
  public static final String NONE="";

  /** Holds prepositions (like "of" etc.) */
  public static final FinalSet<String> prepositions=new FinalSet<String>(
        ",",
        "at",
        "about",
        "and",
        "by",
        "for",
        "from",
        "in",
        "of",
        "on",
        "to",
        "with",
        "who",
        "-",
        "\u2248"
  );

  /** Holds the original noun group */
  public String original=NONE;

  /** Holds the adjective */
  public String adjective=NONE;

  /** Holds the preposition */
  public String preposition=NONE;

  /** Holds the noun group after the preposition */
  public NounGroup postModifier=EMPTY;

  /** Holds the stemmed head of the noun group */
  public String head=NONE;

  /** True if head was upcased */
  public boolean isName=false;

  /** True if head was completely upcased */
  public boolean isAbbreviation=false;

  /** True if the head was plural */
  public boolean isPlural=false;

  /** True if the head was singular (a word form can be both plural and singular)*/
  public boolean isSingular=true;
  
  /** Holds the modifiers before the head  */
  public String preModifier=NONE;

  /** Holds the determiner (if any) */
  public String determiner=NONE;

  /** Holds the original head */
  public String originalHead;
  
  /** Returns the original head. */
  public String getOriginalHead() {
    return originalHead;
  }
  
  /** Returns the adjective. */
  public String getAdjective() {
    return adjective;
  }

  /**Returns the determiner. */
  public String getDeterminer() {
    return determiner;
  }

  /** Returns the head (lowercased singular). */
  public String getHead() {
    return head;
  }

  /** TRUE if the head was an abbreviation */
  public boolean isAbbreviation() {
    return isAbbreviation;
  }

  /** TRUE if the head was a name*/
  public boolean isName() {
    return isName;
  }

  /** TRUE if the head was a plural*/
  public boolean isPlural() {
    return isPlural;
  }

  /** TRUE if the head was singular (a word can be both plural and singular)*/
  public boolean isSingular() {
    return isSingular;
  }

  /**Returns the original. */
  public String getOriginal() {
    return original;
  }

  /** Returns the postModifier. */
  public NounGroup getPostModifier() {
    return postModifier;
  }

  /** Returns the preModifier.  */
  public String getPreModifier() {
    return preModifier;
  }

  /** Returns the preposition.*/
  public String getPreposition() {
    return preposition;
  }

  /** Contains determiners*/
  public static final Set<String> determiners=new FinalSet<String>(
        "the",
        "a",
        "an",
        "this",
        "these",
        "those"
        );
  /** Constructs a noun group from a String */
  public NounGroup(String s) {
    this(Arrays.asList(s.split("[\\s_]+")));
  }  

  /** Constructs a noun group from a list of words */
  public NounGroup(List<String> words) { 
    // Assemble the original
    original=words.toString().replace(", ", "_");
    original=original.substring(1,original.length()-1);
    
    // Cut away preceding determiners
    if(words.size()>0 && determiners.contains(words.get(0).toLowerCase())) {
      determiner=words.get(0).toLowerCase();
      words=words.subList(1, words.size());
    }
    
    // Locate prepositions (but not in first or last position)
    int prepPos;
    for(prepPos=1;prepPos<words.size()-1;prepPos++) {
      if(prepositions.contains(words.get(prepPos))) {
        preposition=words.get(prepPos);
        break;
      }
    }
    
    // Locate "-ing"-adjectives before prepositions (but not at pos 0)
    int ingPos;
    for(ingPos=1;ingPos<prepPos;ingPos++) {
      if(words.get(ingPos).endsWith("ing")) {
        adjective=words.get(ingPos);
        break;
      }
    }

    // Cut off postmodifier in "Blubs blubbing in blah"    
    if(preposition!=NONE && adjective!=NONE && ingPos==prepPos-1) {
      postModifier=new NounGroup(words.subList(prepPos+1, words.size()));
      words=words.subList(0, ingPos);
    }
    // Cut off postmodifier in "Blubs blubbing blah"
    else if(adjective!=NONE) {
      postModifier=new NounGroup(words.subList(ingPos+1, words.size()));
      words=words.subList(0, ingPos);      
    }
    // Cut off postmodifier in "Blubs in blah"
    else if(preposition!=NONE) {
      postModifier=new NounGroup(words.subList(prepPos+1, words.size()));
      if(prepPos>1 && isAdjective.apply(words.get(prepPos-1))) {
        adjective=words.get(prepPos-1);        
        words=words.subList(0, prepPos-1);      
      } else {
        words=words.subList(0, prepPos);      
      }  
    }

    if(words.size()==0) return;

    originalHead=words.get(words.size()-1);
    preModifier=words.subList(0, words.size()-1).toString().replace(", ", "_");
    preModifier=preModifier.substring(1, preModifier.length()-1);
    isName=Name.isName(originalHead);
    isAbbreviation=Name.isAbbreviation(originalHead);
    head=originalHead.toLowerCase();
    isSingular=PlingStemmer.isSingular(head);
    isPlural=PlingStemmer.isPlural(head);
    head=PlingStemmer.stem(head);
  }
  

  /** Checks if the originals match */
  public boolean equals(Object o) {
    return(o instanceof NounGroup && ((NounGroup)o).original.equals(original));
  }

  /** Returns the original */
  public String toString() {
    return(original);
  }

  /** Returns all fields in a String */
  public String description() {
    return("NounGroup:\n"+
           "  Original: "+original+"\n"+
           "  Determiner: "+determiner+"\n"+
           "  preModifiers: "+preModifier+"\n"+
           "  Head: "+head+"\n"+
           "  Singular: "+isSingular+"\n"+                      
           "  Plural: "+isPlural+"\n"+
           "  Abbreviation: "+isAbbreviation+"\n"+
           "  Adjective: "+adjective+"\n"+
           "  Preposition: "+preposition+"\n"+
           "  postModifier: \n"+(postModifier==EMPTY?"":postModifier.description()));
  }

  /** Holds an empty noun group */
  public static final NounGroup EMPTY=new NounGroup("");

  /** Test method   */
  public static void main(String[] args) throws Exception {
    for(String n : new FileLines("c:\\fabian\\eve\\src\\javatools\\NounGroup.txt")) {
      D.p(n);
      D.p(new NounGroup(n).description());
    }
    /*D.p("Enter a noun group and press ENTER. Press CTRL+C to abort");
    while(true) {
      D.p(new NounGroup(D.r()).description());
    }*/
  }

}
