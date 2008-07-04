package yago.javatools;
import java.io.*;
import java.text.*;
import java.util.*;
import java.util.regex.*;
/**This class is part of the
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

The class Name represents a name. There are three sub-types (subclasses) of names:
Abbreviations, person names and company names. These subclasses provide methods to
access the components of the name (like the family name). Use the factory method Name.of to
create a Name-object of the appropriate subclass.<BR>
Example:
<PRE>
  Name.isName("Mouse");
  --> true
  Name.isAbbreviation("PMM");
  --> true  
  Name.isPerson("Mickey Mouse");
  --> false
  Name.couldBePerson("Mickey Mouse");
  --> true
  Name.isPerson("Prof. Mickey Mouse");
  --> true
  Name.of("Prof. Dr. Fabian the Great III of Saarbruecken").describe()
  // equivalent to new PersonName(...) in this case
  -->
  PersonName
    Original: Prof. Dr. Fabian the Great III of Saarbruecken
    Titles: Prof. Dr.
    Given Name: Fabian
    Given Names: Fabian
    Family Name Prefix: null
    Attribute Prefix: the
    Family Name: null
    Attribute: Great
    Family Name Suffix: null
    Roman: III
    City: Saarbruecken
    Normalized: Fabian_Great
</PRE>
*/

public class Name {
  /** Holds the general default name */
  public static final String ANYNAME="NAME";

  /** Contains common family name prefixes (like "von") */
  public static String familyNamePrefix="(?:"+
      "[aA]l|[dD][ea]|[dD]el|[dD]e la|[bB]in|[dD]e las|[dD]e los|[vV][oa][nm]|[dD]i|[zZ]u[mr]|[aA]m|[vV][oa]n de[rnm]|[dD]o|[dD]')";

  /** Says whether this String is a family name prefix */
  public static boolean isFamilyNamePrefix(String s) {
    return(s.matches(familyNamePrefix));
  }
  
  /** Contains attribute Prefixes (like "the" in "Alexander the Great") */
  public static String attributePrefix="(?:"+
      "the|der|die|il|la|le)";

  /** Says whether this String is an attribute Prefix (like "the" in "Alexander the Great") */
  public static boolean isAttributePrefix(String s) {
    return(s.matches(attributePrefix));
  }
 
  /** Contains common name suffixes (like "Junior") */
  public static String familyNameSuffix="(?:"+
      "CBE|"+ // Commander
      "DBE|"+ // Knight or Dame Commander
      "GBE|"+ // Knight or Dame Grand Cross
      "[jJ]r\\.?|"+
      "[jJ]unior|"+
      "hijo|"+
      "hija|"+   
      "P[hH]\\.?[dD]\\.?|"+
      "KBE|"+ // Knight or Dame Commander
      "MBE|"+  // Member
      "M\\.?D\\.|"+
      "OBE|"+ // Officer
      "[sS]enior|"+
      "[sS]r\\.?)";

  /** Says whether this String is a person name suffix */
  public static boolean isPersonNameSuffix(String s) {
    return(s.matches(familyNameSuffix));
  }

  /** Contains common titles (like "Mr.") */
  public static String title="(?:"+
        "[aA]dmiral|"+
        "[aA]mbassador|"+
        "[bB]ishop|"+
        "[bB]brother|"+
        "[cC]aptain|"+
        //"cardinal|"+ // Problems with "Cardinal Health"
        "[cC]hancellor|"+
        "[cC]ol\\.|"+        
        "[cC]olonel|"+
        "[cC]ommander|"+
        "[cC]ongressman|"+
        "[cC]ongresswoman|"+
        "[dD]rs?\\.?|"+
        "[fF]ather|"+
        //"general|"+  // too many problems with this one (cf. "general motors")
        "[gG]gouverneur|"+
        "[gG]ov\\.|"+
        "[gG]overnor|"+
        "[hH]onorable|"+
        "[hH]onourable|"+
        "[jJ]udge|"+
        "[kK]ing|"+
        "[lL]ady|"+
        "[lL]ieutenant|"+
        "[lL]ord|"+
        "[mM]aj\\.|"+
        "[mM]ajor|"+        
        "[mM]aster|"+
        "[mM]essrs\\.?|"+
        "[mM]iss|"+
        "[mM]rs?\\.?|"+
        "[mM]s\\.?|"+
        "[pP]ope|"+
        "[pP][hH]\\.?[dD]|"+
        "[pP]resident|"+
        "[pP]rof\\.?|"+
        "[pP]rofessor|"+
        "[pP]rince|"+        
        "[pP]rincess|"+                
        "[rR]abbi|"+
        "[rR]ev\\.|"+
        "[rR]everend|"+
        "[qQ]ueen|"+
        "[sS]aint|"+
        "[sS]t\\.?|"+
        "[sS]ecretary|"+
        "[sS]enator|"+
        "[sS]ergeant|"+
        "[sS]ir|"+
        "[sS]ister|"+
        "[sS]ultan|" +
        "[eE]mperor|"+
        "[eE]mpress)";
  /** Contains titles that go with the given name (e.g. "Queen" in "Queen Elisabeth"), lowercase*/
  public static Set<String> titlesForGivenNames=new FinalSet<String>(
        "brother","father","king","lady","pope","prince","princess","queen","sister","sultan",
        "emperor","empress");
  /** Says whether this String is a title */
  public static boolean isTitle(String s) {
    return(s.matches(title));
  }

  /** Contains romam digits */
  public static String roman="(?:[XIV]+)";
  
  /** Contains "of" */
  public static String of="of";
  
  /** Contains upper case Characters*/
  public static final String U="\\p{Lu}";

  /** Contains lower case Characters*/
  public static final String L="\\p{Ll}";
  
  /** Contains A characters*/
  public static final String A="\\p{L}";
  
  /** Contains blank */
  public static final String B="[\\s_]+";
  
  /** Contains digits */
  public static final String DG="\\d";

  /** Contains hypens and underscores*/
  public static final String H="[-_]";
  
  /** Contains "|"*/
  public static final String or="|";
  
  /** Contains the pattern for names*/
  public static final Pattern namePattern=Pattern.compile(U+".*"+ or + DG+".*"+A);

  /** Contains the pattern for abbreviations */
  public static final Pattern abbreviationPattern=Pattern.compile("[^"+L+"]+");

  /** Tells whether a String is a name (starts with an upper case char or is a char/digit sequence)*/
  public static boolean isName(String s) {
    return(namePattern.matcher(s).matches());
  }

  /** Tells whether a string is an abbreviation.
    * Every abbreviation is a name.  */
  public static boolean isAbbreviation(String word) {
     return(isName(word) && abbreviationPattern.matcher(word).matches());
  }

  /** The pattern "Name[-Name]" */
  public static final String personNameComponent=
    "(?:al-|Mc|Di|De)?"+U+"(?:"+L+"|['`-]["+L+U+"]|\\.-(?:Mc)?"+U+")*";
    //"(?:al-)?"+U+"(?:['`"+L+"]+|"+L+"?\\.?)(?:-["+U+L+"](?:['`"+L+"]+|"+L+"?\\.?))*";

  /** A Name separator (.-'` )*/
  public static final String personNameSeparator="\\.?"+B;
  
  /** The pattern for person names */
  public static final Pattern personNamePattern=Pattern.compile(
        "((?:"+title+B+")*)"+
        "((?:"+personNameComponent+personNameSeparator+")*?)"+
        "(?:'(.*)'"+B+")?"+
        "("+familyNamePrefix+B+"|"+attributePrefix+B+")?"+ 
        "((?:%(?:(?:"+familyNamePrefix+"|"+personNameComponent+")"+personNameSeparator+")*)?"+personNameComponent+")"+
        "("+B+familyNameSuffix+")?"+ 
        "("+B+roman+")?"+ 
        "(?:"+B+of+B+"("+personNameComponent+"))?"+
        "(?:"+B+"'(.*)')?");

  /** Contains common company name suffixes (like "Inc") */
  public static String companyNameSuffix="(?:"+
        B+"?&_? ?[cC][oO]\\.?|"+
        B+"[cC]orp\\.?|"+
        B+"[cC]orporation|"+
        B+"[iI][nN][cC]\\.?|"+
        B+"[iI]ncorp\\.?|"+
        B+"[iI]ncorporation|"+
        B+"[lL][tT][dD]\\.?|"+
        B+"[lL]imited)";

  /** Contains the pattern for companies*/
  public static final Pattern companyPattern=Pattern.compile("(.+)("+companyNameSuffix+")");
  
  /** Says whether this String is a company name suffix */
  public static boolean isCompanyNameSuffix(String s) {
    return(s.matches(companyNameSuffix));
  }
  
  public static class PersonName extends Name {
    public String titles;
    public String givenNames;
    public String myFamilyNamePrefix;
    public String myAttributePrefix;
    public String familyName;
    public String attribute;
    public String myFamilyNameSuffix;
    public String myRoman;
    public String city;
    public String nickname;
    /** Returns the first given name or null*/
    public String getGivenName() {
      if(givenNames==null) return(null);
      if(givenNames.indexOf('_')==-1) return(givenNames);
      return(givenNames.substring(0, givenNames.indexOf('_')));
    }      
    /** Returns the n-th group or null */
    protected static String getComponent(Matcher m, int n) {
      if(!m.matches() || m.group(n)==null || m.group(n).length()==0) return(null);
      String result=m.group(n);
      if(result.startsWith("%")) result=result.substring(1);
      if(result.matches(".+"+B)) return(result.substring(0,result.length()-1));
      if(result.matches(B+".+")) return(result.substring(1));    
      return(result);        
    }    
    /** Constructs a person name from a String */
    public PersonName(String s) {
      super(s);
      // Check "FamilyName, GivenName"
      if(s.indexOf(',')!=-1) s=(s.substring(s.indexOf(',')+1)+" %"+s.substring(0,s.indexOf(','))).trim();
      s=s.replace(' ', '_');
      Matcher m=personNamePattern.matcher(s);
      if(!m.matches()) return;
      titles=getComponent(m, 1);
      givenNames=getComponent(m, 2);
      nickname=getComponent(m,3);
      if(nickname==null) nickname=getComponent(m, 9);
      familyName=getComponent(m, 5);
      String a=getComponent(m, 4);
      if(a!=null) {
        if(a.matches(Name.attributePrefix)) {      
          myAttributePrefix=a;
          attribute=familyName;
          familyName=null;
        } else {
          myFamilyNamePrefix=a;
        }
      }
      myFamilyNameSuffix=getComponent(m,6);
      myRoman=getComponent(m,7);
      if(familyName!=null && familyName.matches(Name.roman)) {
        myRoman=familyName;
        familyName=null;
      }
      city=getComponent(m,8);
      if(givenNames==null && titles!=null && titlesForGivenNames.contains(titles.toLowerCase())) {
        givenNames=familyName;
        familyName=null;
      }
      if(familyName!=null && familyName.startsWith("'") && familyName.endsWith("'")) {
        nickname=familyName.substring(1,familyName.length()-2);
        familyName=null;
      }
    }
    /**Returns the attribute.*/
    public String getAttribute() {
      return attribute;
    }
    /**Returns the attributePrefix.*/
    public String getAttributePrefix() {
      return myAttributePrefix;
    }
    /**Returns the city.*/
    public String getCity() {
      return city;
    }
    /**Returns the nickname.*/
    public String getNickname() {
      return nickname;
    }
    /**Returns the familyName. */
    public String getFamilyName() {
      return familyName;
    }
    /**Returns the familyNamePrefix.*/
    public String getFamilyNamePrefix() {
      return myFamilyNamePrefix;
    }
    /**Returns the familyNameSuffix.*/
    public String getFamilyNameSuffix() {
      return myFamilyNameSuffix;
    }
    /**Returns the givenNames.*/
    public String getGivenNames() {
      return givenNames;
    }
    /**Returns the roman number.*/
    public String getRoman() {
      return myRoman;
    }
    /**Returns the titles.*/
    public String getTitles() {
      return titles;
    }
    /** Normalizes a person name.*/
    public String normalize() {
      String given=getGivenName();
      if(given==null) return(familyName);
      if(given.endsWith(".")) given=Char.cutLast(given);
      if(familyName!=null) return(given+'_'+familyName.replace(' ','_'));
      if(attribute!=null) return(given+'_'+attribute);
      return(given);
    }   
    /** Returns a description */
    public String describe() {
      return("PersonName\n"+
             "  Original: "+original+"\n"+
             "  Titles: "+getTitles()+"\n"+
             "  Given Name: "+getGivenName()+"\n"+
             "  Given Names: "+getGivenNames()+"\n"+
             "  Nickname: "+getNickname()+"\n"+             
             "  Family Name Prefix: "+getFamilyNamePrefix()+"\n"+             
             "  Attribute Prefix: "+getAttributePrefix()+"\n"+                          
             "  Family Name: "+getFamilyName()+"\n"+             
             "  Attribute: "+getAttribute()+"\n"+                          
             "  Family Name Suffix: "+getFamilyNameSuffix()+"\n"+                          
             "  Roman: "+getRoman()+"\n"+                                       
             "  City: "+getCity()+"\n"+                                       
             "  Normalized: "+normalize());
    }    
    
  }
  
  public static class CompanyName extends Name {
    public String name;
    public String suffix;
    public CompanyName(String s) {
      super(s);
      Matcher m=companyPattern.matcher(s);
      if(!m.matches()) return;
      name=m.group(1);
      suffix=m.group(2);
    }
    /**Returns the name.*/
    public String getName() {
      return name;
    }
    /**Returns the suffix.*/
    public String getSuffix() {
      return suffix;
    }
    public String normalize() {
      return(name);
    }
    /** Returns a description */
    public String describe() {
      return("CompanyName\n"+
             "  Original: "+original+"\n"+
             "  Name: "+name+"\n"+             
             "  Suffix: "+suffix+"\n"+             
             "  Normalized: "+normalize());
    }    
  }

  public static class Abbreviation extends Name {
    public Abbreviation(String s) {
      super(s);
      if(!abbreviationPattern.matcher(s).matches()) return;
    }
    public String normalize() {
      return(super.normalize().toUpperCase());
    }
    /** Returns a description */
    public String describe() {
      return("Abbreviation\n"+
             "  Original: "+original+"\n"+
             "  Normalized: "+normalize());
    }
    
  }
  
  /** Returns true if it is highly probable that the string is a person name.
   * Every person name is a name. */
  public static boolean isPersonName(String m) {
    PersonName p=new PersonName(m);
    if(p.getTitles()!=null) return(true);
    if(p.getRoman()!=null) return(true);
    if(p.getFamilyNameSuffix()!=null) return(true);    
    if(p.getGivenNames()!=null && p.getGivenNames().endsWith(".")) return(true);
    return(false);
  }

  /** Returns true if it is possible that the string is a person name */
  public static boolean couldBePersonName(String s) {
    if(isCompanyName(s)) return(false);
    if(s.indexOf(',')!=-1) s=(s.substring(s.indexOf(',')+1)+" %"+s.substring(0,s.indexOf(','))).trim();
    return(personNamePattern.matcher(s).matches());
  }

  /** Tells if the string is a company name */
  public static boolean isCompanyName(String s) {
    return(companyPattern.matcher(s).matches());
  }

  /** Holds the original name */
  public String original;
  
  /** Returns the original name */
  public String toString() {
    return(original);
  }
  
  /** Returns the letters and digits of the original name (eliminates punctuation)*/
  public String normalize() {
    return(original.replaceAll(B,"_").replaceAll("([\\P{L}&&[^\\d]&&[^_]])", ""));
  }
  
  /** Constructor (use Name.of instead!) */
  protected Name(String s) {
    original=s;
  }
    
  /** Returns a description */
  public String describe() {
    return("Name\n"+
           "  Original: "+original+"\n"+
           "  Normalized: "+normalize());
  }
  
  /** Factory pattern */
  public static Name of(String s) {
    if(isCompanyName(s)) return(new CompanyName(s));
    if(couldBePersonName(s)) return(new PersonName(s));
    if(isAbbreviation(s)) return(new Abbreviation(s));    
    return(new Name(s));
  }
  /** Test routine */
  public static void main(String[] argv) throws Exception {
    D.p(Name.of("Empress Johanna").describe());
    /*for(String s :new FileLines("c:\\fabian\\eve\\eve\\src\\javatools\\NameParserTest.txt")) {      
      if(couldBePersonName(s)) {
        D.p(Name.of(s).describe());
      } else {
        D.p("---- "+s);
      }  
    }*/
  }
}
