package yago.javatools;

import java.io.*;
import java.text.*;
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
  
   The NumberParser normalizes number expressions in English natural language text.
   It can work with expressions like
   <PRE>
         10 million meters
         7 inches
         2.3 sq ft
         12:30 pm
         100 km
         12 ml
         10 Mb
   </PRE>
   Example:
   <PRE>
         System.out.println(NumberParser.normalize("It was 1.2 inches long"));
         --> "It was 0.030479999999999997#meter long"
         
         System.out.println(toLong("more than ten kB"));
         --> 10000.0
   </PRE>
  */

public class NumberParser {

  /** Creates a normalized number from a number and a type*/
  public static final String newNumber(String n,String type) {
    return(n+'#'+type);
  }

  /** Creates a normalized number without a type.
   * @see newNumber(String n, String type)*/
  public static final String newNumber(String n) {
    return(n);
  }

  /** Creates a normalized number from a double and a type.
   * @see newNumber(String n, String type) */
  public static final String newNumber(double d,String type) {
    return(newNumber(d+"",type));
  }

  /** Maps decimal prefixes (like "giga") to their double */
  public static final Map<String,Double> prefixes=new FinalMap<String,Double>(
    "tera",1000000000000.0,
    "T",1000000000000.0,
    "giga",1000000000.0,
    "G",1000000000.0,
    "M",1000000.0,
    "mega",1000000.0,
    "kilo",1000.0,
    "k",1000.0,
    "deci",0.1,
    "",1.0,
    "d",0.1,
    "centi",0.01,
    "c",0.01,
    "milli",0.001,
    "m",0.001,
    "micro",0.000001,
    "mu",0.000001,
    "nano",0.000000001,
    "n",0.000000001
  );

  /** A dot or comma as a RegEx */
  private static final String DC="[\\.,]";
  /** A dot as a RegEx */
  private static final String DOT="\\.";
  /** A comma as a RegEx */
  //private static final String C=",";
  /** A digit as a capturing RegEx */
  private static final String DIG="(\\d)";
  /** A blank as a RegEx */
  private static final String B="[\\s_]*";
  /** A forced blank as a capturing RegEx */
  private static final String FB="([\\W_])";
  /** A forced blank or "th" as a capturing RegEx */
  private static final String FBTH="(\\W|_|th)";
  /** A hyphen as a RegEx with blanks*/
  //private static final String H=B+"(?:-+|to|until)"+B;
  /** ##th as a capturing RegEx with blank*/
  private static final String NTH="(\\d+)(?:th|rd|nd|st)"+FB;
  /** a/one/<> with a blank as a RegEx */
  private static final String A="(?:a|one|A|One)"+B;
  /** Prefixes as a capturing RegEx */
  private static final String P="(tera|T|giga|G|mega|M|kilo|k|deci|d|centi|c|milli|m|micro|mu|nano|n|)";
  /** A number as a capturing RegEx */
  private static final String FLOAT="(-?\\d+(?:\\.[0-9]+)?(?:[Ee]\\-?[0-9]+)?)";
  /** An integer as a capturing regex*/
  private static final String INT="([0-9]+)";
  /** A short integer as a capturing regex*/
  private static final String SINT="([0-9]{1,3})";
  /** A unit as a captuing regex*/
  private static final String UNIT="([a-zA-Z]*(?:\\^\\d)?)";
  /** The number pattern*/
  public static final Pattern NUMBERPATTERN=Pattern.compile(newNumber(FLOAT+"(?:", UNIT+")?"));

  /** Tells whether this string is a normalized number */
  public static boolean isFloat(String s) {
    return(s.matches(FLOAT));
  }

  /** Tells whether this string is a normalized integer number */
  public static boolean isInt(String s) {
    return(s.matches(INT));
  }

  /** Tells whether this string is a normalized number with unit*/
  public static boolean isNumberAndUnit(String s) {
    return(NUMBERPATTERN.matcher(s).matches());
  }

  /** Just a pair of a Pattern and a replacement string */
  private static class FindReplace {
    public String pattern;
    public String replacement;
    public String toString() {
      return(pattern+"   -->   "+replacement);
    }
    public FindReplace(String f,String r) {
      pattern=f;
      replacement=r;
    }
    public StringBuilder apply(StringBuilder s) {           
      Matcher m=Pattern.compile(pattern).matcher(s);
      if(!m.find()) return(s);      
      StringBuilder result=new StringBuilder(s.length()+10);
      int pos=0;
      do{
        for(int i=pos;i<m.start();i++) result.append(s.charAt(i));
        pos=m.end();
        for(int i=0;i<replacement.length();i++) {
          if(replacement.charAt(i)=='$') {
            String rep=m.group(replacement.charAt(i+1)-'0');
            if(rep!=null) result.append(rep);
            i++;
          } else {
            result.append(replacement.charAt(i));
          }
        }
      }while(m.find());      
      for(int i=pos;i<s.length();i++) result.append(s.charAt(i));
      return(result);
    }
  }

  private static class FindCompute extends FindReplace {
    public double factor;
    public double summand;    
    public FindCompute(String f,String unit,double fac,double sum) {
      super(FLOAT+B+P+f+FB,unit);
      factor=fac;
      summand=sum;
    }
    public FindCompute(String f,String unit) {
      this(f,unit,1,0);
    }
    public StringBuilder apply(StringBuilder s) {           
      Matcher m=Pattern.compile(pattern).matcher(s);
      if(!m.find()) return(s);      
      StringBuilder result=new StringBuilder(s.length()+10);
      int pow=(replacement!=null && Character.isDigit(replacement.charAt(replacement.length()-1)))?
          replacement.charAt(replacement.length()-1)-'0':1;
      int pos=0;
      do{
        for(int i=pos;i<m.start();i++) result.append(s.charAt(i));
        pos=m.end();
        double val=Double.parseDouble(m.group(1));
        if(replacement==null)
          result.append((val+summand)*factor*prefixes.get(m.group(2))+"").append(' ');
        else result.append(newNumber((val+summand)*factor*Math.pow(prefixes.get(m.group(2)),pow),replacement)).append(' ');
      }while(m.find());      
      for(int i=pos;i<s.length();i++) result.append(s.charAt(i));
      return(result);
    }
  }
  private static class FindAdd extends FindCompute {
    public FindAdd(String f, String unit,double sum) {
      super(f,unit,1,sum);
    }
  }
  private static class FindMultiply extends FindCompute {
    public FindMultiply(String f, String unit, double fac) {
      super(f,unit,fac,0);
    }
  }
  /** Holds the number patterns */
  private static final FindReplace[] patterns=new FindReplace[]{
  // The internal order of the patterns is essential!
    new FindCompute("(pounds|pound|lb|lbs)","g",453.59237,0), // TODO
    //  --------- separators ------------
    // c.##
    new FindReplace("(\\W)c\\.? ?(\\d)", "$1 about $2"),
    // #-#
    new FindReplace("(\\d)-(\\d)","$1 - $2"),
    // 1 000
    new FindReplace("(\\d{1,3}) (\\d{3}) ?(\\d{3})? ?(\\d{3})?","$1$2$3$4"),
    // .09
    new FindReplace(" "+DC+INT," 0.$1"),
    // 1,000
    new FindReplace("(\\d+),(\\d{3}),?(\\d{3})?,?(\\d{3})?,?(\\d{3})?","$1$2$3$4$5"),
    // 1,00 -> 1.00
    new FindReplace("(\\d),(\\d)","$1.$2"),
    
    //  --------- 2-12 ------------
    new FindReplace(FB+"two"+FB,"$12$2"),
    new FindReplace(FB+"three"+FB,"$13$2"),
    new FindReplace(FB+"four"+FBTH,"$14$2"),
    new FindReplace(FB+"five"+FB,"$15$2"),
    new FindReplace(FB+"six"+FBTH,"$16$2"),
    new FindReplace(FB+"seven"+FBTH,"$17$2"),
    new FindReplace(FB+"eight"+FB,"$18$2"),
    new FindReplace(FB+"nine"+FBTH,"$19$2"),
    new FindReplace(FB+"ten"+FBTH,"$110$2"),
    new FindReplace(FB+"eleven"+FBTH,"$111$2"),
    new FindReplace(FB+"twelve"+FBTH,"$112$2"),
    new FindReplace(FB+"first"+FB,"$1"+newNumber("1","th")+"$2"),
    new FindReplace(FB+"second"+FB,"$1"+newNumber("2","th")+"$2"),
    new FindReplace(FB+"third"+FB,"$1"+newNumber("3","th")+"$2"),
    new FindReplace(FB+"eighth"+FB,"$1"+newNumber("8","th")+"$2"),

    //  --------- billions and millions ------------
    // Currencies often have just a 'm' to indicate million
    new FindReplace("(?i:US\\$|USD|\\$|\\$US)"+B+FLOAT+B+"[Mm]"+FB,"$1 million dollar"),
    new FindReplace("(?i:euro|eur|euros)"+B+FLOAT+B+"[Mm]"+FB,"$1 million euro"),
    new FindReplace("(?i:US\\$|USD|\\$|\\$US)"+B+FLOAT+B+"[bB]"+FB,"$1 billion dollar"),
    new FindReplace("(?i:euro|eur|euros)"+B+FLOAT+B+"[bB]"+FB,"$1 billion euro"),
    // # trillion
    new FindReplace(SINT+DOT+DIG+B+"[Tt]rillion","$1$200000000000"),
    new FindReplace(SINT+DOT+DIG+DIG+B+"[Tt]rillion","$1$2$30000000000"),
    new FindReplace(SINT+DOT+DIG+DIG+DIG+B+"[Tt]rillion","$1$2$3$4000000000"),
    new FindReplace(SINT+B+"[Tt]rillion","$1000000000000"),
    // # billion
    new FindReplace(SINT+DOT+DIG+B+"[Bb]illion","$1$200000000"),
    new FindReplace(SINT+DOT+DIG+DIG+B+"[Bb]illion","$1$2$30000000"),
    new FindReplace(SINT+DOT+DIG+DIG+DIG+B+"[Bb]illion","$1$2$3$4000000"),
    new FindReplace(SINT+B+"[Bb]illion","$1000000000"),
    new FindReplace(SINT+DOT+DIG+B+"[Bb]n?","$1$200000000"),
    new FindReplace(SINT+DOT+DIG+DIG+B+"[Bb]n?","$1$2$30000000"),
    new FindReplace(SINT+DOT+DIG+DIG+DIG+B+"[Bb]n?","$1$2$3$4000000"),
    new FindReplace(SINT+B+"[Bb]n","$1000000000"),
    // # million
    new FindReplace(SINT+DOT+DIG+B+"[Mm]illion","$1$200000"),
    new FindReplace(SINT+DOT+DIG+DIG+B+"[Mm]illion","$1$2$30000"),
    new FindReplace(SINT+DOT+DIG+DIG+DIG+B+"[Mm]illion","$1$2$3$4000"),
    new FindReplace(SINT+B+"[Mm]illion","$1000000"),
    // # thousand
    new FindReplace(SINT+DOT+DIG+B+"[Tt]housand","$1$200"),
    new FindReplace(SINT+B+"[Tt]housand","$1000"),
    // # hundred
    new FindReplace(SINT+B+"[Hh]undred","$100"),
    // # dozen
    new FindMultiply("[Dd]ozens?",null,12),
    // billions
    // new FindReplace("billions"+OF,"3000000000"),
    // millions
    // new FindReplace("millions"+OF,"3000000"),
    // thousands
    // new FindReplace("thousands"+OF,newNumber("3000",CARDINAL)),
    // hundreds
    // new FindReplace("hundreds"+OF,newNumber("300",CARDINAL)),
    // dozens
    // new FindReplace("dozens"+OF,newNumber("40",CARDINAL)),
    // a billion
    new FindReplace(A+"[Bb]illion","1000000000"),
    // a million
    new FindReplace(A+"[Mm]illion","1000000"),
    // a thousand
    new FindReplace(A+"[Tt]housand","1000"),
    // a hundred
    new FindReplace(A+"[Hh]undred","100"),
    // a dozen
    new FindReplace(A+"[Dd]ozen","12"),

    // --------- Ordinal numbers and super scripts -----------
    // 1st
    new FindReplace(NTH,newNumber("$1","th")+"$2"),
    // ^1 
    new FindReplace("¹",""),
    // ^2
    new FindReplace("²","^2"),
    // ^3
    new FindReplace("³","^3"),
    
    // --------- Times and inches ------------------
    new FindReplace(INT+':'+INT+B+"pm"+FB,null) {      
      public StringBuilder apply(StringBuilder s) {           
        Matcher m=Pattern.compile(pattern).matcher(s);
        if(!m.find()) return(s);      
        StringBuilder result=new StringBuilder(s.length()+10);
        int pos=0;
        do{
          for(int i=pos;i<m.start();i++) result.append(s.charAt(i));
          pos=m.end();
          result.append(newNumber((Integer.parseInt(m.group(1))+12)+"."+m.group(2),"oc")).append(m.group(3));
        }while(m.find());      
        for(int i=pos;i<s.length();i++) result.append(s.charAt(i));
        return(result);
      }
    },
    new FindReplace("(\\d+):(\\d{2})(?::(\\d{2})(?:\\.(\\d+))?)?\\s*h",null) {      
      public StringBuilder apply(StringBuilder s) {           
        Matcher m=Pattern.compile(pattern).matcher(s);
        if(!m.find()) return(s);      
        StringBuilder result=new StringBuilder(s.length()+10);
        int pos=0;
        do{
          for(int i=pos;i<m.start();i++) result.append(s.charAt(i));
          pos=m.end();
          double val=Double.parseDouble(m.group(1))*3600+Double.parseDouble(m.group(2))*60;
          if(m.group(3)!=null) val+=Double.parseDouble(m.group(3));
          if(m.group(4)!=null) val+=Double.parseDouble("0."+m.group(4));
          result.append(newNumber(val,"s")).append(' ');
        }while(m.find());      
        for(int i=pos;i<s.length();i++) result.append(s.charAt(i));
        return(result);
      }
    },
    new FindReplace("(\\d+)\\s*h(?:ours?)?\\W*(\\d+)\\s*min(?:utes)?",null) {      
      public StringBuilder apply(StringBuilder s) {           
        Matcher m=Pattern.compile(pattern).matcher(s);
        if(!m.find()) return(s);      
        StringBuilder result=new StringBuilder(s.length()+10);
        int pos=0;
        do{
          for(int i=pos;i<m.start();i++) result.append(s.charAt(i));
          pos=m.end();
          double val=Double.parseDouble(m.group(1))*3600+Double.parseDouble(m.group(2))*60;
          result.append(newNumber(val,"s")).append(' ');
        }while(m.find());      
        for(int i=pos;i<s.length();i++) result.append(s.charAt(i));
        return(result);
      }
    },
    new FindReplace(INT+B+"(?:'|[Ff]t\\.?|[fF]eet)"+B+INT+B+"(?:\"|[iI]ns?\\.?|[iI]nch(?:es))"+FB,null) {      
      public StringBuilder apply(StringBuilder s) {           
        Matcher m=Pattern.compile(pattern).matcher(s);
        if(!m.find()) return(s);      
        StringBuilder result=new StringBuilder(s.length()+10);
        int pos=0;
        do{
          for(int i=pos;i<m.start();i++) result.append(s.charAt(i));
          pos=m.end();
          result.append(newNumber(Integer.parseInt(m.group(1))*0.3048+Integer.parseInt(m.group(2))*0.0254,"m")).append(m.group(3));
        }while(m.find());      
        for(int i=pos;i<s.length();i++) result.append(s.charAt(i));
        return(result);
      }
    },

    // --------- SI-Units ---------------
    new FindReplace(INT+B+"o'?clock am"+FB,newNumber("$1","oc")+"$2"),
    new FindAdd("o'?clock pm","oc",12),
    new FindReplace(INT+B+"o'?clock",newNumber("$1","oc")),
    new FindReplace(INT+':'+INT+B+"am"+FB,newNumber("$1.$2","oc")+"$3"),    
    new FindReplace(INT+B+"am"+FB,newNumber("$1","oc")+"$2"),
    new FindAdd("pm","oc",12),
    new FindReplace(SINT+","+DIG+'%',newNumber("$1.$2","%")),
    new FindReplace(FLOAT+B+'%',newNumber("$1","%")),    
    new FindReplace("(?i:US\\$|USD|\\$|\\$US)"+B+FLOAT,newNumber("$1","dollar")),        
    new FindReplace("(?i:eur|euro|euros)"+B+FLOAT,newNumber("$1","euro")),    
    new FindCompute("((?i:dollars|dollar|\\$|US\\$|USD|\\$US))","dollar"),       
    new FindCompute("((?i:euro?s?))","euro"),
    new FindCompute("(metres|meters|meter|metre|m)\\^2","m^2"),
    new FindCompute("(metres|meters|meter|metre|m)\\^3","m^3"),
    new FindCompute("(metres|meters|meter|metre|m)","m"),
    new FindCompute("(square|sq)"+B+P+"(metres|meters|meter|metre|m)","m^2"),
    new FindCompute("(cubic|cu)"+B+P+"(metres|meters|meter|metre|m)","m^3"),
    new FindCompute("(g|grams|gram)", "g"),
    new FindCompute("(seconds|s)", "s"),
    new FindCompute("(amperes|ampere|A)", "a"),
    new FindCompute("(Kelvin|K)", "K"),
    new FindCompute("(Mol|mol)", "mol"),
    new FindCompute("(candela|cd)", "can"),
    new FindCompute("(radians|rad)", "rad"),
    new FindCompute("(hertz|Hz)", "hz"),
    new FindCompute("(newton|N)", "N"),
    new FindCompute("(joule|J)", "J"),
    new FindCompute("(watt|W)", "W"),
    new FindCompute("(pascal|Pa|pa)", "pa"),
    new FindCompute("(lumen|lm)", "lm"),
    new FindCompute("(lux|lx)", "lx"),
    new FindCompute("(coulomb|C)", "C"),
    new FindCompute("(volt|V)", "V"),
    new FindCompute("(ohm|O)", "ohm"),
    new FindCompute("(farad|F)", "F"),
    new FindCompute("(weber|Wb)", "Wb"),
    new FindCompute("(tesla|T)", "T"),
    new FindCompute("(henry|H)", "H"),
    new FindCompute("(siemens|S)", "S"),
    new FindCompute("(becquerel|Bq)", "Bq"),
    new FindCompute("(gray|Gy)", "Gy"),
    new FindCompute("(sievert|Sv)", "Sv"),
    new FindCompute("(katal|kat)", "kat"),
    new FindCompute("(bytes|b|Bytes|B)", "byte"),

    // --------- Quasi-SI-Units ---------------
    new FindCompute("(degrees|degree)"+B+"(Celsius|C)", "kelvin",1,+273.15),
    new FindCompute("([Mm]inutes|[Mm]inute|[Mm]in|[Mm]ins)", "s",60,0),
    new FindCompute("(hours|hour|h)", "s",3600,0),
    new FindCompute("(days|day)", "s",86400,0),    
    new FindCompute("(litres|liters|litre|liter|l|L)", "m^3",0.001,0),
    new FindCompute("(metric )?(tonnes|tons|tonne|ton|t)", "g",1000000,0),

    // --------- Non-SI-Units ---------------
    new FindCompute("nautical (miles|mile)","m",1852,0),
    new FindCompute("(knots|knot)","m/h",1852,0),
    new FindCompute("(hectares|hectare|ha)", "m^2",10000,0),
    new FindCompute("bar", "pa",100000,0),
    new FindCompute("(inches|inch|in)\\^2", "m^2",0.00064516,0),
    new FindCompute("(foot|ft|feet)\\^2","m^2",0.0009290304,0),
    new FindCompute("(miles|mile|mi)\\^2", "m^2",2589988.110336,0),
    new FindCompute("(inches|inch|in)\\^3", "m^3",0.000016387064,0),
    new FindCompute("(feet|foot|ft)\\^3","m^3",0.028317,0),
    new FindCompute("(yards|yard|yd)\\^3","m^3",0.007646,0),
    new FindCompute("(inches|inch|in)","m",0.0254,0),
    new FindCompute("(foot|feet|ft)","m",0.3048,0),
    new FindCompute("(yards|yard|yd)","m",0.9144,0),
    new FindCompute("(miles|mile|mi)", "m",1609.344,0),
    new FindCompute("(square|sq)"+B+"(inches|inch|in)", "m^2",0.00064516,0),
    new FindCompute("(square|sq)"+B+"(foot|ft|feet)","m^2",0.09290304,0),
    new FindCompute("(acres|acre)","m^2",4046.8564224,0),
    new FindCompute("(square|sq)"+B+"(miles|mile|mi)", "m^2",2589988.11,0),
    new FindCompute("(cubic|cu)"+B+"(inches|inch|in)", "m^3",0.000016387064,0),
    new FindCompute("(cubic|cu)"+B+"(feet|foot|ft)","m^3",0.0283168466,0),
    new FindCompute("(cubic|cu)"+B+"(yards|yard|yd)","m^3",0.764554858,0),
    new FindCompute("acre-foot","m^2",12334.818,0),
    new FindCompute("(gallon|gallons|gal)","m^2",0.0037854118,0),
    new FindCompute("(ounces|ounce|oz)","g",28.3495231,0),
    new FindCompute("(pounds|pound|lb|lbs)","g",453.59237,0),
    new FindCompute("(stones|stone)","g",6350.29318,0),
    new FindCompute("(degrees? Fahrenheit|degrees? F|Fahrenheit)","K",0.55555555555,+459.67),
    new FindCompute("(degrees|degree)","rad",0.0174532925,0)

    // ---------- Hyphens ---------------
    /*new FindReplace(FLOAT+H+FLOAT+B+UNIT+"&%([^&]*)&",newNumber("$1","$3")+"&%$4& to "+newNumber("$2","$3")+"&%$4&"),
    new FindReplace(FLOAT+H+FLOAT+B+UNIT+"&\\*([^&]*)&",newNumber("$1","$3")+"&*$4& to "+newNumber("$2","$3")+"&*$4&"),
    new FindReplace(FLOAT+H+FLOAT,newNumber("$1")+" to "+newNumber("$2"))*/

    // --------- Speed ----------
    /*new FindReplace(MNUMBER+" &%([^&]*)&"+B+"(per|/)"+B+"(hour|h)"+FB,newNumber("$1","ms")+"&*3600&&%$2&$5"),
    new FindReplace(MNUMBER+" &\\*([^&]*)&"+B+"(per|/)"+B+"(hour|h)"+FB,newNumber("$1","ms")+"&*3600&&*$2&$5"),
    new FindReplace(MNUMBER+B+"(per|/)"+B+"(hour|h)"+FB,newNumber("$1","ms")+"$4"),
    new FindReplace(MNUMBER+B+"(per|/)"+B+"(second|s)"+FB,newNumber("$1","ms")+"$4"),        */

  };

  /** Normalizes all numbers in a String */
  public static String normalize(CharSequence s) {
    StringBuilder result=new StringBuilder(" ").append(s).append(' ');
    for(FindReplace fr : patterns) {      
      result=fr.apply(result);
    }
    return(Char.cutLast(result.toString().substring(1)));
  }

  /** Extracts the pure number from a String containing a normalized number, else null */
  public static String getNumber(String d) {    
    Matcher m=NUMBERPATTERN.matcher(d);
    if(m.find()) return(m.group(1));
    return(null);
  }

  /** Extracts the number and its unit from a String containing a normalized number, else null,
   * returns start and end position in pos[0] and pos[1] resp. */
  public static String[] getNumberAndUnit(String d, int[] pos) {    
    Matcher m=NUMBERPATTERN.matcher(d);
    if(!m.find()) return(null);
    pos[0]=m.start();
    pos[1]=m.end();
    return(new String[]{m.group(1),m.group(2)});    
  }

  /** Converts a String that contains a (non-normalized) number to a double or null */
  public static Double toDouble(String d) {
    String number=getNumber(normalize(d));
    if(number==null) return(null);
    return(new Double(number));
  }

  /** Converts a String that contains a (non-normalized) number to a long or null */
  public static Long toLong(String d) {
    String number=getNumber(normalize(d));
    if(number==null) return(null);
    return(new Double(number).longValue());
  }
  
  /** Test method*/
  public static void main(String[] argv) throws Exception {    
    System.out.println("Enter a string containing a number and hit ENTER. Press CTRL+C to abort");
    while(true) {
     String in=D.r();
     System.out.println(normalize(in));
     System.out.println(toDouble(in));
    }
  }

}
