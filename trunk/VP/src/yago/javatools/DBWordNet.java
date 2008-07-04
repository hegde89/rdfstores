package yago.javatools;
import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.sql.*;

import yago.javatools.WordNet.*;
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

This class provides a wrapping for <A HREF=http://wordnet.princeton.edu/>WordNet</A>
by a database. The database can be e.g. an OracleDatabase or a MySQLDatabase.
Before creating instances of this class, you have to build the
database by the construct-method. This method requires the Prolog-version of WordNet,
which can be downloaded from the WordNet-website.<BR>
Example:
<PRE>
  // Execute this only once to create the database!
  Database database=new OracleDatabase("scott","tiger");
  DBWordNet.construct(database, new File("wordnet/prologversion"));
  
  // Use this code to access the data
  Database database=new OracleDatabase("scott","tiger");
  DBWordNet wordnet = new DBWordNet(database);
  System.out.println(wordnet.synsetsFor("house"));
  --->     103413667, 103413668, 103413669 (or similar)
  System.out.println(wordnet.glossFor(103413667));  
  --->     a dwelling that serves as living quarters for one or more families
</PRE>
 */ 
public class DBWordNet {

  /** One enum per SQL table*/
  public enum Table {    
    SYNSETS("wn_s.pl",
          "synset_id",Types.INTEGER,
          "w_num",Types.INTEGER,
          "word",Types.VARCHAR,
          "ss_type",Types.CHAR,
          "sense_number",Types.INTEGER,
          "tag_count",Types.INTEGER),    
    GLOSSES("wn_g.pl",
          "synset_id",Types.INTEGER,
          "gloss",Types.VARCHAR),        
    HYPONYMY("wn_hyp.pl",
          "from_ss",Types.INTEGER,
          "to_ss",Types.INTEGER),
    ENTAILMENT("wn_ent.pl",
          "from_ss",Types.INTEGER,
          "to_ss",Types.INTEGER),
    SIMILARITY("wn_sim.pl",
          "from_ss",Types.INTEGER,
          "to_ss",Types.INTEGER),
    MEREONYMY_M("wn_mm.pl",
          "from_ss",Types.INTEGER,
          "to_ss",Types.INTEGER),
    MEREONYMY_S("wn_ms.pl",
          "from_ss",Types.INTEGER,
          "to_ss",Types.INTEGER),
    MEREONYMY_P("wn_mp.pl",
          "from_ss",Types.INTEGER,
          "to_ss",Types.INTEGER),
    MORPH("wn_der.pl",
          "from_ss",Types.INTEGER,
          "to_ss",Types.INTEGER),
    CLASS("wn_cls.pl",
          "from_ss",Types.INTEGER,
          "to_ss",Types.INTEGER,
          "class_type",Types.CHAR),    
    ANTONYMY("wn_ant.pl",
          "from_synset",Types.INTEGER,
          "from_wnum",Types.INTEGER,
          "to_synset",Types.INTEGER,
          "to_wnum",Types.INTEGER),            
    SEEALSO("wn_sa.pl",
          "from_synset",Types.INTEGER,
          "from_wnum",Types.INTEGER,
          "to_synset",Types.INTEGER,
          "to_wnum",Types.INTEGER),                
    PARTICIPLE("wn_ppl.pl",
          "from_synset",Types.INTEGER,
          "from_wnum",Types.INTEGER,
          "to_synset",Types.INTEGER,
          "to_wnum",Types.INTEGER),                
    PERTAIN("wn_per.pl",
          "from_synset",Types.INTEGER,
          "from_wnum",Types.INTEGER,
          "to_synset",Types.INTEGER,
          "to_wnum",Types.INTEGER),                
    CAUSE("wn_cs.pl",
          "from_ss",Types.INTEGER,
          "to_ss",Types.INTEGER),        
    VERBROUP("wn_vgp.pl",
          "from_ss",Types.INTEGER,
          "null1",Types.INTEGER, // bug in the wordnet files...
          "to_ss",Types.INTEGER,
          "null2",Types.INTEGER),            
    ATTRIBUTE("wn_at.pl",
          "from_ss",Types.INTEGER,
          "to_ss",Types.INTEGER),        
    FRAME("wn_fr.pl",
          "synset",Types.INTEGER,
          "frame",Types.INTEGER,
          "wnum",Types.INTEGER);
    
    /** Holds the file this table was loaded from*/
    protected String file;
    /** Holds an alternating sequence of attribute name and attribute type */
    protected Object[] attributes;
    private Table(String file, Object... attributes) {
      this.file=file;
      this.attributes=attributes;
    }
    /** Returns an alternating sequence of attribute name (String) 
     * and attribute type (from java.sql.Types)*/
    public Object[] getAttributes() {
      return attributes;
    }
    /** Returns the filename this table was loaded from*/
    public String getFile() {
      return file;
    }
    /** Loads this table to a given database from a folder with the Prolog version of WordNet */
    public void load(Database db, File wordNetFolder) throws IOException, SQLException{
      String relation="WN_"+name();
      db.createTable(relation, attributes);
      StringBuilder b=new StringBuilder("\\w*\\(");
      SQLType[] types=new SQLType[attributes.length/2];
      for(int i=0;i<types.length;i++) {
        int type=(Integer)attributes[i*2+1];
        types[i]=db.getSQLType(type);
        switch(type) {
          case Types.INTEGER: b.append("(\\d+),"); break;
          case Types.VARCHAR: b.append("'([^']*)',"); break;
          case Types.CHAR: b.append("(.),"); break;
          default: b.append("([^,]*),"); break;
        }      
      }
      b.setLength(b.length()-1);
      b.append("\\).");
      Pattern pattern=Pattern.compile(b.toString());
      b=new StringBuilder("INSERT INTO "+relation+" VALUES (");
      int chopoff=b.length();
      for(String l : new FileLines(new File(wordNetFolder,file),"Loading "+relation)) {
        Matcher m=pattern.matcher(l.replace("''", "`"));
        if(!m.matches()) {
          Announce.warning("No match for",l);
          continue;
        }
        b.setLength(chopoff);
        for(int i=0;i<types.length;i++) {
          b.append(types[i].format(m.group(i+1))).append(", ");
        }
        b.setLength(b.length()-2);
        b.append(")");
        db.query(b.toString());
      }      
    }
  }
  
  /** Holds the database */
  protected Database database;
  
  /** Constructs a new DBWordNet, basing on a filled database*/
  public DBWordNet(Database db) {
    database=db;
  }
  
  /** Fills the database with WordNet data */
  public static void construct(Database db, File wordNetFolder) throws SQLException, IOException{
     for(Table t : Table.values()) {
       t.load(db,wordNetFolder);
     }
  }
  
  /** Returns target synsets for a relation and a source synset */
  public List<Integer> targetSynsets(Table relation, int source) throws SQLException{
    List<Integer> result=new ArrayList<Integer>();
    for(Integer i : database.query("SELECT to_ss FROM wn_"+relation+
          " WHERE from_ss="+database.getSQLType(Integer.class).format(source),
          ResultIterator.IntegerWrapper)) {
      result.add(i);
    }
    return(result);
  }

  /** Returns synsets for a word */
  public List<Integer> synsetsFor(String word) throws SQLException{
    List<Integer> result=new ArrayList<Integer>();
    for(Integer i : database.query("SELECT synset_id FROM wn_synsets"+
          " WHERE word="+database.getSQLType(String.class).format(word),
          ResultIterator.IntegerWrapper)) {
      result.add(i);
    }
    return(result);
  }

  /** Returns words of a synset */
  public List<String> wordsFor(int synset) throws SQLException{
    List<String> result=new ArrayList<String>();
    for(String s : database.query("SELECT word FROM wn_synsets"+
          " WHERE synset_id="+database.getSQLType(String.class).format(synset),
          ResultIterator.StringWrapper)) {
      result.add(s);
    }
    return(result);
  }
  
  /** Returns the gloss for a synset */
  public String glossFor(int synset) throws SQLException{
    return(database.queryValue("SELECT gloss FROM wn_glosses"+
          " WHERE synset_id="+database.getSQLType(Integer.class).format(synset),
          ResultIterator.StringWrapper));
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception{
     //construct(new OracleDatabase("yago","yago"),new File("C:\\program files\\wordnet\\2.1\\prolog"));
    Table.VERBROUP.load(new OracleDatabase("yago","yago"),new File("C:\\program files\\wordnet\\2.1\\prolog"));
  }
  

  /** Returns the database */
  public Database getDatabase() {
    return database;
  }

}
