package yago.javatools;

import java.sql.*;

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
  
The class PostgresDatabase implements the Database-interface for a
PostgreSQL data base. Make sure that the file "postgresql-<I>version</I>.jdbc3.jar" of the 
Postgres distribution is in the classpath. When using Eclipse, add 
the file via Project ->Properties ->JavaBuildPath ->Libraries 
->ExternalJARFile.<BR>
Example:
<PRE>
     Database d=new PostgresDatabase("user","password");     
     d.queryColumn("SELECT foodname FROM food WHERE origin=\"Italy\"")
     -> [ "Pizza Romana", "Spaghetti alla Bolognese", "Saltimbocca"]
     Database.describe(d.query("SELECT * FROM food WHERE origin=\"Italy\"")
     -> foodname |origin  |calories |
        ------------------------------
        Pizza Rom|Italy   |10000    |
        Spaghetti|Italy   |8000     |
        Saltimboc|Italy   |8000     |        
</PRE>
This class also provides SQL datatypes (extensions of SQLType.java) that
behave according to the conventions of Postgres. For example, VARCHAR string literals print 
inner quotes as doublequotes.*/
public class PostgresDatabase extends Database {

  /** Constructs a non-functional OracleDatabase for use of getSQLType*/
  public PostgresDatabase() {
    java2SQL.put(String.class,varchar);
    type2SQL.put(Types.VARCHAR,varchar);    
  }
  
  /** Constructs a new OracleDatabase from a user, a password and a host
   * @throws ClassNotFoundException 
   * @throws IllegalAccessException 
   * @throws InstantiationException 
   * @throws SQLException */
  public PostgresDatabase(String user, String password, String database, String host, String port) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException  {
    this();
    if(password==null) password="";
    if(host==null) host="localhost";
    if(port==null) port="5432";
    Driver driver= (Driver)Class.forName("org.postgresql.Driver").newInstance();
    DriverManager.registerDriver( driver );
    connection = DriverManager.getConnection(
          "jdbc:postgresql://"+host+":"+port+"/"+database,
        user,
        password
      );
    connection.setAutoCommit( true );
    description="Postgres database "+user+"/"+password+" at "+host+":"+port+" database "+database;
  }  

  public static class Varchar extends SQLType.ANSIvarchar {
    public Varchar(int size) {
      super(size);
    }  
    public Varchar() {
      super();
    } 
    public String toString() {
      return("VARCHAR("+scale+")");
    }
    public String format(Object o) {
      String s=o.toString().replace("'", "''");
      if(s.length()>scale) s=s.substring(0,scale);
      return("'"+s+"'");
    } 
  }
  public static Varchar varchar=new Varchar();
  /**  */
  public static void main(String[] args) throws Exception {
    Database d=new PostgresDatabase("postgres","postgres","postgres",null,null);
    //d.executeUpdate("CREATE table test (a integer, b varchar)");
    d.executeUpdate("INSERT into test values (1,2)");
    ResultSet s=d.query("select * from test");
    s.next();
    D.p(s.getString(1));
  }

}
