package yago.javatools;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
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
  
The class MySQLDatabase implements the Database-interface for a
MySQL data base. Make sure that the file 
"mysql-connector-java-<i>version</i>-bin.jar" from the "MySQL Connector/J" 
(see the <A HREF=http://dev.mysql.com/downloads/ TARGET=_blank>MySQL-website</A>)
is in the classpath. When using Eclipse, add the file via Project 
->Properties ->JavaBuildPath ->Libraries ->ExternalJARFile.<BR>
Example:
<PRE>
     Database d=new MySQLDatabase("user","password","database");     
     d.queryColumn("SELECT foodname FROM food WHERE origin=\"Italy\"")
     -> [ "Pizza Romana", "Spaghetti alla Bolognese", "Saltimbocca"]
     Database.describe(d.query("SELECT * FROM food WHERE origin=\"Italy\"")
     -> foodname |origin  |calories |
        ------------------------------
        Pizza Rom|Italy   |10000    |
        Spaghetti|Italy   |8000     |
        Saltimboc|Italy   |8000     |        
</PRE>
*/
public class MySQLDatabase extends Database {  
  
  /** Constructs a new MySQLDatabase from a user and a password,
   * all other arguments may be null*/
  public MySQLDatabase(String user, String password, String database, String host, String port) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException  {
    Driver driver=(Driver)Class.forName("com.mysql.jdbc.Driver").newInstance();
    DriverManager.registerDriver( driver );
    if(host==null) host="localhost";
    if(database==null) database="test";
    if(port==null) port="";
    else port=":"+port; 
    connection = DriverManager.getConnection(
          "jdbc:mysql://"+host+port+"/"+database+"?user="+user+"&password="+password);
    connection.setAutoCommit( true );  
  }
  
  public static void main(String[] args) throws Exception {
  }
}
