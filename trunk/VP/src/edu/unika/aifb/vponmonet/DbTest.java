package edu.unika.aifb.vponmonet;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class DbTest {
	private static Properties m_props;

	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
		m_props = new Properties();
		m_props.load(new FileInputStream("config.properties"));
		
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection conn = DriverManager.getConnection("jdbc:monetdb://" + m_props.getProperty("db_host") + "/" + m_props.getProperty("db_name"), 
				m_props.getProperty("db_user"), m_props.getProperty("db_password"));
		
//		ObjectInputStream ois = new ObjectInputStream(new FileInputStream("output.string"));
//		Object o = ois.readObject();
//		
//		String s = (String)o;
//		System.out.println(s);
//		int min = Integer.MAX_VALUE, max = 0;
//		for (char t : s.toCharArray()) {
//			System.out.println(t + ": " + (int)t);
//			if ((int)t > max)
//				max = (int)t;
//			if ((int)t < min)
//				min = (int)t;
//		}
//		System.out.println(min + " " + max);
//		System.out.println(s.length());
//		s = s.substring(0, 200);

		PreparedStatement pst = conn.prepareStatement("INSERT INTO http___dbpedia_org_property_othernames (subject, object) VALUES (?, ?)");
		pst.setString(1, "bla");
		pst.setString(2, "asd");
//		pst.setString(2, "asds''ad");
		pst.execute();
		
		System.out.println(URIHash.hash("http://www.Department0.University0.edu/FullProfessor4/Publication15"));
		System.out.println(URIHash.hash("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"));
	}
}
