package edu.unika.aifb.graphindex;

import java.io.File;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.unika.aifb.graphindex.util.Util;

public class BdbTool {

	/**
	 * @param args
	 * @throws DatabaseException 
	 */
	public static void main(String[] args) throws DatabaseException {
		OptionParser op = new OptionParser();
		op.accepts("o", "bdb directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");
		op.accepts("db", "db name")
			.withRequiredArg().ofType(String.class);
		op.accepts("key", "key to retrieve")
			.withRequiredArg().ofType(String.class);
		op.accepts("keytype", "type of key, string or integer")
			.withRequiredArg().ofType(String.class);
		op.accepts("datatype", "type of the data, string or integer")
			.withRequiredArg().ofType(String.class);
		
		OptionSet os = op.parse(args);
		if (!os.has("o") && !os.has("db") && !os.has("key")) {
			System.out.println("wrong args");
			return;
		}
		
		String bdb = (String)os.valueOf("o");
		String dbName = (String)os.valueOf("db");
		String key = (String)os.valueOf("key");
		String keyType = (String)os.valueOf("keytype");
		String dataType = (String)os.valueOf("datatype");
		System.out.println(key);

		EnvironmentConfig config = new EnvironmentConfig();
		config.setTransactional(false);
		config.setAllowCreate(false);

		Environment env = new Environment(new File(bdb), config);

		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(false);
		dbConfig.setAllowCreate(false);
		dbConfig.setSortedDuplicates(false);
		
		Database db = env.openDatabase(null, dbName, dbConfig);
		
		DatabaseEntry dbKey;
		if (keyType != null && keyType.equals("integer"))
			dbKey = new DatabaseEntry(Util.intToBytes(Integer.parseInt(key)));
		else
			dbKey = new DatabaseEntry(key.getBytes());
		
		DatabaseEntry out = new DatabaseEntry();
		db.get(null, dbKey, out, null);
		System.out.println(out);
		
		if (out.getData() != null) {
			if (dataType != null && dataType.equals("integer"))
				System.out.println(Util.bytesToInt(out.getData()));
			else
				System.out.println(new String(out.getData()));
		}
	}

}
