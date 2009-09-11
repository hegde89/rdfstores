/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */
package edu.unika.aifb.facetedSearch.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Stack;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * @author andi
 * 
 */
public class FacetDbUtils {

	public static class DatabaseNames {

		public static final String TREE = "tree_db";

		public static final String LEAVE = "leave_db";

		public static final String ENDPOINT = "endpoint_db";

		public static final String FH_CACHE = "fh_cache_db";

		public static final String FTB_CACHE = "ftb_cache_db";

		public static final String FDB_CACHE = "fdb_cache_db";

		public static final String VECTOR = "vector_db";

		public static final String LITERAL = "literal_db";
	}

	public static class DbConfigFactory {

		public static DatabaseConfig make(boolean allowDups) {

			DatabaseConfig config = new DatabaseConfig();
			config.setTransactional(false);
			config.setAllowCreate(true);
			config.setSortedDuplicates(allowDups);
			config.setDeferredWrite(true);

			return config;
		}
	}

	public static class EnvironmentFactory {

		public static Environment make(File dir)
				throws EnvironmentLockedException, DatabaseException {

			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setTransactional(false);
			envConfig.setAllowCreate(true);

			return new Environment(dir, envConfig);
		}
	}

	public static final String KEY_DELIM = Character.toString((char) 31);

	@SuppressWarnings( { "unchecked" })
	public static <T> T bytes2Object(byte[] bytes) {

		T object = null;
		try {

			object = (T) new ObjectInputStream(new ByteArrayInputStream(bytes))
					.readObject();

		} catch (IOException e) {
			e.printStackTrace();

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return object;
	}

	public static boolean contains(Database db, String key)
			throws DatabaseException, IOException {
		return get(db, key) == null ? false : true;
	}

	@SuppressWarnings("unchecked")
	public static <T> T get(Database db, String key) throws DatabaseException,
			IOException {

		T res = null;
		Cursor cursor = db.openCursor(null, null);
		DatabaseEntry keyEntry = new DatabaseEntry(object2Bytes(key));
		DatabaseEntry out = new DatabaseEntry();

		if (cursor.getSearchKey(keyEntry, out, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			res = (T) bytes2Object(out.getData());
		}

		cursor.close();

		return res;
	}

	@SuppressWarnings("unchecked")
	public static <T> Stack<T> getAll(Database db, String key)
			throws DatabaseException, IOException {

		Stack<T> res = new Stack<T>();
		Cursor cursor = db.openCursor(null, null);
		DatabaseEntry keyEntry = new DatabaseEntry(object2Bytes(key));
		DatabaseEntry out = new DatabaseEntry();

		while (cursor.getNext(keyEntry, out, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			Object nextRes = bytes2Object(out.getData());
			res.add((T) nextRes);

		}

		cursor.close();

		return res;
	}

	public static String getKey(String... elems) {

		String key = "";

		for (int i = 0; i < elems.length; i++) {

			key += elems[i];

			if (i != elems.length - 1) {
				key += FacetDbUtils.KEY_DELIM;
			}
		}

		return key;
	}

	@SuppressWarnings("unchecked")
	public static <T> T getNext(Cursor cursor, String key, String entry)
			throws DatabaseException, IOException {

		T res = null;

		DatabaseEntry keyEntry = new DatabaseEntry(object2Bytes(key));
		DatabaseEntry out = entry == null ? new DatabaseEntry()
				: new DatabaseEntry(object2Bytes(entry));

		if (cursor.getNext(keyEntry, out, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			Object nextRes = bytes2Object(out.getData());
			res = (T) nextRes;
		}

		return res;
	}

	public static <T> byte[] object2Bytes(T object) {

		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

		try {

			ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
			objectStream.writeObject(object);

		} catch (IOException e) {
			e.printStackTrace();
		}

		return byteStream.toByteArray();
	}

	public static void store(Database db, String key, Object object2store) {

		try {
			db.put(null, new DatabaseEntry(object2Bytes(key)),
					new DatabaseEntry(object2Bytes(object2store)));
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
}
