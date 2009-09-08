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

import java.io.IOException;
import java.util.Stack;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import edu.unika.aifb.graphindex.util.Util;

/**
 * @author andi
 * 
 */
public class FacetDbUtils {

	public class DatabaseNames {

		public static final String TREE = "tree_db";

		public static final String LEAVE = "leave_db";

		public static final String ENDPOINT = "endpoint_db";

		public static final String FH_CACHE = "fh_cache_db";

		public static final String FTB_CACHE = "ftb_cache_db";

		public static final String FDB_CACHE = "fdb_cache_db";

		public static final String VECTOR = "vector_db";

		public static final String LITERAL = "literal_db";
	}

	public static final String KEY_DELIM = Character.toString((char) 31);

	@SuppressWarnings("unchecked")
	public static <T> T get(Database db, String key) throws DatabaseException,
			IOException {

		T res = null;
		Cursor cursor = db.openCursor(null, null);
		DatabaseEntry keyEntry = new DatabaseEntry(Util.objectToBytes(key));
		DatabaseEntry out = new DatabaseEntry();

		if (cursor.getSearchKey(keyEntry, out, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			res = (T) Util.bytesToObject(out.getData());
		}

		cursor.close();

		return res;
	}

	@SuppressWarnings("unchecked")
	public static <T> Stack<T> getAll(Database db, String key)
			throws DatabaseException, IOException {

		Stack<T> res = new Stack<T>();
		Cursor cursor = db.openCursor(null, null);
		DatabaseEntry keyEntry = new DatabaseEntry(Util.objectToBytes(key));
		DatabaseEntry out = new DatabaseEntry();

		while (cursor.getNext(keyEntry, out, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			Object nextRes = Util.bytesToObject(out.getData());
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

	public static void store(Database db, String key, Object object2store) {

		try {
			db.put(null, new DatabaseEntry(Util.objectToBytes(key)),
					new DatabaseEntry(Util.objectToBytes(object2store)));
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
