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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * @author andi
 * 
 */
public class FacetDbUtils {

	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(FacetDbUtils.class);
	public static final String KEY_DELIM = Character.toString((char) 31);

	public static <T> boolean contains(Database db, String key,
			EntryBinding<T> binding) throws DatabaseException, IOException {
		return get(db, key, binding) == null ? false : true;
	}

	public static <T> T get(Database db, String key, EntryBinding<T> binding)
			throws DatabaseException, IOException {

		T res = null;

		Cursor cursor = db.openCursor(null, null);
		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry out = new DatabaseEntry();

		StringBinding.stringToEntry(key, keyEntry);

		if (cursor.getSearchKey(keyEntry, out, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			res = binding.entryToObject(out);
		}

		cursor.close();

		return res;
	}

	public static <T> List<T> getAllAsList(Database db, String key,
			EntryBinding<T> binding) throws DatabaseException, IOException {

		List<T> res = new ArrayList<T>();

		Cursor cursor = db.openCursor(null, null);

		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry out = new DatabaseEntry();

		StringBinding.stringToEntry(key, keyEntry);

		// while (cursor.getNextDup(keyEntry, out, LockMode.DEFAULT) ==
		// OperationStatus.SUCCESS) {
		// res.add(binding.entryToObject(out));
		// }

		OperationStatus retVal = cursor.getSearchKey(keyEntry, out,
				LockMode.DEFAULT);

		while (retVal == OperationStatus.SUCCESS) {

			res.add(binding.entryToObject(out));
			retVal = cursor.getNextDup(keyEntry, out, LockMode.DEFAULT);
		}

		cursor.close();

		return res;
	}

	public static <T> HashSet<T> getAllAsSet(Database db, String key,
			EntryBinding<T> binding) throws DatabaseException, IOException {

		HashSet<T> res = new HashSet<T>();

		Cursor cursor = db.openCursor(null, null);

		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry out = new DatabaseEntry();

		StringBinding.stringToEntry(key, keyEntry);

		// while (cursor.getNext(keyEntry, out, LockMode.DEFAULT) ==
		// OperationStatus.SUCCESS) {
		// res.add(binding.entryToObject(out));
		// }

		OperationStatus retVal = cursor.getSearchKey(keyEntry, out,
				LockMode.DEFAULT);

		while (retVal == OperationStatus.SUCCESS) {

			res.add(binding.entryToObject(out));
			retVal = cursor.getNextDup(keyEntry, out, LockMode.DEFAULT);
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

	public static <T> T getNext(Cursor cursor, String key, DatabaseEntry entry,
			EntryBinding<T> binding) throws DatabaseException, IOException {

		T res = null;

		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry out = entry == null ? new DatabaseEntry() : entry;

		StringBinding.stringToEntry(key, keyEntry);

		if (cursor.getNextDup(keyEntry, entry, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			res = binding.entryToObject(out);
		}

		return res;
	}

	// public static <T> byte[] object2Bytes(T object) {
	//
	// ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
	//
	// try {
	//
	// ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
	// objectStream.writeObject(object);
	//
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	//
	// return byteStream.toByteArray();
	// }

	public static <T> boolean store(Database db, String key, T entry,
			EntryBinding<T> binding) throws UnsupportedEncodingException,
			DatabaseException {

		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();

		binding.objectToEntry(entry, data);
		StringBinding.stringToEntry(key, keyEntry);

		OperationStatus status = db.put(null, keyEntry, data);

		if (status == OperationStatus.SUCCESS) {

			return true;

		} else if (status == OperationStatus.KEYEXIST) {

			// s_log.debug("already contained data for key '" + key
			// + "' ... data was overwritten!");

			return false;

		} else {

			throw new DatabaseException("data for key '" + key
					+ "' insertion got status " + status);
		}
	}

	public static <T> boolean storeNoOverwrite(Database db, String key,
			T entry, EntryBinding<T> binding)
			throws UnsupportedEncodingException, DatabaseException {

		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();

		binding.objectToEntry(entry, data);
		StringBinding.stringToEntry(key, keyEntry);

		OperationStatus status = db.putNoOverwrite(null, keyEntry, data);

		if (status == OperationStatus.SUCCESS) {

			return true;

		} else if (status == OperationStatus.KEYEXIST) {

			return false;

		} else {

			throw new DatabaseException("data for key '" + key
					+ "' insertion got status " + status);
		}
	}

	public static <T> void test(Database db, EntryBinding<T> binding) {

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry out = new DatabaseEntry();
		Cursor cursor = null;

		try {
			cursor = db.openCursor(null, null);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}

		try {
			while (cursor.getNext(key, out, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

				if (out.getData() != null) {

					T object = binding.entryToObject(out);

					System.out.println("key = " + key.getData() + " data= "
							+ object.toString());

				}
			}
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
}
