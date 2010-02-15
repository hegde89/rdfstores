/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer Project. 
 * 
 * Faceted Search Layer Project is free software: you can redistribute
 * it and/or modify it under the terms of the GNU General Public License, 
 * version 2 as published by the Free Software Foundation. 
 *  
 * Faceted Search Layer Project is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 * GNU General Public License for more details. 
 *  
 * You should have received a copy of the GNU General Public License 
 * along with Faceted Search Layer Project.  If not, see <http://www.gnu.org/licenses/>. 
 */
package edu.unika.aifb.facetedSearch.index.db;

import org.apache.log4j.Logger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DeadlockException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;

import edu.unika.aifb.facetedSearch.FacetEnvironment;

/**
 * @author andi
 * 
 */
public class TransactionalStorageHelperThread<K, V> extends Thread {

	private static Logger s_log = Logger
			.getLogger(TransactionalStorageHelperThread.class);

	/*
	 * 
	 */
	private Database m_db;
	private Environment m_env;

	/*
	 * 
	 */
	private EntryBinding<K> m_keyBinding;
	private EntryBinding<V> m_valueBinding;

	/*
	 * 
	 */
	private K m_key;
	private V m_value;

	public TransactionalStorageHelperThread(Environment env, Database db,
			EntryBinding<K> keyBinding, EntryBinding<V> valueBinding, K key,
			V value) throws DatabaseException {

		setName("TransactionalStorageHelperThread:" + getId());

		m_db = db;
		m_env = env;

		m_keyBinding = keyBinding;
		m_valueBinding = valueBinding;

		m_key = key;
		m_value = value;
	}

	@Override
	public void run() {

		Transaction txn = null;

		boolean retry = true;
		int retry_count = 0;

		while (retry) {

			try {

				// Get a transaction
				txn = m_env.beginTransaction(null, null);

				DatabaseEntry key = new DatabaseEntry();
				m_keyBinding.objectToEntry(m_key, key);
				DatabaseEntry value = new DatabaseEntry();
				m_valueBinding.objectToEntry(m_value, value);

				m_db.put(txn, key, value);

				// commit
				s_log.debug(getName() + " : committing txn!");

				try {

					txn.commit();
					txn = null;

				} catch (DatabaseException e) {
					s_log.debug("Error on txn commit: " + e.toString());
				}

				retry = false;

			} catch (DeadlockException de) {

				// retry if necessary
				if (retry_count < FacetEnvironment.DefaultValue.DB_WRITER_MAX_RETRY) {

					s_log.debug(getName() + " : retry ... ");
					retry = true;
					retry_count++;

				} else {

					s_log.debug(getName() + "giving up!!");
					retry = false;
				}

			} catch (DatabaseException e) {

				retry = false;
				s_log.debug(getName() + "DatabaseException!!");
				e.printStackTrace();

			} finally {
				if (txn != null) {
					try {
						txn.abort();
						s_log.debug(getName() + ": txn.abort()");
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}