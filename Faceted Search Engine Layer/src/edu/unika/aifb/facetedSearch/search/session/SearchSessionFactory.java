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
package edu.unika.aifb.facetedSearch.search.session;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.connection.impl.RdfStoreConnection;
import edu.unika.aifb.facetedSearch.connection.impl.RdfStoreConnectionProvider;
import edu.unika.aifb.facetedSearch.exception.MissingParameterException;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class SearchSessionFactory {

	private GenericRdfStore m_store;
	private RdfStoreConnection m_con;

	private static SearchSessionFactory s_instance;
	private static SearchSession s_pool[] = new SearchSession[FacetEnvironment.DefaultValue.MAX_SESSIONS];
	private static ReentrantLock[] s_locks = new ReentrantLock[FacetEnvironment.DefaultValue.MAX_SESSIONS];
	private static Semaphore s_sem = new Semaphore(
			FacetEnvironment.DefaultValue.MAX_SESSIONS);

	public static SearchSessionFactory getInstance(Properties props) {
		return s_instance == null ? s_instance = new SearchSessionFactory(props)
				: s_instance;
	}

	private SearchSessionFactory(Properties props) {

		this.m_con = RdfStoreConnectionProvider.getInstance(props)
				.getConnection();
		try {
			this.m_store = this.m_con.loadOrCreateStore();
		} catch (InvalidParameterException e1) {
			e1.printStackTrace();
		} catch (MissingParameterException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (StorageException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}

		for (int i = 0; i < FacetEnvironment.DefaultValue.MAX_SESSIONS; i++) {
			s_locks[i] = new ReentrantLock();
			try {
				s_pool[i] = new SearchSession(this.m_store, i, props);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public int acquire() throws InterruptedException {
		s_sem.acquire();
		for (int i = 0; i < FacetEnvironment.DefaultValue.MAX_SESSIONS; i++) {
			if (s_locks[i].tryLock()) {
				return i;
			}
		}
		System.err.println("acquire: All the semplore is being used!");
		return -1;
	}

	public void close() {
		this.m_con.close();
	}

	public void closeSession(SearchSession session) {
		session.clean();
		this.release(session.getId());
	}

	public SearchSession getSession(int id) {
		return s_pool[id];
	}

	public void release(int id) {
		s_locks[id].unlock();
		s_sem.release();
	}
}
