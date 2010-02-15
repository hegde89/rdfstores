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

import java.util.Collection;

import org.apache.log4j.Logger;

import com.sleepycat.collections.StoredMap;

/**
 * @author andi
 * 
 */
public class StorageHelperThread<K, V> extends Thread {

	private static Logger s_log = Logger.getLogger(StorageHelperThread.class);

	/*
	 * 
	 */
	private StoredMap<K, V> m_map;

	/*
	 * 
	 */
	private K m_key;
	private V m_value;
	private Collection<V> m_values;

	public StorageHelperThread(StoredMap<K, V> map, K key, Collection<V> values) {

		super("StorageHelperThread");

		m_map = map;
		m_key = key;
		m_values = values;
	}

	public StorageHelperThread(StoredMap<K, V> map, K key, V value) {

		super("StorageHelperThread");

		m_map = map;
		m_key = key;
		m_value = value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {

		if ((m_key != null) && (m_value != null)) {

			m_map.put(m_key, m_value);
			s_log.debug("stored key '" + m_key + "' value '" + m_value + "'");
		}

		if ((m_key != null) && (m_values != null)) {

			for (V value : m_values) {
				m_map.put(m_key, value);
			}

			s_log.debug("stored key '" + m_key + "' values '" + m_values + "'");
		}

		m_key = null;
		m_value = null;
		m_values = null;
	}
}
