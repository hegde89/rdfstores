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
package edu.unika.aifb.facetedSearch.connection.impl;

import java.util.Properties;

import edu.unika.aifb.facetedSearch.connection.IConnectionProvider;

/**
 * @author andi
 * 
 */
public class RdfStoreConnectionProvider implements IConnectionProvider {

	private static IConnectionProvider s_instance;
	private Properties m_props;

	private RdfStoreConnectionProvider(Properties props) {
		m_props = props;
	}

	public static IConnectionProvider getInstance(Properties props) {
		return s_instance == null ? s_instance = new RdfStoreConnectionProvider(
				props)
				: s_instance;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.connection.IConnectionProvider#close()
	 */
	public void close() {
		// TODO 
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.connection.IConnectionProvider#closeConnection
	 * (edu.unika.aifb.facetedSearch.connection.IConnection)
	 */
	public void closeConnection(RdfStoreConnection con) {
		con.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.connection.IConnectionProvider#getConnection
	 * ()
	 */
	public RdfStoreConnection getConnection() {
		return RdfStoreConnection.getInstance(m_props);
	}
}
