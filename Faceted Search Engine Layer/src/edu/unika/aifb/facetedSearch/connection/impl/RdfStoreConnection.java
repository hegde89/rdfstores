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

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Properties;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.connection.IConnection;
import edu.unika.aifb.facetedSearch.exception.ExceptionHelper;
import edu.unika.aifb.facetedSearch.exception.MissingParameterException;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class RdfStoreConnection implements IConnection {

	private static RdfStoreConnection s_instance;
	private Properties m_props;

	private RdfStoreConnection(Properties props) {
		m_props = props;
	}

	protected static RdfStoreConnection getInstance(Properties props) {
		return s_instance == null ? s_instance = new RdfStoreConnection(props)
				: s_instance;
	}

	public void close() {
		// TODO
	}

	public GenericRdfStore createStore() throws MissingParameterException,
			InvalidParameterException, IOException, StorageException,
			InterruptedException {

		return new GenericRdfStore(m_props,FacetEnvironment.StoreAction.CREATE_STORE);
	}

	public GenericRdfStore loadOrCreateStore() throws InvalidParameterException,
			MissingParameterException, IOException, StorageException,
			InterruptedException {

		GenericRdfStore store = null;

//		try{
//			store = loadStore();
//		}
//		catch(Exception e){
			store = createStore();
//		}		

		return store;
	}

	public GenericRdfStore loadStore() throws MissingParameterException,
			InvalidParameterException, IOException, StorageException,
			InterruptedException {

		String idxDir = m_props.getProperty(FacetEnvironment.Property.INDEX_DIRECTORY);

		if (idxDir == null) {
			throw new MissingParameterException(ExceptionHelper.createMessage(
					FacetEnvironment.Property.INDEX_DIRECTORY, ExceptionHelper.Cause.MISSING));
		}

		return new GenericRdfStore(m_props, FacetEnvironment.StoreAction.LOAD_STORE);
	}
}