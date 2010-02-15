/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer project. 
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
package edu.unika.aifb.facetedSearch.connection;

import java.io.IOException;
import java.security.InvalidParameterException;

import edu.unika.aifb.facetedSearch.exception.MissingParameterException;
import edu.unika.aifb.facetedSearch.store.IStore;
import edu.unika.aifb.graphindex.storage.StorageException;

public interface IConnection {

	public void close();

	public IStore loadStore() throws MissingParameterException,
			InvalidParameterException, IOException, StorageException,
			InterruptedException;

	public IStore createStore() throws MissingParameterException,
			InvalidParameterException, IOException, StorageException,
			InterruptedException;

	public IStore loadOrCreateStore() throws MissingParameterException,
			InvalidParameterException, IOException, StorageException,
			InterruptedException;

}
