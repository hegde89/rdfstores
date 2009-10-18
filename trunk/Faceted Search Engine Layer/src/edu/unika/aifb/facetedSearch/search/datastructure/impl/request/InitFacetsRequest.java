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
package edu.unika.aifb.facetedSearch.search.datastructure.impl.request;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.query.Query;
import edu.unika.aifb.graphindex.query.StructuredQuery;

/**
 * @author andi
 * 
 */
public class InitFacetsRequest extends Query {

	private StructuredQuery m_sQuery;
	private Table<String> m_res;

	public InitFacetsRequest(String name) {
		super(name);
	}

	public InitFacetsRequest(Table<String> res) {
		super("InitFacetsRequest");
		setRes(res);
	}

	public StructuredQuery getQuery() {
		return m_sQuery;
	}

	public Table<String> getRes() {
		return m_res;
	}

	public void setQuery(StructuredQuery sQuery) {
		m_sQuery = sQuery;
	}

	public void setRes(Table<String> res) {
		m_res = res;
	}

}
