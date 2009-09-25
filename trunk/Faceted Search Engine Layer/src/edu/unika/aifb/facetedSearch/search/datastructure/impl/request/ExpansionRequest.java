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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueTuple;

/**
 * @author andi
 * 
 */
public class ExpansionRequest extends AbstractFacetRequest {

	private List<FacetFacetValueTuple> m_tuplesRemoved;

	public ExpansionRequest() {
		super("expansionRequest");
		init();
	}

	public ExpansionRequest(String name) {
		super(name);
		init();
	}

	public boolean addTuple(FacetFacetValueTuple tuple) {
		return m_tuplesRemoved.add(tuple);
	}

	public List<FacetFacetValueTuple> getTuples() {
		return m_tuplesRemoved;
	}

	private void init() {
		setTuples(new ArrayList<FacetFacetValueTuple>());
	}

	public Iterator<FacetFacetValueTuple> iterator() {
		return m_tuplesRemoved.iterator();
	}

	public boolean removeTuple(FacetFacetValueTuple tuple) {
		return m_tuplesRemoved.remove(tuple);
	}

	public void setTuples(List<FacetFacetValueTuple> tuplesRemoved) {
		m_tuplesRemoved = tuplesRemoved;
	}
}
