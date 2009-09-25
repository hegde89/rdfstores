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
package edu.unika.aifb.facetedSearch.search.datastructure.impl.query;

import java.util.Collection;
import java.util.List;

import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueTuple;
import edu.unika.aifb.graphindex.query.Query;

/**
 * @author andi
 * 
 */
public class FacetedQuery {

	private double m_id;
	private Query m_initialQuery;
	private List<FacetFacetValueTuple> m_tuples;

	public FacetedQuery(Query query) {

		m_initialQuery = query;
		updateId();
	}

	public boolean addFacetFacetValueTuple(FacetFacetValueTuple tuple) {

		boolean success = m_tuples.add(tuple);
		updateId();

		return success;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (obj instanceof FacetedQuery) {

			// boolean facetsEqual = true;
			// FacetedQuery otherQuery = (FacetedQuery) obj;
			//
			// if (otherQuery.getTuples().size() == m_tuples.size()) {
			//
			// for (FacetFacetValueTuple otherTuple : otherQuery.getTuples()) {
			//
			// if (!m_tuples.contains(otherTuple)) {
			// facetsEqual = false;
			// break;
			// }
			// }
			// } else {
			// facetsEqual = false;
			// }
			//
			// return m_initialQuery.equals(otherQuery.getInitialQuery())
			// && facetsEqual;

			return m_id == ((FacetedQuery) obj).getId();

		} else {
			return false;
		}

	}

	public double getId() {
		return m_id;
	}
	public Query getInitialQuery() {
		return m_initialQuery;
	}
	public List<FacetFacetValueTuple> getTuples() {
		return m_tuples;
	}

	public boolean removeAllFacetFacetValueTuples(
			Collection<FacetFacetValueTuple> tuples) {

		boolean success = m_tuples.removeAll(tuples);
		updateId();

		return success;
	}

	public boolean removeFacetFacetValueTuple(FacetFacetValueTuple tuple) {

		boolean success = m_tuples.remove(tuple);
		updateId();

		return success;
	}

	public void setTuples(List<FacetFacetValueTuple> tuples) {

		m_tuples = tuples;
		updateId();
	}

	private void updateId() {

		double sum = m_initialQuery.hashCode();

		for (FacetFacetValueTuple tuple : m_tuples) {
			sum += tuple.getId();
		}

		m_id = sum;
	}
}
