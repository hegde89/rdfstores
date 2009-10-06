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
package edu.unika.aifb.facetedSearch.facets.model.impl;

import it.unimi.dsi.fastutil.doubles.Double2ObjectOpenHashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.unika.aifb.facetedSearch.facets.model.IFacetFacetValueList;

/**
 * @author andi
 * 
 */
public class FacetFacetValueList implements IFacetFacetValueList, Serializable {

	public enum CleanType {
		VALUES, HISTORY, ALL, SUBFACETS
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -1255846606211212174L;

	/*
	 * 
	 */
	private Double2ObjectOpenHashMap<AbstractFacetValue> m_nodeID2facetValueMap;

	/*
	 * 
	 */
	private Facet m_facet;

	/*
	 * 
	 */
	private List<Facet> m_subfacets;
	private List<AbstractFacetValue> m_facetValueList;

	/*
	 * 
	 */
	private List<AbstractBrowsingObject> m_history;

	public FacetFacetValueList() {
		init();
	}

	public FacetFacetValueList(Facet facet) {
		init();
		setFacet(facet);
	}

	public boolean addBrowsingObject2History(AbstractBrowsingObject obj) {

		if (!m_history.contains(obj)) {
			return m_history.add(obj);
		} else {
			return false;
		}
	}

	public boolean addFacetFacetValueTuple(FacetFacetValueTuple tuple) {

		if (m_facet == null) {
			m_facet = tuple.getFacet();
		}

		if (tuple.getFacet().equals(m_facet)
				&& !m_nodeID2facetValueMap.containsKey(tuple.getFacetValue()
						.getNodeId())) {

			m_nodeID2facetValueMap.put(tuple.getFacetValue().getNodeId(), tuple
					.getFacetValue());
			return m_facetValueList.add(tuple.getFacetValue());

		} else {
			return false;
		}
	}

	public boolean addFacetValue(AbstractFacetValue fv) {

		if (!m_nodeID2facetValueMap.containsKey(fv.getNodeId())) {

			m_nodeID2facetValueMap.put(fv.getNodeId(), fv);
			return m_facetValueList.add(fv);
		} else {
			return false;
		}
	}

	public void addSubFacet(Facet facet) {

		if (!m_subfacets.contains(facet)) {
			m_subfacets.add(facet);
		}
	}

	public void clean(CleanType type) {

		switch (type) {
			case ALL : {

				m_facetValueList.clear();
				m_subfacets.clear();
				m_history.clear();
				break;
			}
			case HISTORY : {

				m_history.clear();
				break;
			}
			case VALUES : {

				m_facetValueList.clear();
				break;
			}
			case SUBFACETS : {

				m_subfacets.clear();
				break;
			}
		}

	}

	public Facet getFacet() {
		return m_facet;
	}

	public Iterator<Facet> getSubFacetIterator() {
		return m_subfacets.iterator();
	}

	public AbstractFacetValue getFacetValue(double nodeID) {
		return m_nodeID2facetValueMap.get(nodeID);
	}

	public List<AbstractFacetValue> getFacetValueList() {
		return m_facetValueList;
	}
	public Iterator<AbstractFacetValue> getFacetValueIterator() {
		return m_facetValueList.iterator();
	}

	public List<AbstractBrowsingObject> getHistory() {
		return m_history;
	}

	public Iterator<AbstractBrowsingObject> getHistoryIterator() {
		return m_history.iterator();
	}

	public List<Facet> getSubfacets() {
		return m_subfacets;
	}

	private void init() {

		m_subfacets = new ArrayList<Facet>();
		m_history = new ArrayList<AbstractBrowsingObject>();
		m_facetValueList = new ArrayList<AbstractFacetValue>();
		m_nodeID2facetValueMap = new Double2ObjectOpenHashMap<AbstractFacetValue>();
	}

	public boolean listContains(AbstractFacetValue fv) {
		return m_nodeID2facetValueMap.containsKey(fv.getNodeId());
	}

	public void setFacet(Facet facet) {
		m_facet = facet;
	}

	public void setFacetValueList(List<AbstractFacetValue> facetValueList) {

		clean(CleanType.VALUES);

		for (AbstractFacetValue fv : facetValueList) {
			addFacetValue(fv);
		}
	}

	public void setHistory(List<AbstractBrowsingObject> history) {
		m_history = history;
	}

	public void setSubfacets(List<Facet> facet2subfacets) {
		m_subfacets = facet2subfacets;
	}
}
