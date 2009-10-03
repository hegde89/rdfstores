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
package edu.unika.aifb.facetedSearch.facets.tree.model.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Set;

import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.tree.model.IStaticNode;
import edu.unika.aifb.facetedSearch.index.db.binding.AbstractSingleFacetValueBinding;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;

/**
 * @author andi
 * 
 */
public class StaticNode extends Node implements IStaticNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5743733958338988213L;
	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(StaticNode.class);

	/*
	 * berkeley db ...
	 */

	private SearchSessionCache m_cache;
	private StoredMap<AbstractSingleFacetValue, Integer> m_countFVMap;
	private StoredMap<String, Integer> m_countSMap;

	/*
	 * 
	 */

	private int m_countFV;
	private int m_countS;
	private int m_height;
	private int m_depth;
	private int m_size;

	public StaticNode() {
		super();
		init();
	}
	
	public StaticNode(String value) {
		super(value);
		init();
	}

	public StaticNode(String value, int type, int content) {
		super(value, type, content);
		init();
	}

	public void addSortedObjects(
			Collection<AbstractSingleFacetValue> abstractFacetValues,
			String source) {

		for (AbstractSingleFacetValue fv : abstractFacetValues) {

			/*
			 * add literal to sorted literal list ...
			 */

			if (fv instanceof Literal) {

				try {

					m_cache.addLiteral4Node(this, (Literal) fv);

				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				} catch (DatabaseException e) {
					e.printStackTrace();
				}
			}

			/*
			 * update countFV
			 */

			Integer countFV;

			if ((countFV = m_countFVMap.get(fv)) == null) {
				countFV = 0;
			}

			countFV++;
			m_countFVMap.put(fv, countFV);

			/*
			 * update source for facet value
			 */

			try {
				m_cache.addSource4FacetValue(fv, source);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void addSourceIndivdiual(String ind) {

		Integer countS;

		if ((countS = m_countSMap.get(ind)) == null) {
			countS = 0;
		}

		countS++;
		m_countSMap.put(ind, countS);
	}

	public void addUnsortedObjects(
			Collection<AbstractSingleFacetValue> abstractFacetValues,
			String source) {

		for (AbstractSingleFacetValue fv : abstractFacetValues) {

			/*
			 * update countFV
			 */

			Integer countFV;

			if ((countFV = m_countFVMap.get(fv)) == null) {
				countFV = 0;
			}

			countFV++;
			m_countFVMap.put(fv, countFV);

			/*
			 * update source for facet value
			 */

			try {
				m_cache.addSource4FacetValue(fv, source);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}
	}

	public int getCountFV() {
		return m_countFV;
	}

	public int getCountS() {
		return m_countS;
	}

	// public int getCountS4Object(String object) {
	//
	// try {
	// return m_cache.getCountS4Object(object);
	// } catch (DatabaseException e) {
	// e.printStackTrace();
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	//
	// return 0;
	// }
	//
	// public int getCountS4Objects(Collection<String> objects) {
	//
	// try {
	// return m_cache.getCountS4Objects(objects);
	// } catch (DatabaseException e) {
	// e.printStackTrace();
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	//
	// return 0;
	// }

	public int getDepth() {
		return m_depth;
	}

	public int getHeight() {
		return m_height;
	}

	// public String getName() {
	// return m_name;
	// }

	public Set<AbstractSingleFacetValue> getObjects() {

		return m_countFVMap.keySet();
	}

	public int getSize() {
		return m_size;
	}

	public Collection<Literal> getSortedLiterals() {
		return m_cache.getLiterals4Node(this);
	}

	public Set<String> getSourceIndivdiuals() throws DatabaseException,
			IOException {

		return m_countSMap.keySet();
	}

	private void init() {

		m_countFV = -1;
		m_countS = -1;
		m_height = -1;
		m_size = -1;

	}

	private void initStoredMaps() {

		/*
		 * bindings ...
		 */

		AbstractSingleFacetValueBinding fvBind = new AbstractSingleFacetValueBinding();

		TupleBinding<String> strgBind = TupleBinding
				.getPrimitiveBinding(String.class);

		TupleBinding<Integer> intBind = TupleBinding
				.getPrimitiveBinding(Integer.class);

		/*
		 * maps ...
		 */

		m_countFVMap = new StoredMap<AbstractSingleFacetValue, Integer>(m_cache
				.getDB(FacetEnvironment.DatabaseName.FCO_CACHE), fvBind,
				intBind, true);

		m_countSMap = new StoredMap<String, Integer>(m_cache
				.getDB(FacetEnvironment.DatabaseName.FCS_CACHE), strgBind,
				intBind, true);

	}

	public void setCache(SearchSessionCache cache) {
		m_cache = cache;
		initStoredMaps();
	}

	public void setCountFV(int countFV) {
		m_countFV = countFV;
	}

	public void setCountS(int countS) {
		m_countS = countS;
	}

	public void setDepth(int depth) {
		m_depth = depth;
	}

	public void setHeight(int height) {
		m_height = height;
	}

	// public void setLiteralCounts(HashMap<String, HashSet<Integer>>
	// literalCounts) {
	// m_LiteralSources = literalCounts;
	// }

	// public void setName(String name) {
	// m_name = name;
	// }

	public void setSize(int size) {
		m_size = size;
	}

	// public void setSortedLiterals(List<String> sortedLiterals) {
	// m_sortedLiterals = sortedLiterals;
	// }
}
