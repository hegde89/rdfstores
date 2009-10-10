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
package edu.unika.aifb.facetedSearch.algo.construction.tree.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.jcs.access.exception.CacheException;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.EdgeType;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.ComparatorPool;
import edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.DynamicNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.util.FacetUtils;

/**
 * @author andi
 * 
 */
public class FacetSimpleClusterBuilder implements IBuilder {

	private static Logger s_log = Logger
			.getLogger(FacetSingleLinkageClusterBuilder.class);

	/*
	 * 
	 */
	private SearchSession m_session;
	private SearchSessionCache m_cache;

	/*
	 * 
	 */
	private BuilderHelper m_helper;

	/*
	 * 
	 */
	private ComparatorPool m_compPool;

	public FacetSimpleClusterBuilder(SearchSession session, BuilderHelper helper) {

		m_session = session;
		m_cache = session.getCache();

		m_helper = helper;
		m_compPool = ComparatorPool.getInstance();
	}

	public boolean build(FacetTree tree, StaticNode node) {

		if (node.getFacet().isDataPropertyBased()) {

			if (node instanceof FacetValueNode) {
				return false;
			} else {

				long time1 = System.currentTimeMillis();

				try {

					doClustering(tree, node);

				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (CacheException e) {
					e.printStackTrace();
				}

				long time2 = System.currentTimeMillis();

				s_log.debug("did clustering for node '" + node + "' in "
						+ (time2 - time1) + " ms!");
				return true;
			}

		} else {
			s_log.error("facet " + node.getFacet() + " has invalid facetType!");
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder#clean()
	 */
	public void clean() {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder#close()
	 */
	public void close() {

	}

	private void doClustering(FacetTree tree, StaticNode node)
			throws CacheException, DatabaseException, IOException {

		int datatype = node.getFacet().getDataType() == DataType.NOT_SET
				? FacetEnvironment.DataType.STRING
				: node.getFacet().getDataType();

		List<AbstractSingleFacetValue> lits;

		if (!(node instanceof FacetValueNode) && !(node instanceof DynamicNode)) {

			lits = new ArrayList<AbstractSingleFacetValue>();

			String domain = node.getDomain();
			Iterator<String> subjIter = node.getSubjects().iterator();

			while (subjIter.hasNext()) {

				String subject = subjIter.next();

				Iterator<AbstractSingleFacetValue> objIter = node.getObjects(
						subject).iterator();

				while (objIter.hasNext()) {

					AbstractSingleFacetValue fv = objIter.next();

					Iterator<String> sourcesIter = m_cache.getSources4Object(
							domain, subject).iterator();

					if (sourcesIter.hasNext()) {

						while (sourcesIter.hasNext()) {

							m_cache.addObject2SourceMapping(domain, fv
									.getValue(), sourcesIter.next());

						}
					} else {

						m_cache.addObject2SourceMapping(domain, fv.getValue(),
								subject);
					}

					if (!lits.contains(fv)) {
						lits.add(fv);
					}
				}
			}

			/*
			 * merge sort: O(nlogn)
			 */
			Collections.sort(lits, m_compPool.getComparator(datatype));

		} else {
			lits = ((DynamicNode) node).getLiterals();
		}

		if (lits.size() > FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE) {

			int delta = lits.size()
					/ FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE;

			int i = 0;

			while (i < lits.size()) {

				DynamicNode dynNode = new DynamicNode();
				dynNode.setLeftBorder(lits.get(i).getValue());
				dynNode.setContent(node.getContent());
				dynNode.setDomain(node.getDomain());
				dynNode.setSession(m_session);
				dynNode.setFacet(node.getFacet());

				if ((i + delta) >= lits.size()) {

					dynNode
							.setRightBorder(lits.get(lits.size() - 1)
									.getValue());
					dynNode.setValue(FacetUtils.getLiteralValue(dynNode
							.getLeftBorder())
							+ " - "
							+ FacetUtils.getLiteralValue(dynNode
									.getRightBorder()));
					dynNode.setLiterals(lits.subList(i, lits.size()));

				} else {

					dynNode.setRightBorder(lits.get(i + delta).getValue());
					dynNode.setValue(FacetUtils.getLiteralValue(dynNode.getLeftBorder()) + " - "
							+ FacetUtils.getLiteralValue(dynNode.getRightBorder()));
					dynNode.setLiterals(lits.subList(i, i + delta + 1));
				}

				tree.addVertex(dynNode);

				Edge edge = tree.addEdge(node, dynNode);
				edge.setType(EdgeType.CONTAINS);

				i += delta;
			}

		} else {

			m_helper.insertFacetValues(tree, node, lits);
		}
	}
}
