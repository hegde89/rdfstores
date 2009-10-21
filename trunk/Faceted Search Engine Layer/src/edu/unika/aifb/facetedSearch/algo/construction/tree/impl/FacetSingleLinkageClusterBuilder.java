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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.jcs.access.exception.CacheException;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.EdgeType;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.IDistanceMetric;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.distance.ClusterDistance;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.distance.DistanceComparator;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.distance.PositionComparator;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.ComparatorPool;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.metric.DistanceMetricPool;
import edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.DynamicNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class FacetSingleLinkageClusterBuilder implements IBuilder {

	private static Logger s_log = Logger
			.getLogger(FacetSingleLinkageClusterBuilder.class);

	/*
	 * 
	 */
	private SearchSession m_session;

	/*
	 * 
	 */
	private BuilderHelper m_helper;

	/*
	 * 
	 */
	private ComparatorPool m_compPool;

	public FacetSingleLinkageClusterBuilder(SearchSession session,
			BuilderHelper helper) {

		m_session = session;
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

	@SuppressWarnings("unchecked")
	private void doClustering(FacetTree tree, StaticNode epNode)
			throws CacheException, DatabaseException, IOException {

		int datatype = epNode.getFacet().getDataType() == DataType.NOT_SET
				? FacetEnvironment.DataType.STRING
				: epNode.getFacet().getDataType();

		IDistanceMetric metric = DistanceMetricPool.getMetric(datatype);
		List<AbstractSingleFacetValue> lits = new ArrayList<AbstractSingleFacetValue>();
		lits.addAll(epNode.getObjects());

		/*
		 * merge sort: O(nlogn)
		 */
		Collections.sort(lits, m_compPool.getComparator(datatype));

		if (lits.size() > FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE) {

			// construct and sort distances for node

			/*
			 * each insert has O(logn)
			 */
			PriorityQueue<ClusterDistance> distanceQueue = new PriorityQueue<ClusterDistance>(
					FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE - 1,
					new DistanceComparator());

			Iterator<AbstractSingleFacetValue> litIter = lits.iterator();

			int current_leftCountS = 0;
			int current_leftCountFV = 0;

			ArrayList<ClusterDistance> current_leftDistances = new ArrayList<ClusterDistance>();
			HashSet<String> current_leftSources = new HashSet<String>();

			while (litIter.hasNext()) {

				Literal current_left = (Literal) litIter.next();
				String ext = current_left.getSourceExt();

				if (litIter.hasNext()) {

					Literal current_right = (Literal) litIter.next();
					ClusterDistance clusterDistance;

					if ((clusterDistance = m_session.getCache().getDistance(
							current_left.getValue(), current_right.getValue(),
							ext)) == null) {

						current_leftSources.addAll(m_session.getCache()
								.getSources4Object(current_left.getDomain(),
										current_left.getValue()));
						current_leftCountS = current_leftSources.size();
						current_leftCountFV += 1;

						BigDecimal distanceValue = metric.getDistance(
								current_left.getParsedLiteral(), current_right
										.getParsedLiteral());

						clusterDistance = new ClusterDistance(current_left
								.getValue(), current_right.getValue());

						clusterDistance.setLeftDistances(current_leftDistances);
						clusterDistance.setValue(distanceValue);
						clusterDistance.setLeftCountFV(current_leftCountFV);
						clusterDistance.setLeftCountS(current_leftCountS);

						m_session.getCache().addDistance(
								current_left.getValue(),
								current_right.getValue(), ext, clusterDistance);

					}

					boolean success = distanceQueue.offer(clusterDistance);

					if (!success) {

						s_log.error("could not insert distance '"
								+ clusterDistance + "'!");

					}

					current_leftDistances.add(clusterDistance);
				}
			}

			/*
			 * O(klogk)
			 */

			PriorityQueue<ClusterDistance> posQueue = new PriorityQueue<ClusterDistance>(
					FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE - 1,
					new PositionComparator());

			for (int i = 0; i < FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE - 1; i++) {

				ClusterDistance clusterDistance = distanceQueue.poll();
				posQueue.offer(clusterDistance);

			}

			Edge edge;
			DynamicNode dynNode;
			ClusterDistance firstDis;
			ClusterDistance secondDis;

			if (!posQueue.isEmpty()) {

				// first distance
				firstDis = posQueue.poll();

				dynNode = new DynamicNode();
				dynNode.setDomain(epNode.getDomain());
				dynNode.setSession(m_session);
				dynNode.setCountS(firstDis.getLeftCountS());
				dynNode.setCountFV(firstDis.getLeftCountFV());
				dynNode.setLeftBorder(lits.get(0).getValue());
				dynNode.setRightBorder(firstDis.getLeftBorder());
				dynNode.setLiterals(lits.subList(0, lits.indexOf(firstDis
						.getLeftBorder())));

				tree.addVertex(dynNode);
				edge = tree.addEdge(epNode, dynNode);
				edge.setType(EdgeType.CONTAINS);

				while (!posQueue.isEmpty()) {

					secondDis = posQueue.poll();

					dynNode = new DynamicNode();
					dynNode.setDomain(epNode.getDomain());
					dynNode.setSession(m_session);
					dynNode.setCountS(secondDis.getLeftCountS()
							- firstDis.getLeftCountS());
					dynNode.setCountFV(secondDis.getLeftCountFV()
							- firstDis.getLeftCountFV());
					dynNode.setLeftBorder(firstDis.getRightBorder());
					dynNode.setRightBorder(secondDis.getLeftBorder());
					dynNode.setLiterals(lits.subList(lits.indexOf(firstDis
							.getRightBorder()), lits.indexOf(secondDis
							.getLeftBorder())));

					tree.addVertex(dynNode);
					edge = tree.addEdge(epNode, dynNode);
					edge.setType(EdgeType.CONTAINS);

					if (posQueue.isEmpty()) {

						dynNode = new DynamicNode();
						dynNode.setDomain(epNode.getDomain());
						dynNode.setSession(m_session);
						dynNode.setCountS(epNode.getCountS()
								- secondDis.getLeftCountS());
						dynNode.setCountFV(epNode.getCountFV()
								- secondDis.getLeftCountFV());
						dynNode.setLeftBorder(secondDis.getLeftBorder());
						dynNode.setRightBorder(lits.get(lits.size() - 1)
								.getValue());
						dynNode.setLiterals(lits.subList(lits.indexOf(secondDis
								.getRightBorder()), lits.size() - 1));

						tree.addVertex(dynNode);
						edge = tree.addEdge(epNode, dynNode);
						edge.setType(EdgeType.CONTAINS);

					} else {
						firstDis = secondDis;
					}
				}
			}

		} else {

			m_helper.insertFacetValues(tree, epNode, lits);
		}
	}
}