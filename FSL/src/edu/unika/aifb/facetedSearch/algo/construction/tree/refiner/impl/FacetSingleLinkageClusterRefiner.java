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
package edu.unika.aifb.facetedSearch.algo.construction.tree.refiner.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Stack;

import org.apache.jcs.access.exception.CacheException;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;
import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.EdgeType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeType;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.IDistanceMetric;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.distance.ClusterDistance;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.distance.DistanceComparator;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.distance.PositionComparator;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.ComparatorPool;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.metric.DistanceMetricPool;
import edu.unika.aifb.facetedSearch.algo.construction.tree.refiner.IRefiner;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.DynamicClusterNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.SingleValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticClusterNode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */

public class FacetSingleLinkageClusterRefiner implements IRefiner {

	private static Logger s_log = Logger
			.getLogger(FacetSingleLinkageClusterRefiner.class);

	/*
	 * 
	 */
	private SearchSession m_session;

	/*
	 * 
	 */
	private ComparatorPool m_compPool;

	/*
	 * 
	 */
	private HashSet<String> m_parsedFacetValues;
	private HashSet<String> m_parsedSources;

	public FacetSingleLinkageClusterRefiner(SearchSession session) {

		m_session = session;
		init();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.algo.construction.tree.refiner.IRefiner#
	 * clean()
	 */
	public void clean() {
		m_parsedFacetValues.clear();
		m_parsedSources.clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.algo.construction.tree.refiner.IRefiner#
	 * close()
	 */
	public void close() {
		clean();
	}

	@SuppressWarnings("unchecked")
	private void doClustering(FacetTree tree, StaticClusterNode node)
			throws CacheException, DatabaseException, IOException {

		int datatype = node.getCurrentFacet().getDataType() == DataType.NOT_SET
				? FacetEnvironment.DataType.STRING
				: node.getCurrentFacet().getDataType();

		List<AbstractSingleFacetValue> lits;

		if (!(node instanceof DynamicClusterNode)) {

			int countS = 0;
			int countSOverlapping = 0;

			lits = new ArrayList<AbstractSingleFacetValue>();

			String domain = node.getDomain();
			Iterator<String> subjIter = m_session.getCache().getSubjects4Node(
					node).iterator();

			while (subjIter.hasNext()) {

				String subject = subjIter.next();

				Iterator<AbstractSingleFacetValue> objIter = m_session
						.getCache().getObjects4StaticNode(node, subject)
						.iterator();

				while (objIter.hasNext()) {

					AbstractSingleFacetValue fv = objIter.next();

					Iterator<String> sourcesIter = m_session.getCache()
							.getSources4Object(domain, subject).iterator();

					if (sourcesIter.hasNext()) {

						while (sourcesIter.hasNext()) {

							String thisSource = sourcesIter.next();

							if (!m_parsedSources.contains(thisSource)) {

								m_session.getCache().addObject2SourceMapping(
										domain, fv.getValue(), thisSource);

								m_parsedSources.add(thisSource);
								countS++;
							}

							countSOverlapping++;
						}
					} else {

						if (!m_parsedSources.contains(subject)) {

							m_session.getCache().addObject2SourceMapping(
									domain, fv.getValue(), subject);

							m_parsedSources.add(subject);
							countS++;
						}

						countSOverlapping++;
					}

					if (!lits.contains(fv)) {
						lits.add(fv);
					}
				}
			}

			node.setCountFV(lits.size());
			node.setCountS(countS);
			node.setCountSOverlapping(countSOverlapping);

			/*
			 * merge sort: O(nlogn)
			 */
			Collections.sort(lits, m_compPool.getComparator(datatype));

		} else {

			lits = m_session.getCache().getLiterals4DynNode(
					(DynamicClusterNode) node);

			node.setCountFV(lits.size());
		}

		if (lits.size() > FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE) {

			IDistanceMetric metric = DistanceMetricPool.getMetric(datatype);

			/*
			 * each insert has O(logn)
			 */
			PriorityQueue<ClusterDistance> distanceQueue = new PriorityQueue<ClusterDistance>(
					FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE - 1,
					new DistanceComparator());

			Stack<AbstractSingleFacetValue> litStack = new Stack<AbstractSingleFacetValue>();
			litStack.addAll(lits);

			int current_leftIdx = 0;
			int current_leftCountS = 0;
			int current_leftCountFV = 0;

			HashSet<String> current_leftSources = new HashSet<String>();

			while (!litStack.isEmpty()) {

				Literal current_left = (Literal) litStack.pop();

				if (!litStack.isEmpty()) {

					Literal current_right = (Literal) litStack.peek();
					ClusterDistance clusterDistance;

					if ((clusterDistance = m_session.getCache().getDistance(
							current_left.getValue(), current_right.getValue())) == null) {

						current_leftSources.addAll(m_session.getCache()
								.getSources4Object(current_left.getDomain(),
										current_left.getValue()));

						current_leftCountS = current_leftSources.size();
						current_leftCountFV++;

						BigDecimal distanceValue = metric.getDistance(
								current_left.getParsedLiteral(), current_right
										.getParsedLiteral());

						clusterDistance = new ClusterDistance(current_left
								.getValue(), current_right.getValue());

						clusterDistance.setLeftIdx(current_leftIdx);
						clusterDistance.setRightIdx(current_leftIdx + 1);

						current_leftIdx++;

						clusterDistance.setValue(distanceValue);
						clusterDistance.setLeftCountFV(current_leftCountFV);
						clusterDistance.setLeftCountS(current_leftCountS);

						m_session.getCache().addDistance(
								current_left.getValue(),
								current_right.getValue(), clusterDistance);

					}

					boolean success = distanceQueue.offer(clusterDistance);

					if (!success) {

						s_log.error("could not insert distance '"
								+ clusterDistance + "'!");

					}
				}
			}

			current_leftSources.clear();

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
			DynamicClusterNode dynNode;
			ClusterDistance firstDis;
			ClusterDistance secondDis;

			ArrayList<DynamicClusterNode> newNodes = new ArrayList<DynamicClusterNode>();

			if (!posQueue.isEmpty()) {

				// first distance
				firstDis = posQueue.poll();

				dynNode = new DynamicClusterNode();
				dynNode.setDomain(node.getDomain());
				dynNode.setCountS(firstDis.getLeftCountS());
				dynNode.setCountFV(firstDis.getLeftCountFV());
				dynNode.setLeftBorder(lits.get(0).getValue());
				dynNode.setRightBorder(firstDis.getLeftBorder());

				m_session.getCache().storeLiterals(dynNode,
						lits.subList(0, firstDis.getLeftIdx()));

				tree.addVertex(dynNode);
				newNodes.add(dynNode);

				edge = tree.addEdge(node, dynNode);
				edge.setType(EdgeType.CONTAINS);

				while (!posQueue.isEmpty()) {

					secondDis = posQueue.poll();

					dynNode = new DynamicClusterNode();
					dynNode.setDomain(node.getDomain());
					dynNode.setCountS(secondDis.getLeftCountS()
							- firstDis.getLeftCountS());
					dynNode.setCountFV(secondDis.getLeftCountFV()
							- firstDis.getLeftCountFV());
					dynNode.setLeftBorder(firstDis.getRightBorder());
					dynNode.setRightBorder(secondDis.getLeftBorder());

					m_session.getCache().storeLiterals(
							dynNode,
							lits.subList(firstDis.getRightIdx(), secondDis
									.getLeftIdx()));

					tree.addVertex(dynNode);
					newNodes.add(dynNode);

					edge = tree.addEdge(node, dynNode);
					edge.setType(EdgeType.CONTAINS);

					if (posQueue.isEmpty()) {

						dynNode = new DynamicClusterNode();
						dynNode.setDomain(node.getDomain());
						dynNode.setCountS(node.getCountS()
								- secondDis.getLeftCountS());
						dynNode.setCountFV(node.getCountFV()
								- secondDis.getLeftCountFV());
						dynNode.setLeftBorder(secondDis.getLeftBorder());
						dynNode.setRightBorder(lits.get(lits.size() - 1)
								.getValue());

						m_session.getCache().storeLiterals(
								dynNode,
								lits.subList(secondDis.getRightIdx(), lits
										.size() - 1));

						tree.addVertex(dynNode);
						newNodes.add(dynNode);

						edge = tree.addEdge(node, dynNode);
						edge.setType(EdgeType.CONTAINS);

					} else {
						firstDis = secondDis;
					}
				}

				if (FacetedSearchLayerConfig.getRankingMetric().equals(
						FacetedSearchLayerConfig.Value.Ranking.BROWSE_ABILITY)) {

					for (DynamicClusterNode newDynNode : newNodes) {

						ClusterDistance maxDis = null;

						lits = m_session.getCache().getLiterals4DynNode(
								newDynNode);

						Iterator<AbstractSingleFacetValue> litIter = m_session
								.getCache().getLiterals4DynNode(newDynNode)
								.iterator();

						while (litIter.hasNext()) {

							Literal current_left = (Literal) litIter.next();

							if (litIter.hasNext()) {

								Literal current_right = (Literal) litIter
										.next();

								ClusterDistance clusterDistance = m_session
										.getCache().getDistance(
												current_left.getValue(),
												current_right.getValue());

								if ((clusterDistance != null)
										&& ((maxDis == null) || (maxDis
												.getValue().compareTo(
														clusterDistance
																.getValue()) < 0))) {
									maxDis = clusterDistance;
								}
							}
						}

						newDynNode.setHeightIndicator(lits.indexOf(maxDis
								.getLeftBorder())
								/ lits.size());
					}
				}
			} else {

				int i = 0;

				while (i < lits.size()) {

					if (!m_parsedFacetValues.contains(lits.get(i).getValue())) {

						SingleValueNode fvNode = new SingleValueNode(
								((Literal) lits.get(i)).getLiteralValue());
						fvNode.setContent(node.getContent());
						fvNode.setCurrentFacet(node.getCurrentFacet());
						fvNode.setTopLevelFacet(node.getTopLevelFacet());
						fvNode.setType(NodeType.LEAVE);
						fvNode.setDomain(node.getDomain());

						tree.addVertex(fvNode);

						edge = tree.addEdge(node, fvNode);
						edge.setType(EdgeType.CONTAINS);

						m_parsedFacetValues.add(lits.get(i).getValue());
					}

					i++;
				}

				m_parsedFacetValues.clear();
			}
		}
	}

	private void init() {

		m_compPool = ComparatorPool.getInstance();
		m_parsedFacetValues = new HashSet<String>();
		m_parsedSources = new HashSet<String>();
	}

	public boolean refine(FacetTree tree, StaticClusterNode node) {

		if (node.getCurrentFacet().isDataPropertyBased()) {

			if (node instanceof SingleValueNode) {
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
			s_log.error("facet " + node.getCurrentFacet()
					+ " has invalid facetType!");
			return false;
		}
	}
}