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
package edu.unika.aifb.facetedSearch.algo.ranking.metric.impl;

import java.io.IOException;
import java.util.List;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import edu.unika.aifb.facetedSearch.algo.ranking.metric.IRankingMetric;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.DynamicClusterNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.SingleValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticClusterNode;
import edu.unika.aifb.facetedSearch.index.db.util.FacetDbUtils;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;

/**
 * @author andi
 * @deprecated currently not working.
 */
@Deprecated
public class BrowseAbilityMetric implements IRankingMetric {

	/*
	 * 
	 */
	private SearchSession m_session;

	/*
	 * 
	 */
	private FacetTreeDelegator m_treeDelegator;

	/*
	 * weights
	 */
	private double m_weightRange;
	private double m_weightSource;

	private double m_weightHeightBalanceRange;
	private double m_weightSizeBalanceRange;
	private double m_weightHeightRange;

	private double m_weightSizeBalanceSource;
	private double m_weightRatioSource;
	private double m_weightOverlapSource;

	@SuppressWarnings("unused")
	private double m_weightFirstHop;
	@SuppressWarnings("unused")
	private double m_weightSecondHop;

	/*
	 * 
	 */
	// private EntryBinding<String> m_strgBinding;
	private EntryBinding<Double> m_doubleBinding;

	/*
	 * 
	 */
	private Database m_weightCache;
	private Environment m_env;
//	private DatabaseConfig m_dbConfig;

	public BrowseAbilityMetric(SearchSession session) {

		m_session = session;

		m_treeDelegator = (FacetTreeDelegator) m_session
				.getDelegator(Delegators.TREE);

		init();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.algo.ranking.metric.IRankingMetric#computeScore
	 * (edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode)
	 */
	public void computeScore(StaticClusterNode facetNode) {

		// try {
		//
		// if (m_weightCache == null) {
		//
		// File dirEnv = new File("d:/tmp");
		// m_dbConfig.setAllowCreate(false);
		//
		// EnvironmentConfig envConfig = new EnvironmentConfig();
		// envConfig.setTransactional(false);
		// envConfig.setAllowCreate(false);
		//
		// m_env = new Environment(dirEnv, envConfig);
		//
		// m_weightCache = m_env.openDatabase(null,
		// FacetEnvironment.DatabaseName.RANKING_BROWSE_CACHE,
		// m_dbConfig);
		//
		// // m_strgBinding =
		// // TupleBinding.getPrimitiveBinding(String.class);
		// m_doubleBinding = TupleBinding
		// .getPrimitiveBinding(Double.class);
		// }
		// } catch (DatabaseException e) {
		// e.printStackTrace();
		// }

		if (facetNode.containsDataProperty()
				|| facetNode.containsObjectProperty()
				|| facetNode.containsRdfProperty()) {

			try {

				if (facetNode.containsDataProperty()) {

					String key = m_session.getCurrentQuery().getQGraph()
							.toString()
							+ facetNode.getDomain() + facetNode.getPath();

					if (FacetDbUtils.get(m_weightCache, key, m_doubleBinding) == null) {

						double weight = computeScore4Node(facetNode);
						facetNode.setWeight(weight);

						FacetDbUtils.store(m_weightCache, key, weight,
								m_doubleBinding);

					} else {
						facetNode.setWeight(FacetDbUtils.get(m_weightCache,
								key, m_doubleBinding));
					}
				} else {

					// double finalScore;
					// double firstHopScore;
					// double secondHopScore;

					String key = m_session.getCurrentQuery().getQGraph()
							.toString()
							+ facetNode.getDomain() + facetNode.getPath();

					if (FacetDbUtils.get(m_weightCache, key, m_doubleBinding) == null) {

						double weight = computeScore4Node(facetNode);
						facetNode.setWeight(weight);

						FacetDbUtils.store(m_weightCache, key, weight,
								m_doubleBinding);

					} else {
						facetNode.setWeight(FacetDbUtils.get(m_weightCache,
								key, m_doubleBinding));
					}

					// firstHopScore = computeScore4Node(facetNode);
					// secondHopScore = 0;
					//
					// FacetTree tree =
					// m_treeDelegator.getTree(facetNode.getDomain());
					//
					// Stack<Edge> outEdges = new Stack<Edge>();
					// outEdges.addAll(tree.outgoingEdgesOf(facetNode));
					//
					// int secondHopCount = 0;
					//
					// while (!outEdges.isEmpty()) {
					//
					// Edge outEdge = outEdges.pop();
					// Node tar = outEdge.getTarget();
					//
					// if (outEdge.getType() == EdgeType.SUBPROPERTY_OF) {
					//
					// /*
					// * second hop property
					// */
					//
					// secondHopScore += computeScore4Node((StaticClusterNode)
					// tar);
					// secondHopCount++;
					//
					// } else {
					// outEdges.addAll(tree.outgoingEdgesOf(tar));
					// }
					// }
					//
					// finalScore = m_weightFirstHop * firstHopScore
					// + m_weightSecondHop * (secondHopScore / secondHopCount);
					//
					// facetNode.setWeight(finalScore);

					// facetNode.setWeight(firstHopScore);
				}
			} catch (DatabaseException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try {

			m_weightCache.sync();
			m_weightCache.close();

			m_env.sync();
			m_env.close();

			m_weightCache = null;
			m_env = null;

		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param property
	 *            node
	 * @return browse-ability score; normalized between [0;1]
	 */
	private double computeScore4Node(StaticClusterNode propertyNode) {

		/*
		 * set tree height ...
		 */

		FacetTree tree = m_treeDelegator.getTree(propertyNode.getDomain());

		if (!tree.hasHeight()) {

			List<Node> propNodes = tree.getChildren(tree.getRoot());
			double heightMaxTree = Double.MIN_VALUE;

			for (Node propNode : propNodes) {

				StaticClusterNode rangeTop = (StaticClusterNode) m_treeDelegator
						.getChildren((StaticClusterNode) propNode).get(0);

				List<Node> rangeTopChildren = m_treeDelegator
						.getChildren(rangeTop);

				double heightMaxProp = Double.MIN_VALUE;

				for (Node child : rangeTopChildren) {

					StaticClusterNode statNode = (StaticClusterNode) child;

					double likelihood4unbalance;

					if (child instanceof DynamicClusterNode) {
						likelihood4unbalance = 0.2;
					} else if (child instanceof SingleValueNode) {
						heightMaxProp = 1;
						break;
					} else {
						likelihood4unbalance = 1;
					}

					double height = likelihood4unbalance
							* m_session.getCache().getCountFV(statNode)
							+ (1 - likelihood4unbalance)
							* Math.log(m_session.getCache()
									.getCountFV(statNode));

					if (heightMaxProp < height) {
						heightMaxProp = height;
					}
				}

				if (heightMaxTree < heightMaxProp) {
					heightMaxTree = heightMaxProp;
				}
			}

			tree.setHeight(heightMaxTree);
		}

		StaticClusterNode rangeTop = (StaticClusterNode) m_treeDelegator
				.getChildren(propertyNode).get(0);

		List<Node> rangeTopChildren = m_treeDelegator.getChildren(rangeTop);

		/*
		 * scores
		 */

		double heightBalanceRangeScore;
		double sizeBalanceRangeScore;
		double heightScore;

		double sizeBalanceSourceScore;
		double ratioSourceScore;
		double overlapSourceScore;

		double coveragefactor;

		Double finalScore;

		/*
		 * 
		 */

		double heightMaxRange = Integer.MIN_VALUE;
		double heightMinRange = Integer.MAX_VALUE;

		double sizeMaxRange = Integer.MIN_VALUE;;
		double sizeMinRange = Integer.MAX_VALUE;

		double sizeMaxSource = Integer.MIN_VALUE;;
		double sizeMinSource = Integer.MAX_VALUE;

		for (Node child : rangeTopChildren) {

			StaticClusterNode statNode = (StaticClusterNode) child;

			/*
			 * indicators for range
			 */

			/*
			 * height balance
			 */
			if (child instanceof SingleValueNode) {

				heightMaxRange = 1;
				heightMinRange = 1;

			} else {

				double likelihood4unbalance;

				if (child instanceof DynamicClusterNode) {
					likelihood4unbalance = 0.2;
				} else {
					likelihood4unbalance = 1;
				}

				double height = likelihood4unbalance
						* m_session.getCache().getCountFV(statNode)
						+ (1 - likelihood4unbalance)
						* Math.log(m_session.getCache().getCountFV(statNode));

				if (heightMaxRange < height) {
					heightMaxRange = height;
				}

				if (heightMinRange > height) {
					heightMinRange = height;
				}
			}

			/*
			 * size balance
			 */
			if (sizeMaxRange < m_session.getCache().getCountFV(statNode)) {
				sizeMaxRange = statNode.getCountFV();
			}

			if (sizeMinRange > m_session.getCache().getCountFV(statNode)) {
				sizeMinRange = statNode.getCountFV();
			}

			/*
			 * indicators for source
			 */

			/*
			 * size balance
			 */
			if (sizeMaxSource < m_session.getCache().getCountS(statNode)) {
				sizeMaxSource = statNode.getCountS();
			}

			if (sizeMinSource > m_session.getCache().getCountS(statNode)) {
				sizeMinSource = statNode.getCountS();
			}
		}

		double totalMaxHeight = tree.getHeight();

		if (heightMaxRange == Double.MIN_VALUE) {
			heightMaxRange = 1;
		}

		heightBalanceRangeScore = 1 - (heightMaxRange - heightMinRange)
				/ heightMaxRange;
		sizeBalanceRangeScore = 1 - (sizeMaxRange - sizeMinRange)
				/ sizeMaxRange;

		sizeBalanceSourceScore = 1 - (sizeMaxSource - sizeMinSource)
				/ sizeMaxSource;

		heightScore = 1 - Math.abs(totalMaxHeight - heightMaxRange)
				/ totalMaxHeight;

		/*
		 * indicators for source
		 */

		/*
		 * ratio countS / countFV
		 */

		double countFV = m_session.getCache().getCountFV(rangeTop) == 0
				? 1
				: m_session.getCache().getCountFV(rangeTop);

		double countS_FV = m_session.getCache().getCountS(rangeTop) / countFV;

		if (countS_FV > 1) {
			ratioSourceScore = 1 / countS_FV;
		} else {
			ratioSourceScore = 1;
		}

		/*
		 * coverage score
		 */
		StaticClusterNode root = m_treeDelegator.getTree(rangeTop.getDomain())
				.getRoot();
		coveragefactor = rangeTop.getCountS();

		/*
		 * overlapping score
		 */
		overlapSourceScore = 1
				- (m_session.getCache().getCountSOverlapping(rangeTop) - m_session
						.getCache().getCountS(rangeTop))
				/ m_session.getCache().getCountSOverlapping(rangeTop);

		finalScore = (m_weightRange
				* (heightBalanceRangeScore * m_weightHeightBalanceRange
						+ sizeBalanceRangeScore * m_weightSizeBalanceRange + heightScore
						* m_weightHeightRange) + m_weightSource
				* (m_weightSizeBalanceSource * sizeBalanceSourceScore
						+ m_weightRatioSource * ratioSourceScore + m_weightOverlapSource
						* overlapSourceScore))
				* (coveragefactor / root.getCountS());

		// System.out.println("facet:" + rangeTop.getTopLevelFacet().getUri()
		// + ", weight: " + finalScore);
		//
		// System.out.println(FacetEnvironment.DIVIDER);
		//
		// for (Node node : rangeTopChildren) {
		// System.out.println("node:" + node.getValue() + ", countFV: "
		// + node.getCountFV() + ", countS: " + node.getCountS());
		// }
		//
		// System.out.println(FacetEnvironment.DIVIDER);

		return finalScore.isNaN() ? 0 : finalScore;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#finalize()
	 */
	@Override
	protected void finalize() throws Throwable {

		try {

			m_weightCache.sync();
			m_weightCache.close();
			m_env.close();
			m_env.sync();

			m_weightCache = null;
			m_env = null;

		} catch (DatabaseException e) {
			e.printStackTrace();
		}

		super.finalize();
	}
	private void init() {

		/*
		 * 
		 */
		m_weightRange = 0.8;
		m_weightSource = 0.2;

		m_weightHeightBalanceRange = 0;
		m_weightSizeBalanceRange = 0;
		m_weightHeightRange = 1;

		m_weightSizeBalanceSource = 0.2;
		m_weightRatioSource = 0.6;
		m_weightOverlapSource = 0.2;

		m_weightFirstHop = 0.7;
		m_weightSecondHop = 0.3;

	}
}