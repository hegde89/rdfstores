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
//import com.sleepycat.je.DatabaseConfig;
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
 * Approximates Merit-based ranking see 'Automatic Construction of Multifaceted
 * Browsing Interfaces' or Indistinguishable ranking see 'Minimum-Effort Driven
 * Dynamic Faceted Search in Structured Databases'
 * 
 * @author andi
 * @deprecated currently not working.
 */
public class MinimumEffortMetric implements IRankingMetric {

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
	private double m_weightHeightRange;
	private double m_weightOverlapSource;

	/*
	 * 
	 */
	private EntryBinding<Double> m_doubleBinding;

	/*
	 * 
	 */
	private Database m_weightCache;
	private Environment m_env;
//	private DatabaseConfig m_dbConfig;

	public MinimumEffortMetric(SearchSession session) {

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

//		try {
//
//			if (m_weightCache == null) {
//
//				File dirEnv = new File("d:/tmp2");
//				m_dbConfig.setAllowCreate(false);
//
//				EnvironmentConfig envConfig = new EnvironmentConfig();
//				envConfig.setTransactional(false);
//				envConfig.setAllowCreate(false);
//
//				m_env = new Environment(dirEnv, envConfig);
//
//				m_weightCache = m_env.openDatabase(null,
//						FacetEnvironment.DatabaseName.RANKING_BROWSE_CACHE,
//						m_dbConfig);
//
//				// m_strgBinding =
//				// TupleBinding.getPrimitiveBinding(String.class);
//				m_doubleBinding = TupleBinding
//						.getPrimitiveBinding(Double.class);
//			}
//		} catch (DatabaseException e) {
//			e.printStackTrace();
//		}

		try {

			if (facetNode.containsDataProperty()
					|| facetNode.containsObjectProperty()
					|| facetNode.containsRdfProperty()) {

				String key = m_session.getCurrentQuery().getQGraph().toString()
						+ facetNode.getDomain() + facetNode.getPath();

				if (FacetDbUtils.get(m_weightCache, key, m_doubleBinding) == null) {

					double weight = computeScore4Node(facetNode);
					facetNode.setWeight(weight);

					FacetDbUtils.store(m_weightCache, key, weight,
							m_doubleBinding);

				} else {
					facetNode.setWeight(FacetDbUtils.get(m_weightCache, key,
							m_doubleBinding));
				}
			}
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
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
						likelihood4unbalance = 0;
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

		double heightScore;
		double overlapSourceScore;
		// double ratioSourceScore;
		double coveragefactor;
		Double finalScore;
		double totalMaxHeight;

		/*
		 * 
		 */
		double heightMaxRange = Double.MIN_VALUE;

		for (Node child : rangeTopChildren) {

			if (child instanceof SingleValueNode) {
				heightMaxRange = 1;
				break;
			} else {

				double likelihood4unbalance;

				if (child instanceof DynamicClusterNode) {
					likelihood4unbalance = 0.2;
				} else {
					likelihood4unbalance = 1;
				}

				double height = likelihood4unbalance
						* m_session.getCache().getCountFV(
								(StaticClusterNode) child)
						+ (1 - likelihood4unbalance)
						* Math.log(m_session.getCache().getCountFV(
								(StaticClusterNode) child));

				if (heightMaxRange < height) {
					heightMaxRange = height;
				}
			}
		}

		totalMaxHeight = tree.getHeight();

		if (heightMaxRange == Double.MIN_VALUE) {
			heightMaxRange = totalMaxHeight;
		}

		heightScore = (totalMaxHeight - heightMaxRange) / totalMaxHeight;

		/*
		 * coverage score
		 */
		coveragefactor = m_session.getCache().getCountS(rangeTop);

		// /*
		// * ratio countS / countFV
		// */
		// double countS_FV = (m_session.getCache().getCountS(rangeTop) /
		// m_session
		// .getCache().getCountFV(rangeTop));
		//
		// if (countS_FV > 1) {
		// ratioSourceScore = 1 / countS_FV;
		// } else {
		// ratioSourceScore = 1;
		// }

		/*
		 * overlapping score
		 */
		overlapSourceScore = 1
				- (m_session.getCache().getCountSOverlapping(rangeTop) - m_session
						.getCache().getCountS(rangeTop))
				/ m_session.getCache().getCountSOverlapping(rangeTop);

		finalScore = ((heightScore * m_weightHeightRange + m_weightOverlapSource // 0
				// *
				// ratioSourceScore
				* overlapSourceScore))
				* (0.5 * coveragefactor / tree.getRoot().getCountS());

		// System.out.println("facet:" + rangeTop.getTopLevelFacet().getUri()
		// + ", weight: " + finalScore);
		//
		// System.out.println(FacetEnvironment.DIVIDER);
		//
		// for (Node node : rangeTopChildren) {
		// System.out.println("node:" + node.getValue() + ", countFV: "
		// + node.getCountFV());
		// }
		//
		// System.out.println(FacetEnvironment.DIVIDER);

		return finalScore.isNaN() ? 0 : finalScore;
	}

	private void init() {

		/*
		 * 
		 */
		m_weightHeightRange = 0.9;
		m_weightOverlapSource = 0.1;
	}
}