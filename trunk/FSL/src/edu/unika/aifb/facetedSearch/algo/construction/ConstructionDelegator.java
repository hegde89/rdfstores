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
package edu.unika.aifb.facetedSearch.algo.construction;

import java.io.IOException;

import org.apache.jcs.access.exception.CacheException;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.Delegator;
import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;
import edu.unika.aifb.facetedSearch.FacetEnvironment.FacetType;
import edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder;
import edu.unika.aifb.facetedSearch.algo.construction.tree.impl.BuilderHelper;
import edu.unika.aifb.facetedSearch.algo.construction.tree.impl.FacetTreeBuilder;
import edu.unika.aifb.facetedSearch.algo.construction.tree.refiner.IRefiner;
import edu.unika.aifb.facetedSearch.algo.construction.tree.refiner.impl.FacetSimpleClusterRefiner;
import edu.unika.aifb.facetedSearch.algo.construction.tree.refiner.impl.FacetSingleLinkageClusterRefiner;
import edu.unika.aifb.facetedSearch.algo.construction.tree.refiner.impl.FacetSubTreeRefiner;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticClusterNode;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.util.FacetUtils;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class ConstructionDelegator extends Delegator {

	private static Logger s_log = Logger.getLogger(ResultPage.class);

	/*
	 * 
	 */
	private SearchSession m_session;

	/*
	 * 
	 */

	private FacetTreeDelegator m_treeDelegator;

	/*
	 * delegates
	 */
	private BuilderHelper m_helper;
	private IBuilder m_treeBuilderDelegate;
	private IRefiner m_subTreeRefinerDelegate;
	private IRefiner m_clusterRefinerDelegate;

	public ConstructionDelegator(SearchSession session) {

		m_session = session;

		m_helper = new BuilderHelper(m_session);
		m_treeBuilderDelegate = new FacetTreeBuilder(m_session, m_helper);
		m_subTreeRefinerDelegate = new FacetSubTreeRefiner(m_session, m_helper);

		if (FacetedSearchLayerConfig.getClusterer().equals(
				FacetedSearchLayerConfig.Value.LiteralClusterer.SIMPLE)) {

			m_clusterRefinerDelegate = new FacetSimpleClusterRefiner(m_session,
					m_helper);

		} else {

			m_clusterRefinerDelegate = new FacetSingleLinkageClusterRefiner(
					m_session);
		}
	}

	@Override
	public void clean() {

		m_treeBuilderDelegate.clean();
		m_subTreeRefinerDelegate.clean();
		m_clusterRefinerDelegate.clean();
	}

	@Override
	public void close() {

		m_treeBuilderDelegate.close();
		m_subTreeRefinerDelegate.close();
		m_clusterRefinerDelegate.close();
	}

	public boolean construct(Table<String> results)
			throws EnvironmentLockedException, IOException, DatabaseException,
			StorageException, CacheException {

		s_log.debug("start facet construction for new result set '"
				+ results.toString() + "'");

		boolean success = true;
		m_treeDelegator.clean();

		for (String colName : results.getColumnNames()) {

			if (FacetUtils.isVariable(colName)) {

				s_log.debug("start building facet tree for column '" + colName
						+ "'");

				m_treeBuilderDelegate.clean();

				success = ((FacetTreeBuilder) m_treeBuilderDelegate).build(
						results, results.getColumn(colName));

				if (!success) {

					s_log.error("construction of facet tree for column '"
							+ colName + "' failed!");

				} else {
					s_log.debug("finished facet tree for column '" + colName
							+ "'!");
				}

			} else {
				s_log.debug("skipped column '" + colName
						+ "' since it's no variable!");
			}
		}

		return success;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.Delegator#isOpen()
	 */
	@Override
	public boolean isOpen() {
		return true;
	}

	public boolean refine(FacetTree tree, StaticClusterNode node) {

		if (node.getCurrentFacet().getType() == FacetType.DATAPROPERTY_BASED) {

			return refineCluster(tree, node);

		} else if (node.getCurrentFacet().getType() != FacetType.DATAPROPERTY_BASED) {

			return refineSubTree(tree, node);

		} else {

			s_log.error("should not be here: node '" + node + "' and tree '"
					+ tree + "'");
			return false;
		}
	}

	public boolean refineCluster(FacetTree tree, StaticClusterNode node) {

		boolean success;

		if (node.getCurrentFacet().getType() == FacetType.DATAPROPERTY_BASED) {

			try {

				m_clusterRefinerDelegate.clean();
				success = m_clusterRefinerDelegate.refine(tree, node);

			} catch (DatabaseException e) {

				e.printStackTrace();
				success = false;

			} catch (IOException e) {

				e.printStackTrace();
				success = false;

			} catch (CacheException e) {

				e.printStackTrace();
				success = false;

			} catch (StorageException e) {

				e.printStackTrace();
				success = false;
			}
		} else {
			success = false;
		}

		return success;
	}

	public boolean refineSubTree(FacetTree tree, StaticClusterNode node) {

		boolean success;

		if (node.getCurrentFacet().getType() != FacetType.DATAPROPERTY_BASED) {

			try {

				m_subTreeRefinerDelegate.clean();
				success = m_subTreeRefinerDelegate.refine(tree, node);

			} catch (DatabaseException e) {

				e.printStackTrace();
				success = false;

			} catch (IOException e) {

				e.printStackTrace();
				success = false;

			} catch (CacheException e) {

				e.printStackTrace();
				success = false;

			} catch (StorageException e) {

				e.printStackTrace();
				success = false;
			}
		} else {
			success = false;
		}

		return success;
	}

	public void setTreeDelegator(FacetTreeDelegator delegator) {
		m_treeDelegator = delegator;
	}
}
