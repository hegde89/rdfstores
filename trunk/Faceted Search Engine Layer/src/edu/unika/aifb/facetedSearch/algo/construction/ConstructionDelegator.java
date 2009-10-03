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
import java.util.ArrayList;

import org.apache.jcs.access.exception.CacheException;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.Delegator;
import edu.unika.aifb.facetedSearch.FacetEnvironment.FacetType;
import edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder;
import edu.unika.aifb.facetedSearch.algo.construction.tree.impl.BuilderHelper;
import edu.unika.aifb.facetedSearch.algo.construction.tree.impl.FacetSingleLinkageClusterBuilder;
import edu.unika.aifb.facetedSearch.algo.construction.tree.impl.FacetSubTreeBuilder;
import edu.unika.aifb.facetedSearch.algo.construction.tree.impl.FacetTreeBuilder;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Util;

/**
 * @author andi
 * 
 */
public class ConstructionDelegator extends Delegator {

	private static Logger s_log = Logger.getLogger(ResultPage.class);

	/*
	 * 
	 */
	private static ConstructionDelegator s_instance;

	public static ConstructionDelegator getInstance(SearchSession session) {

		return s_instance == null ? s_instance = new ConstructionDelegator(
				session) : s_instance;
	}

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
	private ArrayList<IBuilder> m_builder;

	private IBuilder m_treeBuilder;
	private IBuilder m_subTreeBuilder;
	private IBuilder m_clusterBuilder;

	private ConstructionDelegator(SearchSession session) {

		m_session = session;

		/*
		 * 
		 */

		m_treeDelegator = (FacetTreeDelegator) m_session
				.getDelegator(Delegators.TREE);

		/*
		 * 
		 */

		m_helper = new BuilderHelper(m_session);

		m_treeBuilder = new FacetTreeBuilder(m_session, m_helper);
		m_subTreeBuilder = new FacetSubTreeBuilder(m_session, m_helper);
		m_clusterBuilder = new FacetSingleLinkageClusterBuilder(m_session, m_helper);

		m_builder = new ArrayList<IBuilder>();
		m_builder.add(m_treeBuilder);
		m_builder.add(m_subTreeBuilder);
		m_builder.add(m_clusterBuilder);

	}

	@Override
	public void clean() {

		for (IBuilder builder : m_builder) {
			builder.clean();
		}
	}

	@Override
	public void close() {

		for (IBuilder builder : m_builder) {
			builder.close();
		}
	}

	public boolean constructTree(Table<String> results)
			throws EnvironmentLockedException, IOException, DatabaseException,
			StorageException, CacheException {

		s_log.debug("start facet construction for new result set '"
				+ results.toString() + "'");

		boolean success = true;
		m_treeDelegator.clean();

		for (String colName : results.getColumnNames()) {

			if (Util.isVariable(colName)) {

				s_log.debug("start building facet tree for column '" + colName
						+ "'");

				m_treeBuilder.clean();

				success = ((FacetTreeBuilder) m_treeBuilder).build(results,
						results.getColumn(colName));

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

	public boolean refine(FacetTree tree, StaticNode node) {

		if (node.getFacet().getType() == FacetType.DATAPROPERTY_BASED) {

			return refineCluster(tree, node);

		} else if (node.getFacet().getType() != FacetType.DATAPROPERTY_BASED) {

			return refineSubTree(tree, node);

		} else {

			s_log.error("should not be here: node '" + node + "' and tree '"
					+ tree + "'");
			return false;
		}
	}

	public boolean refineCluster(FacetTree tree, StaticNode node) {

		m_clusterBuilder.clean();

		return ((FacetSingleLinkageClusterBuilder) m_clusterBuilder).build(tree, node);
	}

	public boolean refineSubTree(FacetTree tree, StaticNode node) {

		m_subTreeBuilder.clean();

		try {

			return ((FacetSubTreeBuilder) m_subTreeBuilder).build(tree, node);

		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (CacheException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}

		return false;
	}
}
