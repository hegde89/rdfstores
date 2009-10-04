/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer project. 
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
package edu.unika.aifb.facetedSearch.facets.tree.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jcs.access.exception.CacheException;
import org.apache.log4j.Logger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.Delegator;
import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeType;
import edu.unika.aifb.facetedSearch.algo.construction.ConstructionDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class FacetTreeDelegator extends Delegator {

	private static Logger s_log = Logger.getLogger(FacetTreeDelegator.class);

	/*
	 * 
	 */
	private static FacetTreeDelegator s_instance;

	public static FacetTreeDelegator getInstance(SearchSession session) {
		return s_instance == null
				? s_instance = new FacetTreeDelegator(session)
				: s_instance;
	}

	/*
	 * 
	 */
	private ConstructionDelegator m_constructionDelegator;

	/*
	 * session
	 */
	private SearchSession m_session;
	private SearchSessionCache m_cache;

	/*
	 * stored maps ...
	 */
	private ArrayList<Map<? extends Object, ? extends Object>> m_maps;
	private StoredMap<String, FacetTree> m_domain2treeMap;

	/*
	 * bindings
	 */
	private EntryBinding<FacetTree> m_treeBinding;
	private EntryBinding<String> m_strgBinding;

	private FacetTreeDelegator(SearchSession session) {

		m_session = session;
		m_cache = m_session.getCache();

		m_constructionDelegator = (ConstructionDelegator) session
				.getDelegator(Delegators.CONSTRUCTION);

		init();
	}

	@Override
	public void clean() {

		for (Map<? extends Object, ? extends Object> map : m_maps) {
			map.clear();
		}
	}

	@Override
	public void close() {

		clean();
		m_maps.clear();
		m_maps = null;

		System.gc();
	}

	public List<Node> getChildren(String domain) {

		Node root = m_domain2treeMap.get(domain).getRoot();
		return getChildren(domain, root.getID());
	}

	public List<Node> getChildren(String domain, double fatherID) {

		List<Node> children = new ArrayList<Node>();

		FacetTree tree = m_domain2treeMap.get(domain);
		StaticNode node = (StaticNode) tree.getVertex(fatherID);

		if (!(node instanceof FacetValueNode)) {

			if (!tree.hasChildren(node)) {
				m_constructionDelegator.refine(tree, node);
			}

			children = tree.getChildren(node);
		}

		return children;
	}

	public Set<String> getDomains() {
		return m_domain2treeMap.keySet();
	}

	public Node getFather(String domain, double nodeID) {

		Node father = null;

		FacetTree tree = m_domain2treeMap.get(domain);
		Node child = tree.getVertex(nodeID);

		if (!child.equals(tree.getRoot())) {

			Iterator<Edge> inEdgesIter = tree.incomingEdgesOf(child).iterator();

			if (inEdgesIter.hasNext()) {

				father = tree.getEdgeSource(inEdgesIter.next());

			} else {
				s_log.error("node '" + child + "' has no father!");
			}
		} else {
			s_log.debug("node '" + child + "' is root!");
		}

		return father;
	}

	public Node getNode(String domain, double nodeID) {

		return m_domain2treeMap.get(domain).getVertex(nodeID);
	}

	public List<Double> getRangeLeaves(String domain, double nodeID) {

		FacetTree tree = m_domain2treeMap.get(domain);
		Node node = tree.getVertex(nodeID);

		if ((node.getLeaves() == null) || node.getLeaves().isEmpty()) {

			if (node.containsClass()) {

				Set<Node> leaves = tree.getVertices(NodeType.LEAVE);

				for (Node leave : leaves) {

					if (leave.getPath().startsWith(node.getPath())) {
						node.addLeave(leave.getID());
					}
				}
			} else {

				Iterator<Node> childrenIter = getChildren(domain, nodeID)
						.iterator();

				Node rangeTop = null;

				while (childrenIter.hasNext()) {

					Node child = childrenIter.next();

					if (child.containsClass()) {

						rangeTop = child;
						break;
					}
				}

				if (rangeTop != null) {

					Set<Node> leaves = tree.getVertices(NodeType.LEAVE);

					for (Node leave : leaves) {

						if (leave.getPath().startsWith(node.getPath())) {
							node.addLeave(leave.getID());

						}
					}
				} else {
					s_log.error("tree structure not valid for tree: " + tree);
				}
			}
		}

		return node.getLeaves();
	}

	public FacetTree getTree(String domain) {
		return m_domain2treeMap.get(domain);
	}

	private void init() {

		try {

			StoredClassCatalog cata = new StoredClassCatalog(m_cache
					.getDB(FacetEnvironment.DatabaseName.CLASS));

			m_treeBinding = new SerialBinding<FacetTree>(cata, FacetTree.class);
			m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);

			/*
			 * stored maps
			 */

			m_domain2treeMap = new StoredSortedMap<String, FacetTree>(m_cache
					.getDB(FacetEnvironment.DatabaseName.FTREE_CACHE),
					m_strgBinding, m_treeBinding, true);

			/*
			 * 
			 */

			m_maps = new ArrayList<Map<? extends Object, ? extends Object>>();
			m_maps.add(m_domain2treeMap);

		} catch (IllegalArgumentException e) {

			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	public void initTrees() {

		Table<String> resultTable;

		try {

			resultTable = m_session.getCache().getResultTable();

			((ConstructionDelegator) m_session
					.getDelegator(Delegators.CONSTRUCTION))
					.constructTrees(resultTable);

		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		} catch (CacheException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean isOpen() {

		if (m_maps != null) {

			boolean isOpen = true;

			for (Map<? extends Object, ? extends Object> map : m_maps) {

				if (map == null) {
					isOpen = false;
					break;
				}
			}

			return isOpen;
		} else {
			return false;
		}
	}

	public void storeTree(String domain, FacetTree tree) {
		m_domain2treeMap.put(domain, tree);
	}
}
