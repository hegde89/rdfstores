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
package edu.unika.aifb.facetedSearch.facets.tree;

import it.unimi.dsi.fastutil.doubles.Double2ObjectOpenHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.jcs.access.exception.CacheException;
import org.apache.log4j.Logger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.Delegator;
import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.algo.construction.ConstructionDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
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
	private ConstructionDelegator m_constructionDelegator;

	/*
	 * session
	 */
	private SearchSession m_session;

	/*
	 * maps ...
	 */
	private ArrayList<Map<? extends Object, ? extends Object>> m_maps;
	private StoredMap<String, FacetTree> m_domain2treeMap;
	private Double2ObjectOpenHashMap<Stack<Edge>> m_node2pathMap;

	/*
	 * bindings
	 */
	private EntryBinding<FacetTree> m_treeBinding;
	private EntryBinding<String> m_strgBinding;

	public FacetTreeDelegator(SearchSession session) {

		m_session = session;
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
	}

	public List<Node> getChildren(StaticNode father) {

		List<Node> children = new ArrayList<Node>();
		FacetTree tree = m_domain2treeMap.get(father.getDomain());

		if (!(father instanceof FacetValueNode)) {

			if (!tree.hasChildren(father)) {

				m_constructionDelegator.refine(tree, father);
				m_domain2treeMap.put(father.getDomain(), tree);
			}

			children = tree.getChildren(father);
		}

		return children;
	}

	public List<Node> getChildren(String domain) {

		Node root = m_domain2treeMap.get(domain).getRoot();
		return getChildren((StaticNode) root);
	}

	public List<Node> getChildren(String domain, double fatherID) {

		Node father = m_domain2treeMap.get(domain).getVertex(fatherID);
		return getChildren((StaticNode) father);
	}

	public Set<String> getDomains() {
		return m_domain2treeMap.keySet();
	}

	public Node getFather(Node child) {

		Node father = null;

		FacetTree tree = m_domain2treeMap.get(child.getDomain());

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

	public Node getFather(String domain, double nodeID) {

		Node child = m_domain2treeMap.get(domain).getVertex(nodeID);
		return getFather(child);
	}

	public Node getNode(String domain, double nodeID) {
		return m_domain2treeMap.get(domain).getVertex(nodeID);
	}

	public Stack<Edge> getPathFromRoot(StaticNode toNode) {

		FacetTree tree = m_domain2treeMap.get(toNode.getDomain());

		if (!m_node2pathMap.containsKey(toNode.getID())) {

			Node currentNode = toNode;
			Stack<Edge> path = new Stack<Edge>();
			boolean reachedRoot = toNode.isRoot();

			while (!reachedRoot) {

				Iterator<Edge> incomingEdgesIter = tree.incomingEdgesOf(
						currentNode).iterator();

				if (incomingEdgesIter.hasNext()) {

					Edge edge2father = incomingEdgesIter.next();
					path.push(edge2father);

					Node father = tree.getEdgeSource(edge2father);

					if (father.isRoot()) {
						reachedRoot = true;
					} else {
						currentNode = father;
					}
				} else {
					s_log.error("tree structure is not correct " + this);
					break;
				}
			}

			m_node2pathMap.put(toNode.getID(), path);
		}

		return m_node2pathMap.get(toNode.getID());
	}

	public List<Double> getRangeLeaves(String domain, double nodeID) {

		FacetTree tree = m_domain2treeMap.get(domain);
		Node node = tree.getVertex(nodeID);

		if ((node.getLeaves() == null) || node.getLeaves().isEmpty()) {

			Node subTreeRoot = tree.getSubTreeRoot4Node(node);
			Set<Node> leaves = tree.getLeaves4SubtreeRoot(subTreeRoot.getID());

			for (Node leave : leaves) {

				if (!node.hasPath()) {
					node.updatePath(tree);
				}

				if (leave.getPath().startsWith(node.getPath())) {
					node.addLeave(leave.getID());
				}
			}
		}

		return node.getLeaves();
	}

	public Node getRangeTop(StaticNode property) {

		StaticNode rangeTop = null;
		boolean foundRangeRoot = false;

		List<Node> children = getChildren(property);

		for (Node child : children) {

			if (child.isRangeRoot()) {

				rangeTop = (StaticNode) child;
				foundRangeRoot = true;
				break;
			}
		}

		if (!foundRangeRoot) {
			s_log.error("tree structure is not valid :"
					+ m_domain2treeMap.get(property.getDomain()));
		}

		return rangeTop;
	}

	public FacetTree getTree(String domain) {
		return m_domain2treeMap.get(domain);
	}

	private void init() {

		m_node2pathMap = new Double2ObjectOpenHashMap<Stack<Edge>>();

		try {

			StoredClassCatalog cata = new StoredClassCatalog(m_session
					.getCache().getDB(FacetEnvironment.DatabaseName.CLASS));

			m_treeBinding = new SerialBinding<FacetTree>(cata, FacetTree.class);
			m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);

			/*
			 * stored maps
			 */

			m_domain2treeMap = new StoredMap<String, FacetTree>(m_session
					.getCache()
					.getDB(FacetEnvironment.DatabaseName.FTREE_CACHE),
					m_strgBinding, m_treeBinding, true);

		} catch (IllegalArgumentException e) {

			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}

		m_maps = new ArrayList<Map<? extends Object, ? extends Object>>();
		m_maps.add(m_domain2treeMap);
		m_maps.add(m_node2pathMap);
	}

	public void initTrees(Table<String> resultTable) {

		try {

			((ConstructionDelegator) m_session
					.getDelegator(Delegators.CONSTRUCTION))
					.constructTrees(resultTable);

		} catch (EnvironmentLockedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
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

	public void setConstructionDelegator(ConstructionDelegator delegator) {
		m_constructionDelegator = delegator;
	}

	public void storeTree(String domain, FacetTree tree) {
		m_domain2treeMap.put(domain, tree);
	}
}
