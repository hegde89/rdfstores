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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import cern.colt.map.HashFunctions;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.algo.construction.tree.IFacetTreeBuilder;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeType;
import edu.unika.aifb.facetedSearch.index.FacetIndex;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore.IndexName;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class FacetTreeBuilder implements IFacetTreeBuilder {

	private SearchSession m_session;
	@SuppressWarnings("unused")
	private SearchSessionCache m_cache;

	// indices
	private FacetIndex m_facetIndex;
	private StructureIndex m_structureIndex;

	private HashSet<String> m_SourceExtension;
	private HashMap<String, FacetTree> m_indexedTrees;
	private HashMap<Integer, Node> m_paths;
	private HashSet<Node> m_endPoints;

	public FacetTreeBuilder(SearchSession session) throws IOException,
			EnvironmentLockedException, DatabaseException, StorageException {

		m_session = session;
		m_cache = m_session.getCache();
		m_facetIndex = (FacetIndex) m_session.getStore().getIndex(
				IndexName.FACET_INDEX);
		m_structureIndex = (StructureIndex) m_session.getStore().getIndex(
				IndexName.STRUCTURE_INDEX);

		m_SourceExtension = new HashSet<String>();
		m_indexedTrees = new HashMap<String, FacetTree>();
		m_paths = new HashMap<Integer, Node>();
		m_endPoints = new HashSet<Node>();

	}

	public FacetTree contruct(Table<String> results, int column)
			throws StorageException, IOException, DatabaseException {

		String domain = results.getColumnName(column);
		FacetTree tree = new FacetTree();
		Iterator<String[]> iter = results.iterator();

		while (iter.hasNext()) {

			FacetTree currentStoredTree;

			String resItem = iter.next()[column];
			// TODO: performance for this lookup?
			String extension = m_structureIndex.getExtension(resItem);

			if (!m_SourceExtension.contains(extension)) {

				// add extenison and load tree into RAM
				m_SourceExtension.add(extension);

				FacetTree storedTree = m_facetIndex.getFacetTree(extension);
				m_indexedTrees.put(extension, storedTree);

				currentStoredTree = storedTree;

			} else {
				currentStoredTree = m_indexedTrees.get(extension);
			}

			HashSet<Node> leaves = m_facetIndex.getLeaves(extension, resItem);

			for (Node leave : leaves) {

				Node pos4insertion = tree.getRoot();
				Stack<Edge> edges2insert = new Stack<Edge>();

				// TODO: get from cache
				List<Edge> edges2root = currentStoredTree.getPath2Root(leave)
						.getEdgeList();

				Stack<Edge> path2root = new Stack<Edge>();
				path2root.addAll(edges2root);

				while (!path2root.isEmpty()) {

					Edge currentEdge = path2root.pop();
					int currentPathHash = currentStoredTree.getEdgeTarget(
							currentEdge).getPathHashValue();

					if (!m_paths.containsKey(currentPathHash)) {
						edges2insert.add(currentEdge);
					} else {
						pos4insertion = m_paths.get(currentPathHash);
					}
				}

				if (!edges2insert.isEmpty()) {

					String pathPrefix = pos4insertion.getPath();
					String pathDelta = "";

					while (!edges2insert.isEmpty()) {

						Edge edge2insert = edges2insert.pop();
						Node node2copy = currentStoredTree
								.getEdgeTarget(edge2insert);

						Node newNode = new Node();
						newNode.setContent(node2copy.getContent());
						newNode.setValue(node2copy.getValue());
						newNode.addTypes(node2copy.getTypes());
						newNode.setFacet(node2copy.getFacet());
						newNode.removeType(NodeType.ENDPOINT);
						newNode.setDomain(domain);
						newNode.setPath(pathPrefix + pathDelta);
						newNode.setPathHashValue(HashFunctions.hash(pathPrefix
								+ pathDelta));

						tree.addVertex(newNode);

						Edge edge = tree.addEdge(pos4insertion, newNode);
						edge.setType(edge2insert.getType());

						m_paths.put(HashFunctions.hash(pathPrefix + pathDelta),
								newNode);

						pathDelta = pathDelta + newNode.getValue();
						pos4insertion = newNode;
					}

					pos4insertion.addType(NodeType.ENDPOINT);
					m_endPoints.add(pos4insertion);
				}

			}
		}

		return tree;
	}
}
