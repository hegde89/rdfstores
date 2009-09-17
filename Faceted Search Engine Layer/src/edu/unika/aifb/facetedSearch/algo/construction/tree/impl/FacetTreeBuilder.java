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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

import org.apache.log4j.Logger;

import cern.colt.map.HashFunctions;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.LiteralComparator;
import edu.unika.aifb.facetedSearch.algo.construction.tree.IFacetTreeBuilder;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree.EndPointType;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.FacetType;
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

	private static Logger s_log = Logger.getLogger(FacetTreeBuilder.class);

	private SearchSession m_session;
	private SearchSessionCache m_cache;

	// indices
	private FacetIndex m_facetIndex;
	private StructureIndex m_structureIndex;

	private HashMap<String, FacetTree> m_indexedTrees;
	private HashMap<Integer, StaticNode> m_paths;
	private LiteralComparator m_comparator;

	public FacetTreeBuilder(SearchSession session) throws IOException,
			EnvironmentLockedException, DatabaseException, StorageException {

		m_session = session;
		m_cache = session.getCache();

		m_facetIndex = (FacetIndex) m_session.getStore().getIndex(
				IndexName.FACET_INDEX);
		m_structureIndex = (StructureIndex) m_session.getStore().getIndex(
				IndexName.STRUCTURE_INDEX);

		m_indexedTrees = new HashMap<String, FacetTree>();
		m_paths = new HashMap<Integer, StaticNode>();
		m_comparator = new LiteralComparator();

	}

	public FacetTree contruct(Table<String> results, int column)
			throws StorageException, IOException, DatabaseException {

		FacetTree newTree = new FacetTree();
		newTree.setDomain(results.getColumnName(column));

		Iterator<String[]> iter = results.iterator();

		while (iter.hasNext()) {

			FacetTree currentIndexedTree;

			String resItem = iter.next()[column];
			String sourceExtension = m_structureIndex.getExtension(resItem);

			if (!m_indexedTrees.containsKey(sourceExtension)) {

				FacetTree indexedTree = m_facetIndex.getTree(sourceExtension);
				m_indexedTrees.put(sourceExtension, indexedTree);

				currentIndexedTree = indexedTree;

			} else {

				currentIndexedTree = m_indexedTrees.get(sourceExtension);

			}

			HashSet<Double> leaves = m_facetIndex.getLeaves(sourceExtension,
					resItem);

			StaticNode newLeave;

			for (double leave : leaves) {

				newLeave = insertPath(newTree, currentIndexedTree, leave);
				updateNodes(newTree, resItem, sourceExtension, newLeave);

			}
		}

		sortLiteralLists(newTree);

		return newTree;
	}

	private EndPointType getEndPointType(Node endpoint) {

		EndPointType epType;

		if (endpoint.getFacet().getType() == FacetType.DATAPROPERTY_BASED) {
			epType = EndPointType.DATA_PROPERTY;
		} else if (endpoint.getFacet().getType() == FacetType.OBJECT_PROPERTY_BASED) {
			epType = EndPointType.OBJECT_PROPERTY;
		} else {
			epType = EndPointType.RDF_PROPERTY;
		}

		return epType;
	}

	private StaticNode insertPath(FacetTree newTree, FacetTree indexedTree,
			double leaveId) throws DatabaseException, IOException {

		Node pos4insertion = newTree.getRoot();
		Stack<Edge> edges2insert = new Stack<Edge>();

		Queue<Edge> path2root = indexedTree.getAncestorPath2Root(leaveId);

		while (!path2root.isEmpty()) {

			Edge currentEdge = path2root.poll();
			int currentPathHash = indexedTree.getEdgeSource(currentEdge)
					.getPathHashValue();

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
				Node node2copy = indexedTree.getEdgeTarget(edge2insert);

				StaticNode newNode = new StaticNode();
				newNode.setContent(node2copy.getContent());
				newNode.setFacet(node2copy.getFacet());
				newNode.setValue(node2copy.getValue());
				newNode.setType(node2copy.getType());
				newNode.setDomain(newTree.getDomain());
				newNode.setPath(pathPrefix + pathDelta);
				newNode.setPathHashValue(HashFunctions.hash(pathPrefix
						+ pathDelta));
				newNode.setCache(m_cache);
				newNode.setID(node2copy.getID());
				newNode.setFacet(node2copy.getFacet());

				newTree.addVertex(newNode);

				Edge edge = newTree.addEdge(pos4insertion, newNode);
				edge.setType(edge2insert.getType());

				m_paths
						.put(HashFunctions.hash(pathPrefix + pathDelta),
								newNode);

				pathDelta = pathDelta + newNode.getValue();
				pos4insertion = newNode;
			}

			newTree.addEndPoint(getEndPointType(pos4insertion), pos4insertion);
		}

		return (StaticNode) pos4insertion;
	}

	private void sortLiteralLists(FacetTree tree) {

		Iterator<Node> iter = tree.getEndPoints(EndPointType.DATA_PROPERTY)
				.iterator();

		while (iter.hasNext()) {

			Collections.sort(((StaticNode) iter.next()).getSortedLiterals(),
					m_comparator);

		}
	}

	private void updateNodes(FacetTree newTree, String resItem,
			String sourceExt, StaticNode leave) throws DatabaseException,
			IOException, StorageException {

		Queue<Edge> path2RangeRoot = newTree.getAncestorPath2RangeRoot(leave
				.getID());

		List<String> objects = new ArrayList<String>();
		objects.addAll(m_facetIndex.getObjects(leave.getID(), resItem));

		List<String> rangeExt = new ArrayList<String>();
		rangeExt.addAll(m_facetIndex.getExtensions(leave.getID(), resItem));

		if (!path2RangeRoot.isEmpty()) {

			StaticNode node;
			int height = 0;

			while (!path2RangeRoot.isEmpty()) {

				node = (StaticNode) newTree
						.getEdgeSource(path2RangeRoot.poll());
				node.addSourceIndivdiual(resItem);

				// object property or rdf property based
				if (leave.getFacet().getType() != FacetType.DATAPROPERTY_BASED) {
					node.addUnsortedObjects(objects);
				} else {
					s_log.debug("should not be here ... node:" + node);
				}

				node.addSourceExtension(sourceExt);
				node.addRangeExtensions(rangeExt);

				node.incrementCountS(1);
				node.incrementCountFV(objects.size());

				node.setHeight(height++);

			}
		} else {

			leave.addSourceIndivdiual(resItem);

			if (leave.getFacet().getType() == FacetType.DATAPROPERTY_BASED) {
				// TODO: if not enough RAM > store in db
				leave.addSortedObjects(objects);
			}
			// object property or rdf property based
			else {
				leave.addUnsortedObjects(objects);
			}

			leave.addSourceExtension(sourceExt);
			leave.addRangeExtensions(rangeExt);

			leave.incrementCountS(1);
			leave.incrementCountFV(objects.size());

			leave.setHeight(0);
		}
	}
}
