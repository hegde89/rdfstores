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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.apache.jcs.access.exception.CacheException;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.EdgeType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.FacetType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeContent;
import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeType;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.SingleValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticClusterNode;
import edu.unika.aifb.facetedSearch.index.FacetIndex;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore.IndexName;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class BuilderHelper {

	private static Logger s_log = Logger.getLogger(BuilderHelper.class);

	/*
	 * 
	 */
	private SearchSession m_session;
	private FacetIndex m_facetIndex;

	/*
	 * 
	 */
	private HashSet<String> m_parsedFacetValues;
	private StaticClusterNode m_localNameRangeLeave;

	public BuilderHelper(SearchSession session) {

		m_session = session;
		m_parsedFacetValues = new HashSet<String>();

		try {

			m_facetIndex = (FacetIndex) session.getStore().getIndex(
					IndexName.FACET_INDEX);

		} catch (EnvironmentLockedException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}
	}

	public void clean() {
		m_parsedFacetValues.clear();
		m_localNameRangeLeave = null;
	}

	public StaticClusterNode insertFacetValueAsResource(FacetTree tree,
			StaticClusterNode node, AbstractFacetValue fv) {

		if (m_localNameRangeLeave == null) {
			
			String pathPrefix = node.getPath();
			String pathDelta = "";

			StaticClusterNode labelPropertyNode = new StaticClusterNode();
			labelPropertyNode.setContent(NodeContent.DATA_PROPERTY);
			labelPropertyNode.setTopLevelFacet(node.getTopLevelFacet());
			
			Facet currentFacet = new Facet(RDFS.LABEL.stringValue());
			currentFacet.setDataType(DataType.STRING);
			currentFacet.setType(FacetType.DATAPROPERTY_BASED);
			currentFacet.setDomain(node.getDomain());
			
			labelPropertyNode.setCurrentFacet(currentFacet);
			labelPropertyNode.setValue(RDFS.LABEL.stringValue());
			labelPropertyNode.setType(NodeType.INNER_NODE);
			labelPropertyNode.setDomain(node.getDomain());

			String path = pathPrefix + pathDelta + labelPropertyNode.getValue();
			labelPropertyNode.setPath(path);
			pathDelta = pathDelta + labelPropertyNode.getValue();

			tree.addVertex(labelPropertyNode);

			Edge edge = tree.addEdge(node, labelPropertyNode);
			edge.setType(EdgeType.SUBPROPERTY_OF);

			StaticClusterNode labelPropertyRange = new StaticClusterNode();
			labelPropertyRange.setContent(NodeContent.CLASS);
			labelPropertyRange.setTopLevelFacet(node.getTopLevelFacet());
			labelPropertyRange.setCurrentFacet(labelPropertyNode.getCurrentFacet());
			labelPropertyRange.setValue(XMLSchema.STRING.stringValue());
			labelPropertyRange.setType(NodeType.RANGE_ROOT);
			labelPropertyRange.setDomain(node.getDomain());

			path = pathPrefix + pathDelta + labelPropertyRange.getValue();
			labelPropertyRange.setPath(path);

			tree.addVertex(labelPropertyRange);

			edge = tree.addEdge(labelPropertyNode, labelPropertyRange);
			edge.setType(EdgeType.HAS_RANGE);
			
			/*
			 * update leave group
			 */
			m_session.getCache().updateLeaveGroups(
					labelPropertyRange.getID(), fv.getValue());
			
			m_localNameRangeLeave = labelPropertyRange;
		}
		
		return m_localNameRangeLeave;
	}

	 public void insertFacetValue(FacetTree tree, StaticClusterNode node,
			AbstractFacetValue fv) {

		if (!m_parsedFacetValues.contains(fv.getValue())) {

			SingleValueNode fvNode = new SingleValueNode(fv.getValue());
			fvNode.setContent(NodeContent.CLASS);
			fvNode.setTopLevelFacet(node.getTopLevelFacet());
			fvNode.setCurrentFacet(node.getCurrentFacet());
			fvNode.setType(NodeType.LEAVE);
			fvNode.setDomain(node.getDomain());
			tree.addVertex(fvNode);

			Edge edge = tree.addEdge(node, fvNode);
			edge.setType(EdgeType.CONTAINS);

			m_parsedFacetValues.add(fv.getValue());
		}
	}

	public void insertFacetValues(FacetTree tree, StaticClusterNode node,
			Collection<? extends AbstractFacetValue> values) {

		Iterator<? extends AbstractFacetValue> valueIter = values.iterator();

		while (valueIter.hasNext()) {
			insertFacetValue(tree, node, valueIter.next());
		}

		clean();
	}

	public StaticClusterNode insertPathAtNode(FacetTree newTree, Node leave,
			StaticClusterNode node,
			Int2ObjectOpenHashMap<StaticClusterNode> paths)
			throws DatabaseException, IOException, CacheException {

		int pathHash = (node.getPath() + leave.getPath().substring(4)).hashCode();

		if (!paths.containsKey(pathHash)) {

			StaticClusterNode pos4insertion = node;
			Stack<Edge> edges2insert = new Stack<Edge>();

			Queue<Edge> path2root = m_facetIndex.getPath2Root(leave.getPath());

			while (!path2root.isEmpty()) {

				Edge currentEdge = path2root.poll();

				int currentPathHash = currentEdge.getTarget().getPath()
						.hashCode();

				if (!paths.containsKey(currentPathHash)) {
					edges2insert.add(currentEdge);
				} else {
					pos4insertion = paths.get(currentPathHash);
					break;
				}
			}

			if (!edges2insert.isEmpty()) {

				String pathPrefix = pos4insertion.getPath();
				String pathDelta = "";

				while (!edges2insert.isEmpty()) {

					Edge edge2insert = edges2insert.pop();
					Node node2copy = edge2insert.getTarget();

					StaticClusterNode newNode = new StaticClusterNode();
					newNode.setContent(node2copy.getContent());
					newNode.setTopLevelFacet(node.getTopLevelFacet());
					newNode.setCurrentFacet(node2copy.getTopLevelFacet());
					newNode.setValue(node2copy.getValue());
					newNode.setType(node2copy.getType());
					newNode.setDomain(newTree.getDomain());

					String path = pathPrefix + pathDelta + newNode.getValue();
					newNode.setPath(path);
					paths.put(path.hashCode(), newNode);
					pathDelta = pathDelta + newNode.getValue();

					newTree.addVertex(newNode);

					Edge edge = newTree.addEdge(pos4insertion, newNode);
					edge.setType(edge2insert.getType());

					pos4insertion = newNode;
				}
			}

			return pos4insertion;

		} else {
			
			return paths.get(pathHash);
		}
	}

	public StaticClusterNode insertPathAtRoot(FacetTree newTree, Node leave,
			Int2ObjectOpenHashMap<StaticClusterNode> paths)
			throws DatabaseException, IOException, CacheException {

		int pathHash = leave.getPath().hashCode();

		if (!paths.containsKey(pathHash)) {

			StaticClusterNode pos4insertion = newTree.getRoot();
			Stack<Edge> edges2insert = new Stack<Edge>();

			Queue<Edge> path2root = m_facetIndex.getPath2Root(leave.getPath());

			while (!path2root.isEmpty()) {

				Edge currentEdge = path2root.poll();

				int currentPathHash = currentEdge.getTarget().getPath()
						.hashCode();

				if (!paths.containsKey(currentPathHash)) {
					edges2insert.add(currentEdge);
				} else {
					pos4insertion = paths.get(currentPathHash);
					break;
				}
			}

			if (!edges2insert.isEmpty()) {

				String pathPrefix = pos4insertion.getPath();
				String pathDelta = "";

				while (!edges2insert.isEmpty()) {

					Edge edge2insert = edges2insert.pop();
					Node node2copy = edge2insert.getTarget();

					StaticClusterNode newNode = new StaticClusterNode();
					newNode.setContent(node2copy.getContent());
					newNode.setTopLevelFacet(node2copy.getTopLevelFacet());
					newNode.setCurrentFacet(node2copy.getTopLevelFacet());
					newNode.setValue(node2copy.getValue());
					newNode.setType(node2copy.getType());
					newNode.setDomain(newTree.getDomain());

					String path = pathPrefix + pathDelta + newNode.getValue();
					newNode.setPath(path);
					paths.put(path.hashCode(), newNode);
					pathDelta = pathDelta + newNode.getValue();

					newTree.addVertex(newNode);

					Edge edge = newTree.addEdge(pos4insertion, newNode);
					edge.setType(edge2insert.getType());

					pos4insertion = newNode;
				}
			}

			return pos4insertion;

		} else {

			return paths.get(pathHash);
		}
	}

	public FacetTree pruneRanges(FacetTree tree, Set<StaticClusterNode> leaves)
			throws DatabaseException {

		Iterator<StaticClusterNode> iter = leaves.iterator();

		while (iter.hasNext()) {

			StaticClusterNode currentNode = iter.next();
			boolean reachedRangeRoot = false;

			// walk to root

			while (!reachedRangeRoot) {

				Iterator<Edge> incomingEdgesIter = tree.incomingEdgesOf(
						currentNode).iterator();

				if (currentNode.isRangeRoot()) {

					reachedRangeRoot = true;

				} else if (incomingEdgesIter.hasNext()) {

					Edge edge2father = incomingEdgesIter.next();
					Node father = tree.getEdgeSource(edge2father);

					if (father.isRangeRoot()) {

						reachedRangeRoot = true;

					} else {

						if ((tree.outgoingEdgesOf(father).size() == 1)
								&& !leaves.contains(father)) {

							Edge edge2fathersfather = tree.incomingEdgesOf(
									father).iterator().next();

							StaticClusterNode fathersfather = (StaticClusterNode) tree
									.getEdgeSource(edge2fathersfather);

							Edge newEdge = tree.addEdge(fathersfather,
									currentNode);
							newEdge.setType(EdgeType.SUBCLASS_OF);

							tree.removeEdge(father, currentNode);
							tree.removeEdge(fathersfather, father);

							tree.removeVertex(father);

						} else {

							currentNode = (StaticClusterNode) father;
						}
					}
				} else {
					s_log.error("tree structure is not correct: " + tree);
					break;
				}
			}
		}

		return tree;
	}
}