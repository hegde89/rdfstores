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

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.EdgeType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeContent;
import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeType;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractFacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
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

	/*
	 * 
	 */
	private FacetIndex m_facetIndex;

	/*
	 * 
	 */
	private HashSet<String> m_parsedFacetValues;

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

	public void clearParsedFacetValues() {
		m_parsedFacetValues.clear();
	}

	public void insertFacetValue(FacetTree tree, StaticNode node,
			AbstractFacetValue fv) {

		if (!m_parsedFacetValues.contains(fv.getValue())) {

			FacetValueNode fvNode = new FacetValueNode(fv.getValue());
			fvNode.setContent(NodeContent.CLASS);
			fvNode.setFacet(node.getFacet());
			fvNode.setType(NodeType.LEAVE);
			fvNode.setDomain(node.getDomain());
			fvNode.setSession(m_session);
			fvNode.setDepth(node.getDepth() + 1);

			tree.addVertex(fvNode);

			Edge edge = tree.addEdge(node, fvNode);
			edge.setType(EdgeType.CONTAINS);

			m_parsedFacetValues.add(fv.getValue());
		}
	}

	public void insertFacetValues(FacetTree tree, StaticNode node,
			Collection<? extends AbstractFacetValue> values) {

		Iterator<? extends AbstractFacetValue> valueIter = values.iterator();

		while (valueIter.hasNext()) {
			insertFacetValue(tree, node, valueIter.next());
		}

		clearParsedFacetValues();
	}

	public StaticNode insertPathAtNode(FacetTree newTree, Node leave,
			StaticNode node, Int2ObjectOpenHashMap<StaticNode> paths)
			throws DatabaseException, IOException, CacheException {

		int pathHash = leave.getPath().hashCode();

		if (!paths.containsKey(pathHash)) {

			StaticNode pos4insertion = node;
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

					StaticNode newNode = new StaticNode();
					newNode.setContent(node2copy.getContent());
					newNode.setFacet(node2copy.getFacet());
					newNode.setValue(node2copy.getValue());
					newNode.setType(node2copy.getType());
					newNode.setDomain(newTree.getDomain());
					newNode.setSession(m_session);
					newNode.setDepth(pos4insertion.getDepth() + 1);

					if (node2copy.getFacet().getType() == FacetEnvironment.FacetType.RDF_PROPERTY_BASED) {
						newNode.setTypeLeave(true);
					}

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

			return paths.get(leave.getPath().hashCode());
		}
	}

	public StaticNode insertPathAtRoot(FacetTree newTree, Node leave,
			Int2ObjectOpenHashMap<StaticNode> paths) throws DatabaseException,
			IOException, CacheException {

		return insertPathAtNode(newTree, leave, newTree.getRoot(), paths);
	}

	public FacetTree pruneRanges(FacetTree tree, Set<StaticNode> leaves)
			throws DatabaseException {

		Iterator<StaticNode> iter = leaves.iterator();

		while (iter.hasNext()) {

			StaticNode currentNode = iter.next();
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

							StaticNode fathersfather = (StaticNode) tree
									.getEdgeSource(edge2fathersfather);
							fathersfather
									.setHeight(currentNode.getHeight() + 1);

							Edge newEdge = tree.addEdge(fathersfather,
									currentNode);
							newEdge.setType(EdgeType.SUBCLASS_OF);

							tree.removeEdge(father, currentNode);
							tree.removeEdge(fathersfather, father);

							tree.removeVertex(father);

						} else {

							currentNode = (StaticNode) father;
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
