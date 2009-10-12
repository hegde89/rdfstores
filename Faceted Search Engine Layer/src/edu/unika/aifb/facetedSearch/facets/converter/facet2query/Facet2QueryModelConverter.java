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
package edu.unika.aifb.facetedSearch.facets.converter.facet2query;

import java.util.Stack;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.EdgeType;
import edu.unika.aifb.facetedSearch.facets.converter.AbstractConverter;
import edu.unika.aifb.facetedSearch.facets.tree.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.facetedSearch.util.FacetUtils;
import edu.unika.aifb.graphindex.query.StructuredQuery;

/**
 * @author andi
 * 
 */
public class Facet2QueryModelConverter extends AbstractConverter {

	@SuppressWarnings("unused")
	private static Logger s_log = Logger
			.getLogger(Facet2QueryModelConverter.class);

	/*
	 * 
	 */
	private static Facet2QueryModelConverter s_instance;

	public static Facet2QueryModelConverter getInstance(SearchSession session) {
		return s_instance == null ? s_instance = new Facet2QueryModelConverter(
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

	private Facet2QueryModelConverter(SearchSession session) {

		m_session = session;
		m_treeDelegator = (FacetTreeDelegator) m_session
				.getDelegator(Delegators.TREE);
	}

	public StructuredQuery node2facetFacetValuePath(StaticNode node) {

		StructuredQuery sq = new StructuredQuery("");

		Stack<Edge> path = m_treeDelegator.getPathFromRoot(node);

		String currentSubject = null;
		String currentProperty = null;
		String currentObject = null;

		Edge edge;
		Node src;
		Node tar;

		String currentVar = node.getDomain();

		while (!path.isEmpty()) {

			edge = path.pop();
			src = edge.getSource();

			if (src.isSubTreeRoot()) {

				currentSubject = currentVar;

				while (edge.getType() != EdgeType.HAS_RANGE) {
					edge = path.pop();
				}

				src = edge.getSource();
				currentProperty = src.getValue();

				if (path.isEmpty()) {

					tar = edge.getTarget();

					if (tar.getFacet().isDataPropertyBased()) {

						currentObject = "[" + tar.getValue() + "]";
						sq.addEdge(currentSubject, currentProperty,
								currentObject);

					} else {

						currentObject = m_session.getCurrentQuery()
								.getNextVar();
						sq.addEdge(currentSubject, currentProperty,
								currentObject);
					}
				} else {

					edge = path.pop();

					while (!path.isEmpty()
							&& (edge.getType() == EdgeType.SUBCLASS_OF)) {
						edge = path.pop();
					}

					tar = edge.getTarget();

					if (path.isEmpty()) {

						if (tar.getFacet().isDataPropertyBased()) {

							if (tar instanceof FacetValueNode) {

								currentObject = tar.getValue();
								sq.addEdge(currentSubject, currentProperty,
										currentObject);

							} else {

								currentObject = "[" + tar.getValue() + "]";

								sq.addEdge(currentSubject, currentProperty,
										currentObject);
							}

						} else {

							currentObject = m_session.getCurrentQuery()
									.getNextVar();

							if (!FacetUtils.isGenericNode(tar)) {

								sq.addEdge(currentObject,
										FacetEnvironment.RDF.NAMESPACE
												+ FacetEnvironment.RDF.TYPE,
										tar.getValue());
							}

							sq.addEdge(currentSubject, currentProperty,
									currentObject);
						}
					} else {

						src = edge.getSource();
						
						currentObject = m_session.getCurrentQuery()
								.getNextVar();

						if (!FacetUtils.isGenericNode(src)) {

							sq.addEdge(currentObject,
									FacetEnvironment.RDF.NAMESPACE
											+ FacetEnvironment.RDF.TYPE, src
											.getValue());
						}

						sq.addEdge(currentSubject, currentProperty,
								currentObject);

						currentVar = currentObject;
						path.push(edge);
					}
				}
			}
		}

		return sq;
	}
}
