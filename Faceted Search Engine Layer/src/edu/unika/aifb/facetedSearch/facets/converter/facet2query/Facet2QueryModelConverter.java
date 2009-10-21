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

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.facets.converter.AbstractConverter;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
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

	public static Facet2QueryModelConverter getInstance() {
		return s_instance == null
				? s_instance = new Facet2QueryModelConverter()
				: s_instance;
	}

	private Facet2QueryModelConverter() {

	}

	public StructuredQuery node2facetFacetValuePath(StaticNode node) {

		// SearchSession session = node.getSession();
		StructuredQuery sq = new StructuredQuery("");
		//		
		// FacetTreeDelegator treeDelegator = (FacetTreeDelegator)
		// session.getDelegator(Delegators.TREE);
		// Stack<Edge> path = treeDelegator.getPathFromRoot(node);
		//
		// String currentSubject = null;
		// String currentProperty = null;
		// String currentObject = null;
		//
		// Edge edge;
		// Node src;
		// Node tar;
		//
		// String currentVar = node.getDomain();
		// sq.getQueryGraph().addVertex(new QNode(currentVar));
		// sq.setAsSelect(currentVar);
		//
		// while (!path.isEmpty()) {
		//
		// edge = path.pop();
		// src = edge.getSource();
		//
		// if (src.isSubTreeRoot()) {
		//
		// currentSubject = currentVar;
		//
		// while (edge.getType() != EdgeType.HAS_RANGE) {
		// edge = path.pop();
		// }
		//
		// src = edge.getSource();
		// currentProperty = src.getValue();
		//
		// if (path.isEmpty()) {
		//
		// tar = edge.getTarget();
		//
		// if (tar.getFacet().isDataPropertyBased()) {
		//
		// currentObject = "[" + tar.getValue() + "]";
		// sq.addEdge(currentSubject, currentProperty,
		// currentObject);
		//
		// } else {
		//
		// currentObject = session.getCurrentQuery()
		// .getNextVar();
		// sq.addEdge(currentSubject, currentProperty,
		// currentObject);
		// }
		// } else {
		//
		// edge = path.pop();
		//
		// while (!path.isEmpty()
		// && (edge.getType() == EdgeType.SUBCLASS_OF || edge
		// .getType() == EdgeType.CONTAINS)) {
		// edge = path.pop();
		// }
		//
		// tar = edge.getTarget();
		//
		// if (path.isEmpty()) {
		//
		// if (tar.getFacet().isDataPropertyBased()) {
		//
		// if (tar instanceof FacetValueNode) {
		//
		// currentObject = tar.getValue();
		// sq.addEdge(currentSubject, currentProperty,
		// currentObject);
		//
		// } else {
		//
		// currentObject = "[" + tar.getValue() + "]";
		//
		// sq.addEdge(currentSubject, currentProperty,
		// currentObject);
		// }
		//
		// } else {
		//
		// currentObject = session.getCurrentQuery()
		// .getNextVar();
		//
		// if (!(tar instanceof FacetValueNode)) {
		//
		// if (!FacetUtils.isGenericNode(tar)) {
		//
		// sq
		// .addEdge(
		// currentObject,
		// FacetEnvironment.RDF.NAMESPACE
		// + FacetEnvironment.RDF.TYPE,
		// tar.getValue());
		// }
		//
		// sq.addEdge(currentSubject, currentProperty,
		// currentObject);
		// } else {
		//
		// sq.addEdge(currentSubject, currentProperty, tar
		// .getValue());
		// }
		// }
		// } else {
		//
		// src = edge.getSource();
		//
		// currentObject = session.getCurrentQuery()
		// .getNextVar();
		//
		// if (!FacetUtils.isGenericNode(src)) {
		//
		// sq.addEdge(currentObject,
		// FacetEnvironment.RDF.NAMESPACE
		// + FacetEnvironment.RDF.TYPE, src
		// .getValue());
		// }
		//
		// sq.addEdge(currentSubject, currentProperty,
		// currentObject);
		//
		// currentVar = currentObject;
		// path.push(edge);
		// }
		// }
		// }
		// }

		return sq;
	}
}
