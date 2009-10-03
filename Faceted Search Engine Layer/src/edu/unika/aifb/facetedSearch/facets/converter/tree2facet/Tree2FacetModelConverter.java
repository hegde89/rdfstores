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
package edu.unika.aifb.facetedSearch.facets.converter.tree2facet;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeContent;
import edu.unika.aifb.facetedSearch.facets.converter.AbstractConverter;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetValueCluster;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.model.impl.Resource;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.DynamicNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class Tree2FacetModelConverter extends AbstractConverter {

	private static Logger s_log = Logger
			.getLogger(Tree2FacetModelConverter.class);

	/*
	 * 
	 */
	private static Tree2FacetModelConverter s_instance;

	public static Tree2FacetModelConverter getInstance(SearchSession session) {
		return s_instance == null ? s_instance = new Tree2FacetModelConverter(
				session) : s_instance;
	}

	/*
	 * 
	 */
	@SuppressWarnings("unused")
	private SearchSession m_session;

	private Tree2FacetModelConverter(SearchSession session) {
		m_session = session;
	}

	public Facet node2facet(Node node) {
		return node.getFacet();
	}

	public FacetFacetValueTuple node2facetFacetValue(Node node) {

		FacetFacetValueTuple tuple = new FacetFacetValueTuple();
		tuple.setFacet(node.getFacet());
		tuple.setFacetValue(node2facetValue(node));

		return tuple;
	}

	public AbstractFacetValue node2facetValue(Node node) {

		AbstractFacetValue fv = null;

		if (node instanceof FacetValueNode) {

			FacetValueNode fvn = (FacetValueNode) node;

			if (node.getFacet().isDataPropertyBased()) {

				fv = new Literal();
				((Literal) fv).setDomain(fvn.getDomain());
				((Literal) fv).setNodeId(fvn.getID());
				((Literal) fv).setValue(fvn.getValue());
				((Literal) fv).setCountS(fvn.getCountS());
				((Literal) fv).setSourceExt(fvn.getSourceExtension());
				((Literal) fv).setRangeExt(fvn.getRangeExtension());
				((Literal) fv).setIsResource(false);

			} else {

				fv = new Resource();
				((Resource) fv).setDomain(fvn.getDomain());
				((Resource) fv).setNodeId(fvn.getID());
				((Resource) fv).setValue(fvn.getValue());
				((Resource) fv).setCountS(fvn.getCountS());
				((Resource) fv).setSourceExt(fvn.getSourceExtension());
				((Resource) fv).setRangeExt(fvn.getRangeExtension());
				((Resource) fv).setIsResource(true);
			}

		} else if (node instanceof DynamicNode) {

			DynamicNode dyn = (DynamicNode) node;
			fv = new FacetValueCluster();

			((FacetValueCluster) fv).setDomain(dyn.getDomain());
			((FacetValueCluster) fv).setNodeId(dyn.getID());
			((FacetValueCluster) fv).setValue(dyn.getValue());
			((FacetValueCluster) fv).setCountS(dyn.getCountS());
			((FacetValueCluster) fv).setSourceExt(dyn.getSourceExtensions());
			((FacetValueCluster) fv).setRangeExt(dyn.getRangeExtensions());

		} else if (node instanceof StaticNode) {

			StaticNode stat = (StaticNode) node;
			fv = new FacetValueCluster();

			((FacetValueCluster) fv).setDomain(stat.getDomain());
			((FacetValueCluster) fv).setNodeId(stat.getID());
			((FacetValueCluster) fv).setValue(stat.getValue());
			((FacetValueCluster) fv).setCountS(stat.getCountS());
			((FacetValueCluster) fv).setSourceExt(stat.getSourceExtensions());
			((FacetValueCluster) fv).setRangeExt(stat.getRangeExtensions());

		} else {
			s_log.error("should not be here: node '" + node + "'!");
		}

		return fv;
	}

	public List<AbstractFacetValue> nodeList2facetValueList(List<Node> nodeList) {

		List<AbstractFacetValue> fvList = new ArrayList<AbstractFacetValue>();

		for (Node node : nodeList) {

			if (node.getContent() == NodeContent.CLASS) {
				fvList.add(node2facetValue(node));
			}
		}

		return fvList;
	}
}
