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

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.converter.AbstractConverter;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractFacetValueCluster;
import edu.unika.aifb.facetedSearch.facets.model.impl.DateTimeLiteralFacetValueCluster;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.model.impl.LiteralFacetValueCluster;
import edu.unika.aifb.facetedSearch.facets.model.impl.Resource;
import edu.unika.aifb.facetedSearch.facets.model.impl.ResourceFacetValueCluster;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.DynamicClusterNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.SingleValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticClusterNode;
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
	private SearchSession m_session;

	public Tree2FacetModelConverter(SearchSession session) {
		m_session = session;
	}

	public Facet node2CurrentFacet(Node node) {

		Facet facet = node.getCurrentFacet();
		facet.setDomain(node.getDomain());
		facet.setNodeId(node.getID());
		facet.setContent(node.getContent());
		facet.setWeight(node.getWeight());

		if (node instanceof StaticClusterNode) {
			StaticClusterNode stat = (StaticClusterNode) node;
			facet.setCountS(m_session.getCache().getCountS(stat));
		}

		return facet;
	}

	public FacetFacetValueTuple node2facetFacetValue(Node node) {

		FacetFacetValueTuple tuple = new FacetFacetValueTuple();
		tuple.setTopLevelFacet(node.getTopLevelFacet());
		tuple.setCurrentFacet(node.getCurrentFacet());
		tuple.setFacetValue(node2facetValue(node));

		return tuple;
	}

	public AbstractFacetValue node2facetValue(Node node) {

		AbstractFacetValue fv = null;

		if (node instanceof SingleValueNode) {

			SingleValueNode fvn = (SingleValueNode) node;

			if (node.getCurrentFacet().isDataPropertyBased()) {

				fv = new Literal();
				((Literal) fv).setDomain(fvn.getDomain());
				((Literal) fv).setNodeId(fvn.getID());
				((Literal) fv).setValue(fvn.getValue());
				((Literal) fv).setCountS(m_session.getCache()
						.getCountS4FacetValueNode(fvn));
				((Literal) fv).setIsResource(false);
				((Literal) fv).setContent(fvn.getContent());
				((Literal) fv).setType(fvn.getType());

			} else {

				fv = new Resource();
				((Resource) fv).setDomain(fvn.getDomain());
				((Resource) fv).setNodeId(fvn.getID());
				((Resource) fv).setValue(fvn.getValue());
				((Resource) fv).setCountS(m_session.getCache()
						.getCountS4FacetValueNode(fvn));
				((Resource) fv).setContent(fvn.getContent());
				((Resource) fv).setIsResource(true);
				((Resource) fv).setType(fvn.getType());
			}

		} else if (node instanceof DynamicClusterNode) {

			DynamicClusterNode dyn = (DynamicClusterNode) node;

			if ((dyn.getCurrentFacet().getDataType() == FacetEnvironment.DataType.DATE)
					|| (dyn.getCurrentFacet().getDataType() == FacetEnvironment.DataType.DATE_TIME)
					|| (dyn.getCurrentFacet().getDataType() == FacetEnvironment.DataType.TIME)) {

				fv = new DateTimeLiteralFacetValueCluster();
				((DateTimeLiteralFacetValueCluster) fv).setHasCalChildren(dyn
						.hasCalChildren());
				((DateTimeLiteralFacetValueCluster) fv).setCalPrefix(dyn
						.getCalPrefix());
			} else {

				fv = new LiteralFacetValueCluster();
				((LiteralFacetValueCluster) fv).setLeftClusterBorder(dyn
						.getLeftBorder());
				((LiteralFacetValueCluster) fv).setRightClusterBorder(dyn
						.getRightBorder());
			}

			((AbstractFacetValueCluster) fv).setDomain(dyn.getDomain());
			((AbstractFacetValueCluster) fv).setNodeId(dyn.getID());
			((AbstractFacetValueCluster) fv).setValue(dyn.getValue());
			((AbstractFacetValueCluster) fv).setCountS(m_session.getCache()
					.getCountS4DynNode(dyn));
			((AbstractFacetValueCluster) fv).setContent(dyn.getContent());
			((AbstractFacetValueCluster) fv).setType(dyn.getType());

		} else if (node instanceof StaticClusterNode) {

			StaticClusterNode stat = (StaticClusterNode) node;

			if (stat.getCurrentFacet().isDataPropertyBased()) {

				if ((stat.getCurrentFacet().getDataType() == FacetEnvironment.DataType.DATE)
						|| (stat.getCurrentFacet().getDataType() == FacetEnvironment.DataType.DATE_TIME)
						|| (stat.getCurrentFacet().getDataType() == FacetEnvironment.DataType.TIME)) {

					fv = new DateTimeLiteralFacetValueCluster();
					((DateTimeLiteralFacetValueCluster) fv)
							.setHasCalChildren(true);
					((DateTimeLiteralFacetValueCluster) fv).setCalPrefix(null);

				} else {

					fv = new LiteralFacetValueCluster();
					((LiteralFacetValueCluster) fv).setLeftClusterBorder(null);
					((LiteralFacetValueCluster) fv).setRightClusterBorder(null);
				}

			} else {

				fv = new ResourceFacetValueCluster();
			}

			((AbstractFacetValueCluster) fv).setDomain(stat.getDomain());
			((AbstractFacetValueCluster) fv).setNodeId(stat.getID());
			((AbstractFacetValueCluster) fv).setValue(stat.getValue());
			((AbstractFacetValueCluster) fv).setCountS(m_session.getCache()
					.getCountS4StaticNode(stat));
			((AbstractFacetValueCluster) fv).setContent(stat.getContent());
			((AbstractFacetValueCluster) fv).setType(stat.getType());
			((AbstractFacetValueCluster) fv).setIsRangeRoot(stat.isRangeRoot());

		} else {
			s_log.error("should not be here: node '" + node + "'!");
		}

		return fv;
	}

	public Facet node2TopLevelFacet(Node node) {

		Facet facet = node.getTopLevelFacet();
		facet.setDomain(node.getDomain());
		facet.setNodeId(node.getID());
		facet.setContent(node.getContent());
		facet.setWeight(node.getWeight());

		if (node instanceof StaticClusterNode) {
			StaticClusterNode stat = (StaticClusterNode) node;
			facet.setCountS(m_session.getCache().getCountS(stat));
		}

		return facet;
	}

	// public List<Facet> nodeList2facetList(List<Node> nodeList) {
	//
	// List<Facet> facetList = new ArrayList<Facet>();
	//
	// for (Node node : nodeList) {
	// facetList.add(node2facet(node));
	// }
	//
	// return facetList;
	// }

	public List<AbstractFacetValue> nodeList2facetValueList(List<Node> nodeList) {

		List<AbstractFacetValue> fvList = new ArrayList<AbstractFacetValue>();

		for (Node node : nodeList) {

			// if (node.getContent() == NodeContent.CLASS) {
			fvList.add(node2facetValue(node));
			// }
		}

		return fvList;
	}
}