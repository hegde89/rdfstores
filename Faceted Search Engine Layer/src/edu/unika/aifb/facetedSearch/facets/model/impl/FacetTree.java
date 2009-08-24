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
package edu.unika.aifb.facetedSearch.facets.model.impl;

import java.util.Iterator;
import java.util.Set;

import org.jgrapht.graph.DefaultDirectedWeightedGraph;

import edu.unika.aifb.facetedSearch.facets.model.IFacetTree;
import edu.unika.aifb.facetedSearch.facets.model.INode;

/**
 * @author andi
 * 
 */
public class FacetTree<V, E> extends DefaultDirectedWeightedGraph<V, E>
		implements IFacetTree<V, E> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5087213640961764186L;

	private Root m_root;

	public FacetTree(Class<? extends E> edgeClass) {
		super(edgeClass);
	}

	public FacetTree(Class<? extends E> edgeClass, Root root) {
		super(edgeClass);
		this.m_root = root;
	}

	@SuppressWarnings("unchecked")
	public V getNodeById(Class clazz, double id) {

		Iterator<V> iter = this.vertexSet().iterator();
		V node = null;

		while (iter.hasNext()) {

			if ((node = iter.next()).getClass() == clazz) {

				if (((INode) node).getID() == id) {
					break;
				} else {
					node = null;
				}
			}
		}

		return node;
	}

	public V getNodeById(double id) {

		Iterator<V> iter = this.vertexSet().iterator();
		V node = null;

		while (iter.hasNext()) {

			if ((node = iter.next()) instanceof INode) {

				if (((INode) node).getID() == id) {
					break;
				} else {
					node = null;
				}
			}
		}

		return null;
	}

	public Set<V> getNodes(V v) {

		// if (this.m_modified || this.m_staticLeaves.isEmpty()) {
		//
		// this.m_staticLeaves.clear();
		//
		// Set<V> vertices = this.vertexSet();
		//
		// for (V vertex : vertices) {
		// if (vertex instanceof StaticFacetValueClusterLeave) {
		// this.m_staticLeaves
		// .add((StaticFacetValueClusterLeave) vertex);
		// }
		// }
		//
		// }

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.facets.api.IFacetTree#getRoot()
	 */
	public Root getRoot() {
		return this.m_root;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.api.IFacetTree#setRoot(edu.unika.
	 * aifb.facetedSearch.facets.api.IRoot)
	 */
	public void setRoot(Root root) {
		this.m_root = root;
	}
}
