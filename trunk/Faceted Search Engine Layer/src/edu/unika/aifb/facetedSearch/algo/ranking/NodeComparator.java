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
package edu.unika.aifb.facetedSearch.algo.ranking;

import java.util.Comparator;

import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;

/**
 * @author andi
 * 
 */
public class NodeComparator implements Comparator<Node> {

	public int compare(Node o1, Node o2) {

		Double d1 = o1.getWeight();
		Double d2 = o2.getWeight();

		return d1.compareTo(d2);
	}
}
