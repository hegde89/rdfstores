/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */
package edu.unika.aifb.facetedSearch.facets.tree.model;

import it.unimi.dsi.fastutil.doubles.DoubleList;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTree;

/**
 * @author andi
 * 
 */
public interface INode {

	public boolean addLeave(double leave);

	public boolean containsClass();

	public boolean containsDataProperty();

	public boolean containsObjectProperty();

	public boolean containsProperty();

	public boolean containsRdfProperty();

	public int getContent();

	public String getDomain();

	public Facet getTopLevelFacet();

	public double getID();

	public DoubleList getLeaves();

	public String getPath();

	public int getType();

	public String getValue();

	public double getWeight();

	public boolean hasPath();

	public boolean hasSameValueAs(Object object);

	public boolean isGeneric();

	public boolean isInnerNode();

	public boolean isLeave();

	public boolean isRangeRoot();

	public boolean isRoot();

	public boolean isSubTreeRoot();

	public void setContent(int content);

	public void setDomain(String domain);

	public void setTopLevelFacet(Facet facet);

	public void setGeneric(boolean generic);

	public void setID(double id);

	public void setIsSubTreeRoot(boolean isSubTreeRoot);

	public void setLeaves(DoubleList leaves);

	public void setPath(String path);

	public void setType(int type);

	public void setValue(String label);

	public void setWeight(double weight);

	public void updatePath(FacetTree tree);
}
