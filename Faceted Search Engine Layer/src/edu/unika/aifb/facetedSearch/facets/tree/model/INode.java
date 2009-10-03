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

import java.io.Serializable;
import java.util.HashSet;

import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;

/**
 * @author andi
 * 
 */
public interface INode extends Serializable {

	public int getContent();

	public String getDomain();

	public Facet getFacet();

	public double getID();

	public String getPath();

	public int getPathHashValue();

	public HashSet<String> getRangeExtensions();

	public HashSet<String> getSourceExtensions();

	public int getType();

	public String getValue();

	public double getWeight();

	public boolean hasPath();

	public boolean hasPathHashValue();

	public boolean hasSameValueAs(Object object);

	public boolean isInnerNode();

	public boolean isLeave();

	public boolean isRangeRoot();

	public boolean isRoot();

	public Facet makeFacet(String uri, int ftype, int dtype);

	public void setContent(int content);

	public void setDomain(String domain);

	public void setFacet(Facet facet);

	public void setID(double id);

	public void setPath(String path);

	public void setPathHashValue(int pathHashValue);

	public void setType(int type);

	public void setValue(String label);

	public void setWeight(double weight);
}
