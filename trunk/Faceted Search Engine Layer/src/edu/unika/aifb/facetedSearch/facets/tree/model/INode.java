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

import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.FacetType;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeContent;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeType;

/**
 * @author andi
 * 
 */
public interface INode extends Serializable {

	// public void addRangeExtension(String extension);
	//
	// public void addRangeExtensions(Collection<String> extensions);
	//
	// public void addRangeExtensions(String extensions);
	//
	// public void addSourceExtension(String extension);
	//
	// public void addSourceExtensions(Collection<String> extensions);
	//
	// public void addSourceExtensions(String extensions);

	public NodeContent getContent();

	public String getDomain();

	// public void addSourceIndivdiual(String ind);

	// public SearchSessionCache getCache();

	public Facet getFacet();

	public double getID();

	// public void addType(NodeType type);
	//
	// public void addTypes(Collection<NodeType> collection);

	public String getPath();

	public int getPathHashValue();

	public HashSet<String> getRangeExtensions();

	public HashSet<String> getSourceExtensions();

	public NodeType getType();

	public String getValue();

	// public HashSet<String> getSourceIndivdiuals() throws DatabaseException,
	// IOException;

	public double getWeight();

	// public boolean hasChildren();

	public boolean hasPath();

	// public Set<INode> getChildren();
	public boolean hasPathHashValue();

	public boolean hasSameValueAs(Object object);

	public boolean isInnerNode();

	public boolean isLeave();

	// public boolean isEndPoint();

	public boolean isRangeRoot();

	public boolean isRoot();

	public Facet makeFacet(String uri, FacetType ftype, DataType dtype);

	// public void setCache(SearchSessionCache cache);

	// public void removeType(NodeType type);

	public void setContent(NodeContent content);

	public void setDomain(String domain);

	public void setFacet(Facet facet);

	public void setID(double id);

	public void setPath(String path);

	public void setPathHashValue(int pathHashValue);

	// public void setRangeExtensions(HashSet<String> extensions);
	//
	// public void setSourceExtensions(HashSet<String> sourceExtensions);

	public void setType(NodeType type);

	public void setValue(String label);

	public void setWeight(double weight);
}
