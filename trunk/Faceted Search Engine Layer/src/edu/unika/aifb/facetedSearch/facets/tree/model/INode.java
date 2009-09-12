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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeContent;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeType;

/**
 * @author andi
 * 
 */
public interface INode extends Serializable {

	public void addRangeExtension(String extension);

	public void addRangeExtensions(List<String> extensions);

	public void addType(NodeType type);

	public void addTypes(Collection<NodeType> collection);

	public NodeContent getContent();

	public String getDomain();

	public String getFacet();

	public double getID();

	public String getPath();

	public int getPathHashValue();

	public List<String> getRangeExtensions();

	public List<String> getSourceExtensions();

	// public boolean hasChildren();

	public HashSet<NodeType> getTypes();

	public String getValue();

	public double getWeight();

	public boolean hasPath();

	// public Set<INode> getChildren();
	public boolean hasPathHashValue();

	public boolean isEndPoint();

	public boolean isInnerNode();

	public boolean isLeave();

	public boolean isRangeRoot();

	public boolean isRoot();

	public void removeType(NodeType type);

	public void setContent(NodeContent content);

	public void setDomain(String domain);

	public void setFacet(String facet);

	public void setID(double id);

	public void setPath(String path);

	public void setPathHashValue(int pathHashValue);

	public void setRangeExtensions(List<String> extensions);

	public void setSourceExtensions(List<String> sourceExtensions);

	public void setTypes(HashSet<NodeType> types);

	public void setValue(String label);

	public void setWeight(double weight);
}
