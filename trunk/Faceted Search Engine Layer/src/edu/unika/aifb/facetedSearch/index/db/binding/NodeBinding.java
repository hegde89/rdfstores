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
package edu.unika.aifb.facetedSearch.index.db.binding;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;

/**
 * @author andi
 * 
 */
public class NodeBinding extends TupleBinding<Node> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sleepycat.bind.tuple.TupleBinding#entryToObject(com.sleepycat.bind
	 * .tuple.TupleInput)
	 */
	@Override
	public Node entryToObject(TupleInput input) {

		Node node = new Node();

		/*
		 * read node content
		 */
		node.setID(input.readDouble());
		node.setContent(input.readInt());
		node.setValue(input.readString());
		node.setType(input.readInt());
		node.setPath(input.readString());

		/*
		 * read facet
		 */
		Facet facet = new Facet();
		facet.setUri(input.readString());
		facet.setNodeId(input.readDouble());
		facet.setType(input.readInt());
		facet.setDataType(input.readInt());
		facet.setLabel(input.readString());
		
		node.setFacet(facet);

		return node;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sleepycat.bind.tuple.TupleBinding#objectToEntry(java.lang.Object,
	 * com.sleepycat.bind.tuple.TupleOutput)
	 */
	@Override
	public void objectToEntry(Node object, TupleOutput output) {

		/*
		 * node content
		 */
		output.writeDouble(object.getID());
		output.writeInt(object.getContent());
		output.writeString(object.getValue());
		output.writeInt(object.getType());
		output.writeString(object.getPath());

		/*
		 * write facet
		 */
		Facet facet = object.getFacet();
		output.writeString(facet.getUri());
		output.writeDouble(facet.getNodeId());
		output.writeInt(facet.getType());
		output.writeInt(facet.getDataType());
		output.writeString(facet.getLabel());
		
	}
}
