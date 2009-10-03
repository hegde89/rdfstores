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

import java.util.LinkedList;
import java.util.Queue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;

/**
 * @author andi
 * 
 */
public class PathBinding extends TupleBinding<Queue<Edge>> {

	private NodeBinding m_nodeBinding;

	public PathBinding() {
		m_nodeBinding = new NodeBinding();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sleepycat.bind.tuple.TupleBinding#entryToObject(com.sleepycat.bind
	 * .tuple.TupleInput)
	 */
	@Override
	public Queue<Edge> entryToObject(TupleInput input) {

		LinkedList<Edge> edges = new LinkedList<Edge>();
		int listSize = input.readInt();
		int count = 0;

		while (++count < listSize) {

			Edge edge = new Edge();
			edge.setType(input.readInt());

			/*
			 * read nodes
			 */
			Node source = m_nodeBinding.entryToObject(input);
			Node target = m_nodeBinding.entryToObject(input);

			edge.setSource(source);
			edge.setTarget(target);

			edges.add(edge);
		}

		return edges;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sleepycat.bind.tuple.TupleBinding#objectToEntry(java.lang.Object,
	 * com.sleepycat.bind.tuple.TupleOutput)
	 */
	@Override
	public void objectToEntry(Queue<Edge> object, TupleOutput output) {

		output.writeInt(object.size());

		for (Edge edge : object) {

			Node source = edge.getSource();
			Node target = edge.getTarget();

			/*
			 * write edge type
			 */
			output.writeInt(edge.getType());

			/*
			 * write source node
			 */
			m_nodeBinding.objectToEntry(source, output);

			/*
			 * write target node
			 */
			m_nodeBinding.objectToEntry(target, output);
		}
	}
}
