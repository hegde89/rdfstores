package edu.unika.aifb.graphindex.graph.isomorphism;

import org.jgrapht.experimental.isomorphism.IsomorphismRelation;

public interface MappingListener<V,E> {
	public void mapping(IsomorphismRelation<V,E> iso);
}
