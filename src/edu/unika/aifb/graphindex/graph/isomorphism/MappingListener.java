package edu.unika.aifb.graphindex.graph.isomorphism;

import java.util.Map;

public interface MappingListener {
	public void mapping(Map<String,String> mapping);

	public void mapping(VertexMapping vm);
}
