package edu.unika.aifb.graphindex.algorithm.rcp.generic;

import java.util.Collection;
import java.util.List;

public interface GVertex<V,E> {
	public V getLabel();
	
	public Collection<E> getEdgeLabels();

	public List<GVertex<V,E>> getImage(E label);
	public void setImage(E label, List<GVertex<V,E>> image);
	public void addToImage(E label, GVertex<V,E> vertex);

	public int getMovedIn();
	public void setMovedIn(int in);

	public GVertex<V,E> getNext();
	public GVertex<V,E> getPrev();
	public void setNext(GVertex<V,E> next);
	public void setPrev(GVertex<V,E> prev);
	
	public Block<V,E> getBlock();
	public void setBlock(Block<V,E> b);
}