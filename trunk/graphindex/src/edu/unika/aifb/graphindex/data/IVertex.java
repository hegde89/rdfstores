package edu.unika.aifb.graphindex.data;

import java.util.List;
import java.util.Set;

import edu.unika.aifb.graphindex.algorithm.rcp.Block;

public interface IVertex {

	public abstract long getId();

	public abstract int getMovedIn();
	public abstract void setMovedIn(int in);

	public Set<Long> getEdgeLabels();

	public abstract IVertex getNext();
	public abstract IVertex getPrev();
	public abstract void setNext(IVertex next);
	public abstract void setPrev(IVertex prev);

	public abstract Block getBlock();
	public abstract void setBlock(Block b);

	public abstract List<IVertex> getImage(long label);
	public abstract void addToImage(long label, IVertex v);
	public void setImage(long label, List<IVertex> image);

}