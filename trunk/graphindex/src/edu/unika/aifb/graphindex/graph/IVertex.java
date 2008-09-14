package edu.unika.aifb.graphindex.graph;

import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.algorithm.RCPFast.Block;

public interface IVertex {

	public abstract int getMovedIn();

	public abstract void setMovedIn(int in);

	public abstract int getClearedIn();

	public abstract void setClearedIn(int in);

	public abstract long getId();

	public int getInfo(long label);
	public int getSInfo(long label);
	public void setInfo(long label, int val);
	public void setSInfo(long label, int val);
	public void incInfo(long label);
	public void incSInfo(long label);
	public void clearInfo();

	public Set<Long> getEdgeLabels();

	public abstract List<IVertex> getImage(long label);

	public abstract IVertex getNext();

	public abstract IVertex getPrev();

	public abstract void setNext(IVertex next);

	public abstract void setPrev(IVertex prev);

	public abstract Block getBlock();

	public abstract void setBlock(Block b);

	public abstract void addToImage(long label, IVertex v);

}