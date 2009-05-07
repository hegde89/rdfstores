package edu.unika.aifb.graphindex.data;

import java.util.List;
import java.util.Set;

import edu.unika.aifb.graphindex.algorithm.rcp.Block;

public interface IVertex {

	public long getId();

	public int getMovedIn();
	public void setMovedIn(int in);

	public Set<Long> getEdgeLabels();

	public IVertex getNext();
	public IVertex getPrev();
	public void setNext(IVertex next);
	public void setPrev(IVertex prev);

	public Block getBlock();
	public void setBlock(Block b);

	public List<IVertex> getImage(long label);
	public void addToImage(long label, IVertex v);
	public void setImage(long label, List<IVertex> image);

}