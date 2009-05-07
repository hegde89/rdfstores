package edu.unika.aifb.graphindex.data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;


public interface VertexCollection extends Iterable<IVertex> {
	public IVertex addVertex(long id);
	public IVertex getVertex(long id);
	public List<IVertex> toList();
	public void loadFromComponentFile(String fileName) throws FileNotFoundException, IOException;
	public int size();
}
