package edu.unika.aifb.graphindex.preprocessing;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import edu.unika.aifb.graphindex.data.IVertex;

public interface IVertexListProvider {

	public abstract List<IVertex> nextComponent() throws IOException;

	public abstract File getComponentFile();

	public abstract Set<String> getEdges();

}