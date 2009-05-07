package edu.unika.aifb.graphindex.data;

import java.io.FileNotFoundException;
import java.util.Set;

public interface HashValueProvider {

	public Set<Long> getEdges();

	public String getValue(long hash);

	public void clearCache() throws FileNotFoundException;

}