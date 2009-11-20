package edu.unika.aifb.multipartquery;

import edu.unika.aifb.MappingIndex.MappingIndex;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.*;
import edu.unika.aifb.graphindex.storage.StorageException;

import java.io.IOException;
import java.util.*;

public class MultiPartQuery extends StructuredQuery{

	private Map<String, StructuredQuery>  queries = new HashMap<String, StructuredQuery>();
	// Path to Mapping index
	MappingIndex midx = null;
	
	public MultiPartQuery(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	
	public void addQuery(String datasource, StructuredQuery query) {
		queries.put(datasource, query);
	}
	
	public Map<String, StructuredQuery> getMap() {
		return queries;
	}
	
	public MappingIndex getMappingIndex () {
		return midx;
	}
	
	public void setMappingIndex (String path) {
		try {
			midx = new MappingIndex(path, new IndexReader(new IndexDirectory(path)).getIndexConfiguration());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
