package edu.unika.aifb.multipartquery;

import edu.unika.aifb.graphindex.query.*;
import java.util.*;

public class MultiPartQuery extends StructuredQuery{

	private Map<String, StructuredQuery>  queries = new HashMap<String, StructuredQuery>();
	// Path to Mapping index
	String indexDirectory = "C:\\Users\\Christoph\\Desktop\\AIFB\\Mappings";
	
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
	
	public String getMIDXPath () {
		return indexDirectory;
	}

}
