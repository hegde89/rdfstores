package edu.unika.aifb.multipartquery;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.query.*;
import java.util.*;

public class MultiPartQuery extends StructuredQuery{

	private Map<String, StructuredQuery>  queries = new HashMap<String, StructuredQuery>();
	
	public MultiPartQuery(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	
	public void addQuery(String datasource, StructuredQuery query) {
		queries.put(datasource, query);
		Table<String> test = new Table<String>();
	}
	
	public Map<String, StructuredQuery> getMap() {
		return queries;
	}

}
