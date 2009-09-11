package edu.unika.aifb.facetedSearch.search.datastructure;

import edu.unika.aifb.graphindex.query.Query;

public abstract class FacetQuery extends Query {

	protected FacetQuery(String name) {
		super(name);
	}
}
