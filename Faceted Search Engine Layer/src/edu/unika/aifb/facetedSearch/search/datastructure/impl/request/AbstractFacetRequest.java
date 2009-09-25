package edu.unika.aifb.facetedSearch.search.datastructure.impl.request;

import edu.unika.aifb.graphindex.query.Query;

public abstract class AbstractFacetRequest extends Query {

	protected AbstractFacetRequest(String name) {
		super(name);
	}
}
