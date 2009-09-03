/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */
package edu.unika.aifb.facetedSearch.index;

import java.util.Arrays;
import java.util.List;

/**
 * @author andi
 * 
 */
public class FacetEnvironment {

	public class Dir {

		public static final String TREES = "trees";
		public static final String DISTANCES = "distances";
		public static final String VPOS = "vpos";
	}

	public class RDF {

		public static final String NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

		public static final String PROPERTY = "Property";

		public static final String TYPE = "type";

		// ------------------- Properties for ignore list
		// ---------------------------

		public static final String VALUE = "value";
		public static final String SUBJECT = "subject";
		public static final String PREDICATE = "predicate";
		public static final String OBJECT = "object";

	}

	public class RDFS {

		public static final String NAMESPACE = "http://www.w3.org/2000/01/rdf-schema#";

		public static final String CLASS = "Class";

		public static final String SUBCLASS_OF = "subClassOf";
		public static final String SUBPROPERTY_OF = "subPropertyOf";

		public static final String HAS_DOMAIN = "domain";
		public static final String HAS_RANGE = "range";

		// ------------------- Properties for ignore list
		// ---------------------------

		public static final String LABEL = "label";
		public static final String COMMENT = "comment";
		public static final String MEMBER = "member";
		public static final String FIRST = "first";
		public static final String REST = "rest";
		public static final String SEE_ALSO = "seeAlso";
		public static final String IS_DEFINED_BY = "isDefinedBy";

	}

	public static final String SOURCE = "source";

	public static final String LEAVE = "leave";

	public static final String NEW_LINE = System.getProperty("line.separator");

	public static final List<String> PROPERTIES_TO_IGNORE = Arrays
			.asList(new String[] {

					// RDFS Properties

					RDFS.NAMESPACE + RDFS.SUBPROPERTY_OF,
					RDFS.NAMESPACE + RDFS.SUBCLASS_OF,
					RDFS.NAMESPACE + RDFS.HAS_DOMAIN,
					RDFS.NAMESPACE + RDFS.HAS_RANGE,
					RDFS.NAMESPACE + RDFS.LABEL, RDFS.NAMESPACE + RDFS.COMMENT,
					RDFS.NAMESPACE + RDFS.MEMBER, RDFS.NAMESPACE + RDFS.FIRST,
					RDFS.NAMESPACE + RDFS.REST,
					RDFS.NAMESPACE + RDFS.SEE_ALSO,
					RDFS.NAMESPACE + RDFS.IS_DEFINED_BY,

					// RDF Properties

					// RDF.NAMESPACE + RDF.TYPE,
					RDF.NAMESPACE + RDF.VALUE, RDF.NAMESPACE + RDF.SUBJECT,
					RDF.NAMESPACE + RDF.PREDICATE, RDF.NAMESPACE + RDF.OBJECT

			});

}
