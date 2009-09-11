/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer project. 
 * 
 * Faceted Search Layer Project is free software: you can redistribute
 * it and/or modify it under the terms of the GNU General Public License, 
 * version 2 as published by the Free Software Foundation. 
 *  
 * Faceted Search Layer Project is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 * GNU General Public License for more details. 
 *  
 * You should have received a copy of the GNU General Public License 
 * along with Faceted Search Layer Project.  If not, see <http://www.gnu.org/licenses/>. 
 */

package edu.unika.aifb.facetedSearch;

import java.util.Arrays;
import java.util.HashSet;

import org.openrdf.model.vocabulary.XMLSchema;

/**
 * @author andi
 * 
 */
public class FacetEnvironment {

	/*
	 * keys for properties
	 */
	public static final String INDEX_DIRECTORY = "idx.dir";
	public static final String RANKING_ENABLED = "ranking.enabled";
	public static final String FACETS_ENABLED = "facets.enabled";
	public static final String ACTION = "action";
	public static final String FILES = "files";
	public static final String CREATE_DATA_INDEX = "createDataIndex";
	public static final String IGNORE_DATATYPES = "ignoreDataTypes";
	public static final String CREATE_STRUCTURE_INDEX = "createStructureIndex";
	public static final String CREATE_KEYWORD_INDEX = "createKeywordIndex";
	public static final String NEIGHBORHOOD_SIZE = "neighborhood.size";
	public static final String STRUCTURE_INDEX_PATH_LENGTH = "structureIndex.pathLength";
	public static final String STRUCTURE_BASED_DATA_PARTIONING = "structureBased.dataPartitioning";
	public static final String CREATE_DATA_EXTENSIONS = "createDataExtensions";
	public static final String ONTO_LANGUAGE = "onto.language";

	/*
	 * options for property 'action'
	 */
	public static final String CREATE_STORE = "create";
	public static final String LOAD_STORE = "load";

	/*
	 * Default Values
	 */
	
	public static class DefaultValue {
		
		public static String INDEX_DIRECTORY = "D:/Data/DA/idx";
		public static double WEIGHT = -1.0;
		public static boolean RANKING_ENABLED = false;
		public static int DEPTH_K = 3;
		public static String LITERAL_DELIM = Character.toString((char) 94);
		public static int MAX_SESSIONS = 1;
		public static int MAXLENGTH_STRING = 15;
		public static int NUMBER_OF_RESULTS_PER_PAGE = 20;
	}
	


	/*
	 * evaluator types
	 */
	public enum EvaluatorType {
		StructuredQueryEvaluator, KeywordQueryEvaluator, FacetQueryEvaluator, HybridQueryEvaluator
	}

	/*
	 * node types
	 */
	public enum NodeType {
		NODE, ROOT, BLANK_NODE, FACET_VALUE, STATIC_FACET_VALUE_CLUSTER, STATIC_FACET_VALUE_CLUSTER_LEAVE, DYNAMIC_FACET_VALUE_CLUSTER
	}

	/*
	 * abstract-object types
	 */
	public enum ObjectType {
		LITERAL, INDIVIDUAL
	}

	/*
	 * abstract-object types
	 */
	public class ResultItemType {

		public static final String LITERAL = "literal";
		public static final String INDIVIDUAL = "individual";

	}

	/*
	 * data types
	 */
	public enum DataType {
		STRING, TIME, NUMERICAL, DATE, DATE_TIME, UNKNOWN
	}

	/*
	 * xml stuff
	 */
	public static class XMLS {

		public static final HashSet<String> NUMERICAL_TYPES = new HashSet<String>(
				Arrays.asList(new String[] {

				XMLSchema.DECIMAL.stringValue(),

				XMLSchema.INTEGER.stringValue(),
						XMLSchema.NON_POSITIVE_INTEGER.stringValue(),
						XMLSchema.LONG.stringValue(),
						XMLSchema.NON_NEGATIVE_INTEGER.stringValue(),
						XMLSchema.NEGATIVE_INTEGER.stringValue(),
						XMLSchema.INT.stringValue(),
						XMLSchema.UNSIGNED_LONG.stringValue(),
						XMLSchema.POSITIVE_INTEGER.stringValue(),
						XMLSchema.SHORT.stringValue(),
						XMLSchema.UNSIGNED_INT.stringValue(),
						XMLSchema.BYTE.stringValue(),
						XMLSchema.UNSIGNED_SHORT.stringValue(),
						XMLSchema.UNSIGNED_BYTE.stringValue(),

						XMLSchema.FLOAT.stringValue(),
						XMLSchema.DOUBLE.stringValue(),

				}));

		public static final HashSet<String> STRING_TYPES = new HashSet<String>(
				Arrays.asList(new String[] {

				XMLSchema.STRING.stringValue(),
						XMLSchema.NORMALIZEDSTRING.stringValue(),
						XMLSchema.TOKEN.stringValue(),
						XMLSchema.LANGUAGE.stringValue(),
						XMLSchema.NAME.stringValue(),
						XMLSchema.NMTOKEN.stringValue(),
						XMLSchema.NCNAME.stringValue(),
						XMLSchema.NMTOKENS.stringValue(),
						XMLSchema.ID.stringValue(),
						XMLSchema.IDREF.stringValue(),
						XMLSchema.ENTITY.stringValue(),
						XMLSchema.IDREFS.stringValue(),
						XMLSchema.ENTITIES.stringValue(),

				}));
	}

	/*
	 * evaluator types
	 */

	public static class OntologyLanguage {

		public static final String N_3 = "nt";
		public static final String RDF = "rdf";
	}

	/*
	 * Stopwords for Util.java
	 */

	public static final String[] stopWords = {};

	public class Dir {

		public static final String TREES = "trees";
		public static final String DISTANCES = "distances";
		public static final String VPOS = "vpos";
	}

	public class RDF {

		public static final String NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
		public static final String PROPERTY = "Property";
		public static final String TYPE = "type";

		// ------------------- Properties for ignore list -------------------

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

		// ------------------- Properties for ignore list -------------------

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

	public static final HashSet<String> PROPERTIES_TO_IGNORE = new HashSet<String>(
			Arrays.asList(new String[] {

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

			}));
}
