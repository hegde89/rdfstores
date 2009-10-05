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

	public static class CacheName {

		public static final String SOURCES = "src";
		public static final String DISTANCE = "dis";
		public static final String SUBJECTS = "subj";
		public static final String OBJECTS = "obj";
		
	}

	public static class DatabaseName {

		/*
		 * other
		 */
		public static final String CLASS = "class_db";

		/*
		 * indices
		 */
		public static final String PATH = "path_db";
		public static final String LEAVE = "leave_db";
		public static final String OBJECT = "object_db";

		/*
		 * fsl cache
		 */
		public static final String FS_CACHE = "fs_cache_db";
		public static final String FRES_CACHE = "fres_cache_db";
		public static final String FLIT_CACHE = "flit_cache_db";

		/*
		 * tree delegator
		 */
		public static final String FTREE_CACHE = "ftree_cache_db";
		public static final String FPAGE_CACHE = "fpage_cache_db";

		/*
		 * history manager
		 */
		public static final String FHIST_CACHE = "fhist_cache_db";

		/*
		 * facet index builder helper
		 */
		public static final String FH_CACHE = "fh_cache_db";
	}

	public static class DataType {

		public static final int NOT_SET = -1;

		public static final int STRING = 1;
		public static final int TIME = 2;
		public static final int NUMERICAL = 3;
		public static final int DATE = 4;
		public static final int DATE_TIME = 5;
		public static final int UNKNOWN = 6;
	}

	public static class DefaultValue {

		public static String INDEX_DIRECTORY = "D:/Data/DA/idx";
		public static double WEIGHT = -1.0;
		public static boolean RANKING_ENABLED = false;
		public static int DEPTH_K = 3;
		public static String LITERAL_DELIM = Character.toString((char) 94);
		public static int MAX_SESSIONS = 1;
		public static int MAXLENGTH_STRING = 15;
		public static int NUM_OF_RESITEMS_PER_PAGE = 20;
		public static long PRELOAD_TIME = 4000;
		public static int NUM_OF_CHILDREN_PER_NODE = 6;
		public static int RANKING_METRIC = RankingMetricType.COUNT_S;
		public static int LITERAL_CLUSTERER = LiteralClustererType.SIMPLE;
		public static int SIMPLE_CACHE_SIZE = 5000;

	}

	public static class EdgeType {

		public static final int SUBPROPERTY_OF = 1;
		public static final int SUBCLASS_OF = 2;
		public static final int HAS_RANGE = 3;
		public static final int CONTAINS = 4;

	}

	public static class EndPointType {

		public static final int DATA_PROPERTY = 1;
		public static final int OBJECT_PROPERTY = 2;
		public static final int RDF_PROPERTY = 3;
	}

	public enum EvaluatorType {
		StructuredQueryEvaluator, KeywordQueryEvaluator, FacetQueryEvaluator, HybridQueryEvaluator, ChangePageEvaluator
	}

	public static class FacetType {

		public static final int NOT_SET = -1;

		public static final int DATAPROPERTY_BASED = 1;
		public static final int OBJECT_PROPERTY_BASED = 2;
		public static final int RDF_PROPERTY_BASED = 3;
	}

	public static class Keys {

		public static final String RESULT_SET = "re";
		// public static final String RANGEROOT_PATH = "rr";
		// public static final String ROOT_PATH = "r";
	}

	public static class LiteralClustererType {

		public static final int SIMPLE = 1;
		public static final int SINGLE_LINKAGE = 2;

	}

	public static class LucenceIndexName {

		public static final String LEAVE = "leave";
		public static final String OBJECT = "obj";
	}

	public static class NodeContent {

		public static final int TYPE_PROPERTY = 1;
		public static final int DATA_PROPERTY = 2;
		public static final int OBJECT_PROPERTY = 3;
		public static final int CLASS = 4;
	}

	public static class NodeType {

		public static final int ROOT = 1;
		public static final int RANGE_ROOT = 2;
		public static final int INNER_NODE = 3;
		public static final int LEAVE = 4;
	}

	public static class OntologyLanguage {

		public static final String N_3 = "nt";
		public static final String RDF = "rdf";
	}

	public static class Property {

		public static final String INDEX_DIRECTORY = "idx.dir";
		public static final String RANKING_ENABLED = "ranking.enabled";
		public static final String FACETS_ENABLED = "facets.enabled";
		public static final String FILES = "files";
		public static final String CREATE_DATA_INDEX = "createDataIndex";
		public static final String CREATE_KEYWORD_INDEX = "createKeywordIndex";
		public static final String IGNORE_DATATYPES = "ignoreDataTypes";
		public static final String CREATE_STRUCTURE_INDEX = "createStructureIndex";
		public static final String NEIGHBORHOOD_SIZE = "neighborhood.size";
		public static final String STRUCTURE_INDEX_PATH_LENGTH = "structureIndex.pathLength";
		public static final String STRUCTURE_BASED_DATA_PARTIONING = "structureBased.dataPartitioning";
		public static final String CREATE_DATA_EXTENSIONS = "createDataExtensions";
		public static final String ONTO_LANGUAGE = "onto.language";
		public static final String CACHE_DIR = "cache.config";

	}

	public static class RankingMetricType {

		public static final int COUNT_S = 1;
		public static final int COUNT_FV = 2;
		public static final int BROWSE_ABILITY = 3;

	}

	public static class RDF {

		public static final String NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
		public static final String PROPERTY = "Property";
		public static final String TYPE = "type";

		// ------------------- Properties for ignore list -------------------

		public static final String VALUE = "value";
		public static final String SUBJECT = "subject";
		public static final String PREDICATE = "predicate";
		public static final String OBJECT = "object";

	}

	public static class RDFS {

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

	/*
	 * abstract-object types
	 */
	public static class ResultItemType {

		public static final String LITERAL = "literal";
		public static final String INDIVIDUAL = "individual";

	}

	/*
	 * keys for properties
	 */

	public static class StoreAction {

		public static final String ACTION = "action";
		public static final String CREATE_STORE = "create";
		public static final String LOAD_STORE = "load";

	}

	/*
	 * xml stuff
	 */
	public static class XMLS {

		public static final HashSet<String> NUMERICAL_TYPES = new HashSet<String>(
				Arrays.asList(new String[]{

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
				Arrays.asList(new String[]{

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

	public static final String NEW_LINE = System.getProperty("line.separator");

	public static final HashSet<String> PROPERTIES_TO_IGNORE = new HashSet<String>(
			Arrays.asList(new String[]{

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
