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

/**
 * @author andi
 * 
 */
public class Environment {

	/*
	 * keys for properties
	 */
	public static final String INDEX_DIRECTORY = "idx.dir";
	public static final String RANKING_ENABLED = "ranking.enabled";
	public static final String ACTION = "action";
	public static final String FILES = "files";
	public static final String CREATE_DATA_INDEX = "createDataIndex";
	public static final String CREATE_STRUCTURE_INDEX = "createStructureIndex";
	public static final String CREATE_KEYWORD_INDEX = "createKeywordIndex";
	public static final String NEIGHBORHOOD_SIZE = "neighborhood.size";
	public static final String STRUCTURE_INDEX_PATH_LENGTH = "structureIndex.pathLength";
	public static final String STRUCTURE_BASED_DATA_PARTIONING = "structureBased.dataPartitioning";
	public static final String ONTO_LANGUAGE = "onto.language";

	/*
	 * options for property 'action'
	 */
	public static final String CREATE_STORE = "create";
	public static final String LOAD_STORE = "load";

	/*
	 * Default Values
	 */
	public static String DEFAULT_INDEX_DIRECTORY = "D:/Data/DA/idx";
	public static double DEFAULT_WEIGHT = -1.0;
	public static boolean DEFAULT_RANKING_ENABLED = false;
	public static int DEFAULT_DEPTH_K = 3;
	public static String DEFAULT_LITERAL_DELIM = "^";
	public static int MAX_SESSIONS = 1;

	/*
	 * evaluator types
	 */
	public enum EvaluatorType {
		VPEvaluator, CombinedQueryEvaluator, KeywordQueryEvaluator
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
		STRING, TIME, NUMERICAL, DATE
	}
	
	/*
	 * xml stuff
	 */
	public class XML {
		
		public class DataType{
			public static final String STRING = "xsd:string";
			public static final String NUMERICAL = "xsd:numerical";
			public static final String DATE = "xsd:date";
			public static final String TIME = "xsd:time";
		}
		
		public static final String NAMESPACE = "http://www.w3.org/2001/XMLSchema";
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
}
