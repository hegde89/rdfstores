/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer Project. 
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
package edu.unika.aifb.facetedSearch.converter.hermes2fsl;


/**
 * @author andi
 * 
 */
public class QueryConverter {

	// public static edu.unika.aifb.graphindex.query.Query convert(
	// Object hermesQuery) {
	//
	// if (hermesQuery instanceof
	// org.apexlab.service.session.datastructure.Facet) {
	//
	// // TODO
	//
	// return null;
	// } else if (hermesQuery instanceof
	// org.apexlab.service.session.datastructure.Keywords) {
	//
	// org.apexlab.service.session.datastructure.Keywords hermesKeywords =
	// (org.apexlab.service.session.datastructure.Keywords) hermesQuery;
	//
	// Iterator<String> iter_keywords = hermesKeywords.getWordList()
	// .iterator();
	// String keywords = "";
	//
	// while (iter_keywords.hasNext()) {
	// keywords += " " + iter_keywords.next();
	// }
	//
	// return new edu.unika.aifb.graphindex.query.KeywordQuery(
	// Environment.DEFAULT_QUERY_NAME, keywords);
	//
	// } else if (hermesQuery instanceof
	// org.apexlab.service.session.datastructure.QueryGraph) {
	//
	// // TODO
	// return null;
	// } else if (hermesQuery instanceof
	// org.apexlab.service.session.datastructure.Suggestion) {
	//
	// // TODO
	// return null;
	// } else {
	// return null;
	// }
	// }
}
