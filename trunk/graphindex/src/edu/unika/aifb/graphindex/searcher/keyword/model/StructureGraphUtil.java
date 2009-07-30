package edu.unika.aifb.graphindex.searcher.keyword.model;

/**
 * Copyright (C) 2009 Lei Zhang (beyondlei at gmail.com)
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
 */

import org.jgrapht.graph.Pseudograph;

import edu.unika.aifb.graphindex.model.impl.Attribute;
import edu.unika.aifb.graphindex.model.impl.Entity;
import edu.unika.aifb.graphindex.model.impl.NamedConcept;
import edu.unika.aifb.graphindex.model.impl.Relation;


public class StructureGraphUtil {
	public static String[] stopWords = {"Property-3A","Category-3A","User-3A"};
	
	/**
	 * Get the uri of a summary graph element.
	 * @param ele
	 * @return
	 */
//	public static String getResourceUri(SummaryGraphElement ele) {
//		if(ele.getType() == SummaryGraphElement.CONCEPT) {
//			return  ((NamedConcept)ele.getResource()).getUri();
//		}
//		else if(ele.getType() == SummaryGraphElement.ATTRIBUTE) {
//			return ((Attribute)ele.getResource()).getUri();
//		}
//		else if(ele.getType() == SummaryGraphElement.RELATION) {
//			return ((Relation)ele.getResource()).getUri();
//		}
//		else if(ele.getType() == SummaryGraphElement.ENTITY) {
//			return ((Entity)ele.getResource()).getUri();
//		}
//		else {
//			return ele.getResource().getLabel();
//		}
//	}
	
	public static String removeNum(String line) {
		String res = line.replaceFirst("\\u0028.*\\u0029", "");

		return res;
	}
	
//	public static SummaryGraphElement getGraphElementWithoutNum(SummaryGraphElement ele){
//		if(ele.getType() == SummaryGraphElement.ATTRIBUTE || ele.getType() == SummaryGraphElement.RELATION){
//			String uri = removeNum(getResourceUri(ele));
//			SummaryGraphElement element = null;
//			if(ele.getType() == SummaryGraphElement.ATTRIBUTE){
//				element = new SummaryGraphElement(new Attribute(uri), SummaryGraphElement.ATTRIBUTE);
//				element.setDatasource(ele.getDatasource());
//				element.setEF(ele.getEF());
//				element.setTotalCost(ele.getTotalCost());
//				element.setMatchingScore(ele.getMatchingScore());
//				return element;
//			}
//			else {
//				element = new SummaryGraphElement(new Relation(uri), SummaryGraphElement.RELATION);
//				element.setDatasource(ele.getDatasource());
//				element.setEF(ele.getEF());
//				element.setTotalCost(ele.getTotalCost());
//				element.setMatchingScore(ele.getMatchingScore());
//				return element;
//			}
//		}
//		else {
//			return ele;
//		}
//	}
//	
//	public static SummaryGraphEdge getGraphEdgeWithoutNum(SummaryGraphEdge edge){
//		SummaryGraphElement source = edge.getSource();
//		SummaryGraphElement target = edge.getTarget();
//		SummaryGraphElement sourceWithoutNum = getGraphElementWithoutNum(source);
//		SummaryGraphElement targetWithoutNum = getGraphElementWithoutNum(target);
//		if(sourceWithoutNum.equals(source) && targetWithoutNum.equals(target)){
//			return edge; 
//		}
//		else {
//			return new SummaryGraphEdge(sourceWithoutNum, targetWithoutNum, edge.getEdgeLabel());
//		}
//	} 
	
	/**
	 * remove the first < or > from a string.
	 * @param line
	 * @return
	 */
	public static String removeGtOrLs(String line) {
		if(line == null || line.length() == 0) return line;
		
		int begin = line.charAt(0) == '<' ? 1 : 0;
		int end = line.charAt(line.length() - 1) == '>' ? line.length() - 1 : line.length();
		return line.substring(begin,end);
	}
	
	/**
	 * Get the local name of the uri.
	 * @param uri
	 * @return
	 */
	public static String getLocalName(String uri) {
		for(String stopWord: stopWords)
			uri = uri.replaceAll(stopWord, "");
		if( uri.lastIndexOf("#") != -1 ) {
			return uri.substring(uri.lastIndexOf("#") + 1);
		}
		else if(uri.lastIndexOf("/") != -1) {
			return uri.substring(uri.lastIndexOf("/") + 1);
		}
		else if(uri.lastIndexOf(":") != -1) {
			return uri.substring(uri.lastIndexOf(":") + 1);
		}
		else {
			return uri;
		}
	}
	
//	public static void outputEdges(Pseudograph<SummaryGraphElement, SummaryGraphEdge> graph) {
//		for(SummaryGraphEdge edge : graph.edgeSet()) {
//			System.out.println(edge.toString());
//		}
//	}
}
