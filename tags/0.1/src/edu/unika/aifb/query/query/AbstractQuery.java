package edu.unika.aifb.query.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;


public abstract class AbstractQuery implements SQuery {
	
	protected Set<String> variables;
	protected String centerElement;
	protected Set<String> visitedVertices;
	protected Set<String> verticesWithLit;
	protected Set<String> frontierVertices;
	protected List<AtomQuery> path;
//	protected Set<AtomQuery> path;
	
	protected int numVar;
	protected int numDVar;
	protected int numAtom;
	
	public AbstractQuery() {
		variables = new TreeSet<String>();
	}

	public void addAtom(AtomQuery atom) {
		path.add(atom);
	}
	
	public void addVariables(String var) {
		variables.add(var);
	}
	
	public void setCenterElement(String ele) {
		centerElement = ele;
	}
	
	public String getCenterElement() {
		return centerElement;
	} 
	
	public void setVisitedVertices(Set<String> vertices) {
		visitedVertices = vertices;  
	}
	
	public void setVerticesWithLit(Set<String> vertices) {
		verticesWithLit = vertices;
	}
	
	public void setFrontierVertices(Set<String> vertices) {
		frontierVertices = vertices;
	}
	
	public void generateVariables() {
		Map<String, String> map = new HashMap<String, String>();
		int i = 1;
		Random r = new Random();
		String var = "?x" + i++;
		variables.add(var);
		visitedVertices.remove(centerElement);
		map.put(centerElement, var);
		if(frontierVertices != null && frontierVertices.size() != 0)
			visitedVertices.removeAll(frontierVertices);
		for(String vertex : visitedVertices) {
			var = "?x" + i++;
			map.put(vertex, var);
			variables.add(var);
		}
		if(frontierVertices != null && frontierVertices.size() != 0)
		for(String vertex : frontierVertices) {
//			boolean b = r.nextBoolean();
//			if(b == true) {
				var = "?x" + i++;
				map.put(vertex, var);
				variables.add(var);
//			}
		}
		for(AtomQuery aq : path) {
			String subject = aq.getSubject();
			String object = aq.getObject();
			String svar = map.get(subject);
			String ovar = map.get(object);
			if(svar != null)
				aq.setSubject(svar);
			if(ovar != null)
				aq.setObject(ovar);
		}
		
		numAtom = path.size();
		numVar = variables.size();
	} 

	public String getVariables(int max) {
		int i = 0; 
		Random r = new Random();
		numDVar = r.nextInt(max) + 1;
		String vars = "";
		for(String var : variables) {
			if(i >= numDVar) break;
			vars += var +" ";
			i++;
		}
		return vars;
	}

}
