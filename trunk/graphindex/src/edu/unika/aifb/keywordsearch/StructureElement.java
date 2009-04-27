package edu.unika.aifb.keywordsearch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import edu.unika.aifb.keywordsearch.api.IResource;
import edu.unika.aifb.keywordsearch.impl.Attribute;
import edu.unika.aifb.keywordsearch.impl.Entity;
import edu.unika.aifb.keywordsearch.impl.Relation;
import edu.unika.aifb.keywordsearch.impl.NamedConcept;


public class StructureElement implements Serializable,IStructureElement {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8594203186544962955L;
	
	public static final int CONCEPT = 0;
	public static final int RELATION = 1;
	public static final int ENTITY = 2;

	protected IResource resource;

	protected double EF;
	private double m_totalCost;
	private double m_matchingScore;

	protected int type;
	
	protected Map<String,Queue<Cursor>> cursors;
	protected Set<Set<Cursor>> m_exploredCursorCombinations;
	
	public void setExploredCursorCombinations(Set<Set<Cursor>> exploredCursorCombinations) {
		this.m_exploredCursorCombinations = exploredCursorCombinations;
	}
	
	public StructureElement(){}

	public StructureElement(IResource resource, int type) {
		this.resource = resource;
		this.type = type;
	}

	public StructureElement(IResource resource, int type, double EF) {
		this.resource = resource;
		this.type = type;
		this.EF = EF;
	}

	public StructureElement(IResource resource, int type, double score, double EF) {
		this.resource = resource;
		this.type = type;
		this.m_matchingScore = score;
		this.EF = EF;
	}

	public int getType(){
		return type;
	}
	
	public void setType(int type) {
		this.type = type;
	}

	public void setResource(IResource resource){
		this.resource = resource;
	}

	public IResource getResource(){
		return resource;
	}
	public void setEF(double EF){
		this.EF = EF;
	}
	public void setCursors(Map<String,Queue<Cursor>> cursors) {
		this.cursors = cursors;
	}

	public double getEF(){
		return EF;
	}
	
	public void setMatchingScore(double score){
		this.m_matchingScore = score;
	}

	public double getMatchingScore(){
		return m_matchingScore;
	}
		
	public String toString(){
		if(resource == null)return null;
		if(resource instanceof NamedConcept)
			return ((NamedConcept)resource).getUri();
		else if(resource instanceof Attribute )
			return ((Attribute)resource).getUri();
		else if(resource instanceof Entity )
			return ((Entity)resource).getUri();
		else return super.toString();
	}

	public Map<String,Queue<Cursor>> getCursors(){
		return cursors;
	}

	public double getTotalCost() {
		return m_totalCost;
	}

	public void setTotalCost(double cost) {
		m_totalCost = cost;
	}

	public void initCursorQueues(Set<String> keywords){
		cursors = new HashMap<String,Queue<Cursor>>();
		for(String keyword : keywords){
			cursors.put(keyword, new PriorityQueue<Cursor>());
		}
	}
	
	public void addCursor(Cursor cursor, String keyword){
		Queue<Cursor> q = cursors.get(keyword);
		q.add(cursor);
	}
	
	public HashSet<Set<Cursor>> processCursorCombinations(Cursor cursor, String keyword){
		int size = cursors.size();
		int[] guard = new int[size];
		int i = 0;
		Set<String> keywords = cursors.keySet();
		List<List<Cursor>> lists = new ArrayList<List<Cursor>>();
		for(String key : keywords){
			if(key.equals(keyword)){
				List<Cursor> list = new ArrayList<Cursor>();
				list.add(cursor);
				lists.add(list);
				guard[i++] = 0;
			}
			else {
				lists.add(new ArrayList<Cursor>(cursors.get(key)));
				guard[i++] = cursors.get(key).size() - 1;
			}
		}
		HashSet<Set<Cursor>> m_newCursorCombinations = new HashSet<Set<Cursor>>();
		
		int[] index = new int[size];
		for(int j=0;j<index.length;j++) {
			index[j] = 0;
		}
		guard[size-1]++;
		do {
			Set<Cursor> combination = new HashSet<Cursor>();
			for(int m = 0; m < size; m++){
				combination.add(lists.get(m).get(index[m]));
			}
			m_newCursorCombinations.add(combination);
			index[0]++;
			for(int j = 0; j < size; j++){
				if(index[j] > guard[j]){
					index[j] = 0;
					index[j+1]++; 
				}
			}
		}
		while(index[size-1] < guard[size-1]);
		return m_newCursorCombinations;
	}
	
	public boolean isConnectingElement() {
		if( type == RELATION) return false; // Only the concept node will be the connected Element.
		if(m_exploredCursorCombinations != null && m_exploredCursorCombinations.size() > 0) return true;
		
		if(cursors == null || cursors.size() == 0) return false;
		for(Queue<Cursor> queue : cursors.values()){
			if(queue.isEmpty()){
				return false;
			}	
		}
		return true;
	}
	
	
	public void addExploredCursorCombinations(Set<Set<Cursor>> combinations){
		if(combinations != null && combinations.size() != 0)
		{
			if(m_exploredCursorCombinations==null)m_exploredCursorCombinations = new HashSet<Set<Cursor>>();
			m_exploredCursorCombinations.addAll(combinations);
		}
	}
	
	public Set<Set<Cursor>> getExploredCursorCombinations(){
		return m_exploredCursorCombinations;
	}
	
	public boolean equals(Object object){
		if(this == object) return true;
		if(object == null) return false;
		if(!(object instanceof StructureElement)) return false;
		
		StructureElement vertex = (StructureElement)object;
		if(resource instanceof NamedConcept && vertex.getResource() instanceof NamedConcept)
			return ((NamedConcept)resource).getUri().equals(((NamedConcept)vertex.getResource()).getUri());
		else if(resource instanceof Relation && vertex.getResource() instanceof Relation)
			return ((Relation)resource).getUri().equals(((Relation)vertex.getResource()).getUri());
		else if(resource instanceof Entity && vertex.getResource() instanceof Entity) {
			return ((Entity)resource).getUri().equals(((Entity)vertex.getResource()).getUri());
		}
		return false;
	}
	
	public int hashCode(){
		return resource.hashCode();
	}

}