package edu.unika.aifb.keywordsearch;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class TransformedGraphNode {
	
	private int nodeId;
	private Map<Integer,Integer> distances;
	private Map<String, Collection<String>> entityQuery;
	private Collection<TransformedGraphNode> neighbors;
	private Collection<KeywordElement> entities;
	private int maxDistance;
	
	public TransformedGraphNode(int node) {
		this.nodeId = node;
		this.distances = new HashMap<Integer, Integer>();
		this.entityQuery = new HashMap<String, Collection<String>>();
		this.neighbors = new HashSet<TransformedGraphNode>();
		this.maxDistance = 0;
	}
	
	public int getNodeId() {
		return this.nodeId;
	}
	
	public void setEntities(Collection<KeywordElement> entities) {
		this.entities = entities;
	}
	
	public Collection<KeywordElement> getEntities() {
		return this.entities;
	} 
	
	public void addEntityQuery(String predicate, String object) {
		Collection<String> coll = this.entityQuery.get(predicate);
		if(coll == null) {
			coll = new HashSet<String>();
			entityQuery.put(predicate, coll);
		}
		coll.add(object);
	}
	
	public Map<String, Collection<String>> getEntityQuery() {
		return this.entityQuery;
	}
	
	public void setDistance(int nodeId, int dis) {
		if(!distances.containsKey(nodeId)) {
			distances.put(nodeId, dis);
			if(dis > maxDistance)
				maxDistance = dis;
		}	
	}
	
	public int getDistance(int nodeId) {
		return distances.get(nodeId); 
	}
	
	public int getMaxDistance() {
		return maxDistance;
	}
	
	public void addNeighbor(TransformedGraphNode node) {
		this.neighbors.add(node);
	}
	
	public Collection<TransformedGraphNode> getNeighbors() {
		return this.neighbors;
	}
	
	public int getEntitySize() {
		return entities.size();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + new Integer(nodeId).hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TransformedGraphNode other = (TransformedGraphNode)obj;
		if (nodeId != other.getNodeId())
			return false;
		return true;
	}

}
