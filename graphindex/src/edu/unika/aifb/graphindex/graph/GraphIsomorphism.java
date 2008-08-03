package edu.unika.aifb.graphindex.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.Util;

public class GraphIsomorphism {
	
	private class EdgePair {
		public Edge he, pe;
		public EdgePair(Edge hostEdge, Edge patEdge) {
			he = hostEdge;
			pe = patEdge;
		}
		public EdgePair() {
			// TODO Auto-generated constructor stub
		}
		
		public String toString() {
			return "(" + he + "," + pe + ")";
		}
	}
	
	private class VertexPair {
		public Vertex v1, v2;
		public VertexPair(Vertex v1, Vertex v2) {
			this.v1 = v1;
			this.v2 = v2;
		}
		
		@Override
		public String toString() {
			return "(" + v1 + "," + v2 + ")";
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((v1 == null) ? 0 : v1.hashCode());
			result = prime * result + ((v2 == null) ? 0 : v2.hashCode());
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
			VertexPair other = (VertexPair)obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (v1 == null) {
				if (other.v1 != null)
					return false;
			} else if (!v1.equals(other.v1))
				return false;
			if (v2 == null) {
				if (other.v2 != null)
					return false;
			} else if (!v2.equals(other.v2))
				return false;
			return true;
		}
		
		private GraphIsomorphism getOuterType() {
			return GraphIsomorphism.this;
		}
	}
	
	private class EdgeSegment {
		public int start;
		public int end;
		public List<Edge[]> permutations = new ArrayList<Edge[]>();
		
		public String toString() {
			return "(" + start + "," + end + "|" + permutations + ")";
		}
	}
	
	private Map<VertexPair,Boolean> m_isomorphismCache;
	private boolean m_oppositeDirection;
	public int cacheMisses = 0;
	public int cacheHits = 0;
	private static final Logger log = Logger.getLogger(GraphIsomorphism.class);
	
	public GraphIsomorphism(boolean oppositeDirection) {
		m_oppositeDirection = oppositeDirection;
		m_isomorphismCache = new HashMap<VertexPair,Boolean>();
	}
	
	private Set<Edge> getEdges(Vertex v) {
		if (m_oppositeDirection)
			return v.incomingEdges();
		else
			return v.outgoingEdges();
	}
	
	private Set<Edge> getEdges(Vertex v, String label) {
		if (m_oppositeDirection)
			return v.incomingEdges(label);
		else
			return v.outgoingEdges(label);
	}
	
	private Set<String> getEdgeLabels(Vertex v) {
		if (m_oppositeDirection)
			return v.incomingEdgeLabels();
		else
			return v.outgoingEdgeLabels();
	}
	
	private Vertex getTarget(Edge e) {
		if (m_oppositeDirection)
			return e.getSource();
		else
			return e.getTarget();
	}
	
	private Vertex getSource(Edge e) {
		if (!m_oppositeDirection)
			return e.getSource();
		else
			return e.getTarget();
	}
	
	private Edge[] sort(Set<Edge> list) {
		Edge[] edges = list.toArray(new Edge[] {});
		Arrays.sort(edges, new Comparator<Edge>() {
			public int compare(Edge o1, Edge o2) {
				return o1.getLabel().compareTo(o2.getLabel());
			}
		});
		return edges;
	}
	
	private EdgeSegment getSegment(Edge[] edges, int start, int end) {
		EdgeSegment segment = new EdgeSegment();
		
		Edge[] es = new Edge[end - start + 1];
		System.arraycopy(edges, start, es, 0, es.length);
		
		for (int j = 0; j < Util.factorial(es.length); j++)
			segment.permutations.add((Edge[])Util.permute(j, es.clone()));
		
		segment.start = start;
		segment.end = end;
		
		return segment;
	}
	
	private List<List<EdgePair>> getEdgePairingsPermutations(Set<Edge> hostEdgesList, Set<Edge> patEdgesList) {
		Edge[] hostEdges = sort(hostEdgesList);
		Edge[] patEdges = sort(patEdgesList);
		List<EdgeSegment> segments = new ArrayList<EdgeSegment>();
		
		int curStart = -1;
		for (int i = 0; i < hostEdges.length; i++) {
			if (!hostEdges[i].getLabel().equals(patEdges[i].getLabel()))
				return null;
			
			if (i == 0)
				continue;
			
			if (patEdges[i].getLabel().equals(patEdges[i - 1].getLabel())) {
				if (curStart == -1)
					curStart = i - 1;
			}
			else {
				if (curStart != -1) {
					segments.add(getSegment(patEdges, curStart, i - 1));
					curStart = -1;
				}
			}
		}
		
		if (curStart != -1) {
			segments.add(getSegment(patEdges, curStart, patEdges.length - 1));
		}
		
//		log.debug(segments);
		
		int[] states = new int [segments.size()];
		int[] limits = new int [segments.size()];
		for (int i = 0; i < states.length; i++) {
			states[i] = 0;
			limits[i] = segments.get(i).permutations.size();
		}
		
		List<List<EdgePair>> pairList = new ArrayList<List<EdgePair>>();

		boolean done = false;
		while (!done) {
			List<EdgePair> list = new ArrayList<EdgePair>();
			int curSegment = 0;
			for (int i = 0; i < hostEdges.length; i++) {
				EdgePair ep = new EdgePair();
				ep.he = hostEdges[i];
				
				if (curSegment >= segments.size()) {
					ep.pe = patEdges[i];
				}
				else if (i >= segments.get(curSegment).start && i <= segments.get(curSegment).end) {
					ep.pe = segments.get(curSegment).permutations.get(states[curSegment])[i - segments.get(curSegment).start];
				}
				else if (i < segments.get(curSegment).start) {
					ep.pe = patEdges[i];
				}
				else if (i > segments.get(curSegment).end) {
					curSegment++;
					i--;
					continue;
				}
				
				list.add(ep);
			}
			
			pairList.add(list);
			
			boolean carry = true;
			for (int i = 0; i < states.length; i++) {
				if (carry)
					states[i]++;
				
				if (states[i] >= limits[i]) {
					states[i] = 0;
					carry = true;
				}
				else
					carry = false;
			}
			
			done = carry;
		}
		
		return pairList;
	}
	
	private boolean isCached(VertexPair vp) {
		if (m_isomorphismCache.containsKey(vp)) {
//			log.debug(vp + " hit");
			cacheHits++;
			return true;
		}
//		log.debug(vp + " miss");
		cacheMisses++;
		return false;
	}
	
	public void clearCache() {
		m_isomorphismCache.clear();
	}
	
	public String cacheStats() {
		return "cache size: " + m_isomorphismCache.keySet().size() + ", hits: " + cacheHits + ", misses: " + cacheMisses;
	}

	private boolean isIsomorph(Vertex x, Vertex y, Map<String,String> prevMapps) {
		VertexPair vp = new VertexPair(x, y);
		if (isCached(vp))
			return m_isomorphismCache.get(vp);
		
		if (getEdges(x).size() != getEdges(y).size())
			return false;
		
		if (getEdges(x).size() == 0) // leaf nodes
			return true;
		
		if (!getEdgeLabels(x).equals(getEdgeLabels(y)))
			return false;

		if (prevMapps.containsKey(x.getLabel())) {
			if (prevMapps.get(x.getLabel()).equals(y.getLabel()))
				return true;
			else
				return false;
		}
		
		Map<String,String> curMapps = new HashMap<String,String>(prevMapps);
		curMapps.put(x.getLabel(), y.getLabel());
		
		List<List<EdgePair>> pairingPermutations = getEdgePairingsPermutations(getEdges(x), getEdges(y));
//		log.debug(pairingPermutations);
		
		if (pairingPermutations == null) {
			log.debug(getEdges(x) + " " + getEdges(y));
			return false;
		}
		
		for (List<EdgePair> pairing : pairingPermutations) {
			boolean found = true;
			for (EdgePair p : pairing) {
				if (!isIsomorph(getTarget(p.he), getTarget(p.pe), curMapps)) {
					m_isomorphismCache.put(new VertexPair(getTarget(p.he), getTarget(p.pe)), false);
					found = false;
					break;
				}
				else
					m_isomorphismCache.put(new VertexPair(getTarget(p.he), getTarget(p.pe)), true);
			}
			
			if (found) {
//				log.debug(curMapps);
				return true;
			}
		}
		
		return false;
	}
	
	public boolean isIsomorph(Vertex x, Vertex y) {
		boolean ret = isIsomorph(x, y, new HashMap<String,String>());
		m_isomorphismCache.put(new VertexPair(x, y), ret);
		return ret;
	}

	public Map<VertexPair,Boolean> getCache() {
		return m_isomorphismCache;
	}
}
