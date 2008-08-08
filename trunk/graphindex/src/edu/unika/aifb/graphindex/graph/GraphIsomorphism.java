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
			result = prime * result + ((v1 == null) ? 0 : v1.getLabel().hashCode());
			result = prime * result + ((v2 == null) ? 0 : v2.getLabel().hashCode());
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
			if (v1 == null) {
				if (other.v1 != null)
					return false;
			} else if (!v1.getLabel().equals(other.v1.getLabel()))
				return false;
			if (v2 == null) {
				if (other.v2 != null)
					return false;
			} else if (!v2.getLabel().equals(other.v2.getLabel()))
				return false;
			return true;
		}
	}
	
	private class EdgeSegment {
		public int start;
		public int end;
		public int size;
		private Edge[] edges;
		public List<Edge[]> permutations = new ArrayList<Edge[]>();
		
		public EdgeSegment(Edge[] edges, int start, int end) {
			this.start = start;
			this.end = end;
			
			Edge[] es = new Edge[end - start + 1];
			System.arraycopy(edges, start, es, 0, es.length);
			this.edges = es;
			
			size = Util.factorial(es.length);
		}
		
		public Edge[] getPermutation(int idx) {
			Edge[] es = (Edge[])Util.permute(idx, edges.clone());
			return es;
		}
		
		public String toString() {
			return "(" + start + "," + end + "|" + size + ")";
		}
	}
	
	private class EdgePairingsGenerator {
		private int[] states, limits;
		private Edge[] hostEdges, patEdges;
		private List<EdgeSegment> segments;
		private boolean done = false;
		
		public EdgePairingsGenerator(Set<Edge> hostEdgesList, Set<Edge> patEdgesList) {
			hostEdges = sort(hostEdgesList);
			patEdges = sort(patEdgesList);
			segments = new ArrayList<EdgeSegment>();
			
			int curStart = -1;
			for (int i = 0; i < hostEdges.length; i++) {
				if (!hostEdges[i].getLabel().equals(patEdges[i].getLabel())) {
					done = true;
					return;
				}
				
				if (i == 0)
					continue;
				
				if (patEdges[i].getLabel().equals(patEdges[i - 1].getLabel())) {
					if (curStart == -1)
						curStart = i - 1;
				}
				else {
					if (curStart != -1) {
						segments.add(new EdgeSegment(patEdges, curStart, i - 1));
						curStart = -1;
					}
				}
			}
			
			if (curStart != -1) {
				segments.add(new EdgeSegment(patEdges, curStart, patEdges.length - 1));
			}
			
//			log.debug(segments);
			int permutations = 1;
			states = new int [segments.size()];
			limits = new int [segments.size()];
			for (int i = 0; i < states.length; i++) {
				states[i] = 0;
				limits[i] = segments.get(i).size;
				permutations *= limits[i];
			}
//			log.debug(permutations);
//			for (EdgeSegment s : segments)
//				System.out.print(s + " ");
//			System.out.println();
		}
		
		public List<EdgePair> nextPairing() {
			if (done)
				return null;
			
			List<EdgePair> list = new ArrayList<EdgePair>();
			int curSegment = 0;
			for (int i = 0; i < hostEdges.length; i++) {
				EdgePair ep = new EdgePair();
				ep.he = hostEdges[i];
				
				if (curSegment >= segments.size()) {
					ep.pe = patEdges[i];
				}
				else if (i >= segments.get(curSegment).start && i <= segments.get(curSegment).end) {
					ep.pe = segments.get(curSegment).getPermutation(states[curSegment])[i - segments.get(curSegment).start];
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
			
			return list;
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
		return null;
//		EdgeSegment segment = new EdgeSegment();
//		
//		
//		segment.start = start;
//		segment.end = end;
//		
//		return segment;
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
		int permutations = 1;
		int[] states = new int [segments.size()];
		int[] limits = new int [segments.size()];
		for (int i = 0; i < states.length; i++) {
			states[i] = 0;
			limits[i] = segments.get(i).permutations.size();
			permutations *= limits[i];
		}
//		log.debug(permutations);
		
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

	int all = 0, stopCache = 0, stopEdgesSize = 0, stopEdgesSizeZero = 0, stopEdgeLabels = 0, stopPrevMap = 0;
	private boolean isIsomorph(Vertex x, Vertex y, Map<String,String> prevMapps) {
		VertexPair vp = new VertexPair(x, y);
		
		all++;
		
		Boolean cacheValue = m_isomorphismCache.get(vp);
		
		if (cacheValue != null)
			return cacheValue;
		
//		if (isCached(vp))
//			return m_isomorphismCache.get(vp);
		
		stopCache++;
		
		if (getEdges(x).size() != getEdges(y).size())
			return false;
		
		stopEdgesSize++;
		
		if (getEdges(x).size() == 0) // leaf nodes
			return true;
		
		stopEdgesSizeZero++;
		
		if (!getEdgeLabels(x).equals(getEdgeLabels(y)))
			return false;
		
		stopEdgeLabels++;

		if (prevMapps.containsKey(x.getLabel())) {
			if (prevMapps.get(x.getLabel()).equals(y.getLabel()))
				return true;
			else
				return false;
		}
		
		stopPrevMap++;
//		log.debug(x + " " + y + " " + cacheStats());
		
		Map<String,String> curMapps = new HashMap<String,String>(prevMapps);
		curMapps.put(x.getLabel(), y.getLabel());
		
//		List<List<EdgePair>> pairingPermutations = getEdgePairingsPermutations(getEdges(x), getEdges(y));
//		log.debug(pairingPermutations);
		
//		if (pairingPermutations == null) {
//			log.debug(getEdges(x) + " " + getEdges(y));
//			return false;
//		}
		
//		m_gen.initialize(getEdges(x), getEdges(y));
		EdgePairingsGenerator gen = new EdgePairingsGenerator(getEdges(x), getEdges(y));
		
//		for (List<EdgePair> pairing : pairingPermutations) {
		List<EdgePair> pairing;
		while ((pairing = gen.nextPairing()) != null) {
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
	
	private class DFSCountListener implements DFSListener {
		public int count = 0;
		public void encounterBackwardEdge(int tree, Vertex src,
				String edge, Vertex dst, int srcDfsNr, int dstDfsNr) {
		}

		public void encounterForwardEdge(int tree, Vertex src, String edge,
				Vertex dst, int srcDfsNr, int dstDfsNr) {
		}

		public void encounterVertex(int tree, Vertex v, Edge e, int dfsNr) {
			count++;
		}

		public void encounterVertexAgain(int tree, Vertex v, Edge e, int dfsNr) {
		}

		public void treeComplete(int tree) {
		}
	}
	
	public boolean isIsomorph(Vertex x, Vertex y) {
		DFSCountListener listener = new DFSCountListener();
		DFSGraphVisitor dfs = new DFSGraphVisitor(listener, m_oppositeDirection, false, false);
		
		x.acceptVisitor(dfs);
		int xcount = listener.count;
		listener.count = 0;
		dfs.reset();
		
		y.acceptVisitor(dfs);
//		log.debug(xcount + " " + listener.count);
		boolean ret;
		if (xcount != listener.count)
			ret = false;
		else
			ret = isIsomorph(x, y, new HashMap<String,String>());
		m_isomorphismCache.put(new VertexPair(x, y), ret);
		
//		log.debug(all + " " + stopCache + " " + stopEdgesSize + " " + stopEdgesSizeZero + " " + stopEdgeLabels + " " + stopPrevMap);
		
		return ret;
	}

	public Map<VertexPair,Boolean> getCache() {
		return m_isomorphismCache;
	}
}
