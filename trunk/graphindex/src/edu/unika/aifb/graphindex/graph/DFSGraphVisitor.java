package edu.unika.aifb.graphindex.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.Util;

public class DFSGraphVisitor implements GraphVisitor {

	private Set<Vertex> m_visited;
	private boolean m_oppositeDirection, m_edgesSorted, m_allTrees;
	private DFSListener m_listener;
	private int m_dfsNr = 0;
	private Map<String,Integer> m_dfsNrs;
	private Logger log = Logger.getLogger(DFSGraphVisitor.class);
	private int m_curTree = 0; 
	private Vertex m_startNode = null;
	private Map<String,List<List<Edge>>> m_node2EdgesList;
	private Map<String,Integer> m_node2ListIdx;
	private boolean m_collectingEdgesLists;
	
	public DFSGraphVisitor(DFSListener listener) {
		this(listener, false);
	}
	
	public DFSGraphVisitor(DFSListener listener, boolean oppositeDirection) {
		this(listener, oppositeDirection, false, false);
	}
	
	public DFSGraphVisitor(DFSListener listener, boolean oppositeDirection, boolean edgesSorted, boolean allTrees) {
		m_oppositeDirection = oppositeDirection;
		m_edgesSorted = edgesSorted;
		m_visited = new HashSet<Vertex>();
		m_listener = listener;
		m_dfsNrs = new HashMap<String,Integer>();
		m_allTrees = allTrees;
		m_node2EdgesList = new HashMap<String,List<List<Edge>>>();
		m_node2ListIdx = new HashMap<String,Integer>();
		
		if (m_allTrees)
			m_edgesSorted = true;
	}
	
	private boolean isVisited(Vertex v) {
		return m_visited.contains(v);
	}

	private void addVisited(Vertex v) {
		m_visited.add(v);
	}

	private Vertex getSrc(Edge e) {
		return !m_oppositeDirection ? e.getSource() : e.getTarget();
	}

	private Vertex getDst(Edge e) {
		return m_oppositeDirection ? e.getSource() : e.getTarget();
	}
	
	private int getDfsNr(String label) {
		if (!m_dfsNrs.containsKey(label)) 
			m_dfsNrs.put(label, m_dfsNr++);
		return m_dfsNrs.get(label);
	}
	
	private void resetDfs() {
		m_dfsNrs.clear();
		m_dfsNr = 0;
		m_visited.clear();
	}

	private void start() {
		m_collectingEdgesLists = true;
		m_startNode.acceptVisitor(this);
		
		List<String> nodes = new ArrayList<String>(m_node2EdgesList.keySet());
		
		int nrOfTrees = 1;
		
		int[] states = new int [nodes.size()];
		int[] limits = new int [nodes.size()];
		for (int i = 0; i < states.length; i++) {
			states[i] = 0;
			limits[i] = m_node2EdgesList.get(nodes.get(i)).size();
			if (limits[i] > 0)
				nrOfTrees *= limits[i];
			if (m_startNode.getGraph().getName().equals("graph11122"))
				System.out.print(limits[i] + " ");
		}
		if (nrOfTrees > 1000)
			log.debug(nrOfTrees);
		
//		log.debug(m_node2EdgesList);
		
		m_collectingEdgesLists = false;
		boolean done = false;
		m_curTree = 0;
		while (!done) {
			for (int i = 0; i < nodes.size(); i++) {
				m_node2ListIdx.put(nodes.get(i), states[i]);
			}
			
			resetDfs();
			
			m_startNode.acceptVisitor(this);
			m_listener.treeComplete(m_curTree);
			m_curTree++;
			
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
	}
	
	public void visit(Vertex v) {
		if (m_startNode == null) {
			m_startNode = v;
			start();
			return;
		}
		
		if (m_collectingEdgesLists) {
			if (isVisited(v))
				return;
			
			addVisited(v);
			
			List<Edge> edges = new ArrayList<Edge>(m_oppositeDirection ? v.incomingEdges() : v.outgoingEdges());
			
			if (m_edgesSorted) {
				Edge[] list = edges.toArray(new Edge[] {});
				Arrays.sort(list, new Comparator<Edge>() {
					public int compare(Edge o1, Edge o2) {
						return o1.getLabel().compareTo(o2.getLabel());
					}
				});
				edges = new ArrayList<Edge>(Arrays.asList(list));
			}
			
			List<List<Edge>> edgesList = new ArrayList<List<Edge>>();
			
			if (m_allTrees) {
				List<Integer> starts = new ArrayList<Integer>();
				List<Integer> ends = new ArrayList<Integer>();
				List<List<Edge[]>> edgeSegments = new ArrayList<List<Edge[]>>();
				
				int curStart = -1;
				for (int i = 1; i < edges.size(); i++) {
					if (edges.get(i).getLabel().equals(edges.get(i - 1).getLabel())) {
						if (curStart == -1)
							curStart = i - 1;
					}
					else {
						if (curStart != -1) {
							Edge[] es = new Edge[i - curStart];
							System.arraycopy(edges.toArray(), curStart, es, 0, es.length);
							
							List<Edge[]> permutations = new ArrayList<Edge[]>();
							for (int j = 0; j < Util.factorial(es.length); j++)
								permutations.add((Edge[])Util.permute(j, es.clone()));
							edgeSegments.add(permutations);
							
							starts.add(curStart);
							ends.add(i - 1);
							curStart = -1;
						}
					}
				}
				
				if (curStart != -1) {
					Edge[] es = new Edge[edges.size() - curStart];
					System.arraycopy(edges.toArray(), curStart, es, 0, es.length);
					
					List<Edge[]> permutations = new ArrayList<Edge[]>();
					for (int j = 0; j < Util.factorial(es.length); j++)
						permutations.add((Edge[])Util.permute(j, es.clone()));
					edgeSegments.add(permutations);
					
					starts.add(curStart);
					ends.add(edges.size() - 1);
				}
	
	//			log.debug(edges);
	//			log.debug(starts);
	//			log.debug(ends);
	
				int[] states = new int [edgeSegments.size()];
				int[] limits = new int [edgeSegments.size()];
				for (int i = 0; i < states.length; i++) {
					states[i] = 0;
					limits[i] = edgeSegments.get(i).size();
				}
	
				boolean done = false;
				while (!done) {
					List<Edge> list = new ArrayList<Edge>();
					int curSegment = 0;
					for (int i = 0; i < edges.size(); i++) {
						if (curSegment >= starts.size()) {
							list.add(edges.get(i));
						}
						else if (i >= starts.get(curSegment) && i <= ends.get(curSegment)) {
							list.add(edgeSegments.get(curSegment).get(states[curSegment])[i - starts.get(curSegment)]);
						}
						else if (i < starts.get(curSegment)) {
							list.add(edges.get(i));
						}
						else if (i > ends.get(curSegment)) {
							curSegment++;
							i--;
							continue;
						}
					}
					
					edgesList.add(list);
					
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
			}
			else
				edgesList.add(edges);
			
			m_node2EdgesList.put(v.getLabel(), edgesList);
			
			for (Edge e : edges) {
				e.acceptVisitor(this);
				getDst(e).acceptVisitor(this);
			}
		}
		else {
			if (!isVisited(v)) {
				m_listener.encounterVertex(m_curTree, v, null, getDfsNr(v.getLabel()));
//				log.debug("visiting " + v + ", treeidx: " + m_curTree + " " + m_dfsNrs);
			}
			else {
				m_listener.encounterVertexAgain(m_curTree, v, null, getDfsNr(v.getLabel()));
//				log.debug("visiting again " + v + ", treeidx: " + m_curTree + " " + m_dfsNrs);
				return;
			}

			addVisited(v);
			
			List<Edge> edges = m_node2EdgesList.get(v.getLabel()).get(m_node2ListIdx.get(v.getLabel()));
			
			for (Edge e : edges) {
				e.acceptVisitor(this);
				getDst(e).acceptVisitor(this);
			}
		}
	}
	
	public void visit(Edge e) {
//		log.debug("visiting " + e + ", treedix: " + m_curTree + " " + getDfsNrs());
		if (!m_collectingEdgesLists) {
			int srcDfsNr = getDfsNr(getSrc(e).getLabel());
			int dstDfsNr = getDfsNr(getDst(e).getLabel());
			
			if (srcDfsNr > dstDfsNr)
				m_listener.encounterBackwardEdge(m_curTree, getSrc(e), e.getLabel(), getDst(e), srcDfsNr, dstDfsNr);
			else
				m_listener.encounterForwardEdge(m_curTree, getSrc(e), e.getLabel(), getDst(e), srcDfsNr, dstDfsNr);
		}
	}
}
