package edu.unika.aifb.graphindex.algorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.graph.Edge;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphIsomorphism;
import edu.unika.aifb.graphindex.graph.Vertex;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;

public class DGMerger {
	private class Tuple {
		public Vertex v1, v2;
		public Tuple(Vertex v1, Vertex v2) {
			this.v1 = v1;
			this.v2 = v2;
		}
	}
	
	private ExtensionManager m_em = StorageManager.getInstance().getExtensionManager();
	@Deprecated
	private List<Graph> m_guides;
	private Set<Vertex> m_visitedVertices;
	private GraphIsomorphism m_gi;
	
	private static final Logger log = Logger.getLogger(DGMerger.class);

	public DGMerger() {
		this(null);
	}
	
	@Deprecated
	public DGMerger(List<Graph> guides) {
		m_guides = guides;
		m_gi = new GraphIsomorphism(false);
	}
	
	public List<Graph> getMerged() {
		return m_guides;
	}
	
	private void sort() {
		Graph[] list = m_guides.toArray(new Graph[]{});
		Arrays.sort(list, new Comparator<Graph>() {
			public int compare(Graph o1, Graph o2) {
				return ((Integer)o1.numberOfVertices()).compareTo(o2.numberOfVertices()) * -1;
			}
		});
		m_guides = new ArrayList<Graph>(Arrays.asList(list));
	}
	
	/**
	 * Determines if two graphs can be merged by checking if one graph
	 * subsumes the other. This method uses the graph subsumption algorithm,
	 * which checks if corresponding nodes in both graphs have the same
	 * incoming paths. This is more expensive than the weaker implementation,
	 * {@link #subsumesWeak(Graph, Graph)} 
	 * 
	 * @param g1
	 * @param g2
	 * @return true if g1 subsumes g2, false otherwise
	 */
	public boolean subsumes(Graph g1, Graph g2) {
		HashSet<Vertex> visitedVertices = new HashSet<Vertex>();
		m_gi.clearCache();
		
		Stack<Tuple> stack = new Stack<Tuple>();
		stack.push(new Tuple(g1.getRoot(), g2.getRoot()));
		
		while (stack.size() != 0) {
			Tuple cur = stack.pop();
			
			visitedVertices.add(cur.v2);
			
			Map<String,List<Vertex>> v1_out = cur.v1.outgoingEdgeMap();
			Map<String,List<Vertex>> v2_out = cur.v2.outgoingEdgeMap();
			
			if (!m_gi.isIsomorph(cur.v1, cur.v2))
				return false;
			
			for (String l : v2_out.keySet()) {
				List<Vertex> v2_targets = v2_out.get(l);
				List<Vertex> v1_targets = v1_out.get(l);
				
				if (v1_targets == null || v1_targets.size() < v2_targets.size())
					return false;
				
				// TODO not necessary, dataguides have by definition at most one outgoing edge per label
				assert v1_targets.size() == 1;
				assert v2_targets.size() == 1;
				
				if (!visitedVertices.contains(v2_targets.get(0)))
					stack.push(new Tuple(v1_targets.get(0), v2_targets.get(0)));
			}
		}
		
		
		return true;
	}
	
	/**
	 * Checks if two graphs are isomorphic and therefore can be merged. This is
	 * relatively inexpensive as the search space is much reduced, because the root
	 * node of both graphs have to be mapped to each other. 
	 * 
	 * @param g1
	 * @param g2
	 * @return true if g1 and g2 are isomorphic, false otherwise
	 */
	public boolean subsumesWeak(Graph g1, Graph g2) {
		m_gi.clearCache();
		return m_gi.isIsomorph(g1.getRoot(), g2.getRoot());
	}
	
	public void merge(Graph g1, Vertex v1, Graph g2, Vertex v2) throws StorageException {
//		m_em.mergeExtension(v1.getLabel(), v2.getLabel());
		m_em.extension(v1.getLabel()).mergeExtension(m_em.extension(v2.getLabel()));
		m_visitedVertices.add(v2);
		
		for (Edge e : v2.outgoingEdges()) {
			if (!m_visitedVertices.contains(e.getTarget())) {
				List<Vertex> v1out = v1.outgoingEdgeMap().get(e.getLabel());
				if (v1out == null) {
					log.debug(v1.outgoingEdgeMap());
					log.debug(e.getLabel());
					Util.printDOT("g1.dot", g1);
					Util.printDOT("g2.dot", g2);
					log.debug(subsumes(g1, g2));
					log.debug(subsumes(g2, g1));
					log.debug(v1 + " " + v1.outgoingEdges());
					log.debug(v2 + " " + v2.outgoingEdges());
					log.debug(g1.numberOfVertices());
					log.debug(g2.numberOfVertices());
					log.debug(v1.getPaths());
					log.debug(v2.getPaths());
					System.exit(-1);
				}
				assert e != null && v1out != null;
				try {
					merge(g1, v1out.get(0), g2, e.getTarget());
				}
				catch (Exception f) {
					log.debug(e + " " + v1out);
					f.printStackTrace();
				}
			}
		}
	}
	
	/** 
	 * Merge graph g2 into graph g1 by merging extensions of corresponding nodes.
	 * 
	 * @param g1
	 * @param g2
	 * @throws StorageException
	 */
	public void merge(Graph g1, Graph g2) throws StorageException {
		m_visitedVertices = new HashSet<Vertex>();
		merge(g1, g1.getRoot(), g2, g2.getRoot());
	}
	
	/**
	 * Merges all guides in m_guides by comparing all guides to each other.
	 * Really slow and therefore deprecated. 
	 * 
	 * @throws StorageException
	 */
	@Deprecated
	public void merge() throws StorageException {
		log.info("number of guides before merging: " + m_guides.size());
		
		log.debug(m_guides.get(0).numberOfVertices() + " " + m_guides.get(m_guides.size() - 1).numberOfVertices());
		sort(); // sort guides by number of vertices, descending
		log.debug(m_guides.get(0).numberOfVertices() + " " + m_guides.get(m_guides.size() - 1).numberOfVertices());
		
		int edges = 0, vertices = 0;
		
		int subsumptionTests = 0;
		
		List<Graph> merged = new ArrayList<Graph>();
		for (int i = 0; i < m_guides.size(); i++) {
			Graph g1 = m_guides.get(i);
			g1.load();

			for (int j = i + 1; j < m_guides.size(); j++) {
				Graph g2 = m_guides.get(j);
				g2.load();

				subsumptionTests++;
				if (subsumes(g1, g2)) {
//					log.debug("merging " + g2.getName() + " into " + g1.getName());
					m_guides.remove(j);
					j--;
					m_visitedVertices = new HashSet<Vertex>();
					merge(g1, g1.getRoot(), g2, g2.getRoot());
					g2.remove();
				}
				else
					g2.unload(false);
				m_gi.clearCache();
			}

			edges += g1.numberOfEdges();
			vertices += g1.numberOfVertices();
			
			merged.add(g1);
			g1.unload(false);
//			Util.printDOT("merged_" + g1.getName() + ".dot", g1);
			
			log.info(m_guides.size() + " dgs left");
			
			if (Util.freeMemory() < 300000) {
				long free = Util.freeMemory();
//				m_em.unloadAllExtensions();
				log.debug("free: " + Util.freeMemory() / 1000 + ", before: " + free / 1000 + " (" + (Util.freeMemory() - free) / 1000 + " freed)");
			}
		}
		log.debug("subsumption tests: " + subsumptionTests);
		
		m_guides = merged;
		
		int avgVertices = vertices / m_guides.size();
		int avgEdges = edges / m_guides.size();
		
		log.debug(m_gi.cacheStats());
		log.info("number of guides after merging: " + m_guides.size());
		log.info("avg edges: + " + avgEdges + ", avg vertices: " + avgVertices);
		
//		m_em.unloadAllExtensions();
	}
}
