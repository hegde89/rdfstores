package edu.unika.aifb.graphindex.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.Util;

public class DFSCoding {
	private class CodeEntry {
		public int srcNr, dstNr;
		public String src, dst;
		public String edge;
		public CodeEntry(int srcNr, int dstNr, String src, String edge, String dst) {
			super();
			this.dst = dst;
			this.dstNr = dstNr;
			this.edge = edge;
			this.src = src;
			this.srcNr = srcNr;
		}
		
		@Override
		public String toString() {
			return "(" + srcNr + "," + dstNr + "," + edge + ")";
		}
		
		public String toCanonicalLabel() {
			return srcNr + "|" + dstNr + "|" + edge;
		}
	}
	private class DFSLabelListener implements DFSListener {
		private List<CodeEntry> m_forward, m_backward;
		private String minCode = null;
		private Graph m_graph;
		
		public DFSLabelListener(Vertex v) {
			m_graph = v.getGraph();
			m_forward = new ArrayList<CodeEntry>();
			m_backward = new ArrayList<CodeEntry>();
		}
		
		public void encounterBackwardEdge(int tree, Vertex src, String edge, Vertex dst, int srcDfsNr, int dstDfsNr) {
			m_backward.add(new CodeEntry(srcDfsNr, dstDfsNr, src.getLabel(), edge, dst.getLabel()));
		}

		public void encounterForwardEdge(int tree, Vertex src, String edge, Vertex dst, int srcDfsNr, int dstDfsNr) {
			m_forward.add(new CodeEntry(srcDfsNr, dstDfsNr, src.getLabel(), edge, dst.getLabel()));
		}

		public void encounterVertex(int tree, Vertex v, Edge e, int dfsNr) {
		}

		public void encounterVertexAgain(int tree, Vertex v, Edge e, int dfsNr) {
		}
		
		public void treeComplete(int tree) {
			if (tree % 2000 == 0 && tree != 0) {
				log.debug(tree);
				if (tree == 2000)
					Util.printDOT("tree_" + m_graph.getName() + ".dot", m_graph);
			}
			String code = getCanonicalLabel(m_forward, m_backward);
			if (minCode == null || code.compareTo(minCode) < 0)
				minCode = code;
			m_forward.clear();
			m_backward.clear();
		}
		
		public String getMinCode() {
			return minCode;
		}
		
//		public Map<Integer,List<CodeEntry>> getForwardEdges() {
//			return m_forward;
//		}
//		
//		public Map<Integer,List<CodeEntry>> getBackwardEdges() {
//			return m_backward;
//		}
	}

	private static final Logger log = Logger.getLogger(DFSCoding.class);
	
	private String getCanonicalLabel(List<CodeEntry> f, List<CodeEntry> b) {
		String label = "";
		for (int i = 0; i < f.size(); i++) {
//			code.add(f.get(i));
			label += f.get(i).toCanonicalLabel() + "|";
			if (b != null && b.size() > 0) {
				int curBackSrc = b.get(0).srcNr;
				if (f.get(i).dstNr == curBackSrc) {
					while (b.size() > 0 && b.get(0).srcNr == curBackSrc) {
//						code.add(b.get(0));
						label += b.get(0).toCanonicalLabel() + "|";
						b.remove(0);
					}
				}
			}
		}

		return label;
	}
	
	public String getCanonicalLabel(Vertex v, boolean oppositeDirection) {
		DFSLabelListener listener = new DFSLabelListener(v);
		DFSGraphVisitor dfs = new DFSGraphVisitor(listener, oppositeDirection, true, true);

		dfs.visit(v);
		
//		String[] labels = new String [listener.getForwardEdges().keySet().size()];
//		for (int i = 0; i < labels.length; i++) {
//			labels[i] = getCanonicalLabel(listener.getForwardEdges().get(i), listener.getBackwardEdges().get(i));
//		}
//		log.debug(v + " " + labels.length);
//		Arrays.sort(labels);
		String label = listener.getMinCode();
		return label == null ? "" : label;
	}
}
