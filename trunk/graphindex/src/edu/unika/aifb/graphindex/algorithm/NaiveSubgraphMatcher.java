package edu.unika.aifb.graphindex.algorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.graph.Edge;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.Vertex;

public class NaiveSubgraphMatcher implements SubgraphMatcher {

	private Logger log = Logger.getLogger(NaiveSubgraphMatcher.class);

	public List<Map<String,String>> findMapping(String indent, Vertex hostVertex, Vertex patternVertex) {
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
//		log.debug(indent + hostVertex + " " + patternVertex);
		
		List<Edge> pat = new ArrayList<Edge>(patternVertex.outgoingEdges());
		
		Map<String,List<Edge>> pat2host = new HashMap<String,List<Edge>>();
		Map<Edge,List<Map<String,String>>> host2Mapping = new HashMap<Edge,List<Map<String,String>>>();
		for (Edge patternEdge : pat) {
			Set<Edge> hostEdges = hostVertex.outgoingEdges(patternEdge.getLabel());
//			log.debug(indent + patternEdge + " hostEdges: " + hostEdges);
			
			if (hostEdges.size() == 0)
				return null;
			
			for (Edge hostEdge : hostEdges)
				host2Mapping.put(hostEdge, findMapping(indent + "\t", hostEdge.getTarget(), patternEdge.getTarget()));
			pat2host.put(patternEdge.getLabel(), new ArrayList<Edge>(hostEdges));
		}
//		log.debug(indent + "host2Mapping: " + host2Mapping);
		
		int[] states = new int [pat.size()];
		int[] limits = new int [pat.size()];
		for (int i = 0; i < states.length; i++) {
			states[i] = 0;
			limits[i] = pat2host.get(pat.get(i).getLabel()).size();
		}
		
		boolean done = false;
		while (!done) {
			Map<String,String> map = new HashMap<String,String>();
			boolean invalid = false;
			for (int i = 0; i < states.length; i++) {
				List<Map<String,String>> subList = host2Mapping.get(pat2host.get(pat.get(i).getLabel()).get(states[i]));
				if (subList != null) {
					// TODO join
					if (subList.size() != 1)
						System.exit(-1);
					
					Map<String,String> subMap = subList.get(0);

					for (String key : map.keySet()) {
						String val = map.get(key);
						if (subMap.containsKey(key) && !subMap.get(key).equals(val)) {
							invalid = true;
							break;
						}
					}
					
					if (invalid)
						break;
					
					map.putAll(subMap);
//					for (Map<String,String> subMap : subList)
//						map.putAll(subMap);
				}
					
			}
			if (!invalid)
				list.add(map);
			
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
		
		if (list.size() == 0)
			list.add(new HashMap<String,String>());
		
		for (Map<String,String> map : list)
			map.put(patternVertex.getLabel(), hostVertex.getLabel());
//		log.debug(indent + "returning " + list);
		return list;
	}
	
	public Matching match(Graph host, Graph pattern) {
		Matching matching = new Matching();
		
		// TODO add early stop conditions (edge subsets)
		
		Set<Vertex> processedHostVertices = new HashSet<Vertex>();

		Vertex patternRoot = pattern.getRoot();
		
		for (Edge patternEdge : patternRoot.outgoingEdges()) {
			for (Edge hostEdge : host.edges(patternEdge.getLabel())) {
				if (processedHostVertices.contains(hostEdge.getSource()))
					continue;
				
				List<Map<String,String>> mappingList = new ArrayList<Map<String,String>>();
				
				Vertex hostVertex = hostEdge.getSource();
				Vertex patternVertex = patternEdge.getSource();
		
				mappingList = findMapping("", hostVertex, patternVertex);
				if (mappingList != null) {
					for (Iterator<Map<String,String>> i = mappingList.iterator(); i.hasNext(); ) {
						if (i.next().size() < pattern.vertices().size())
							i.remove();
					}
					matching.addMappings(mappingList);
					log.debug("finished: " + mappingList.size());
//					log.debug("--------------------------------------------");
				}
				
				processedHostVertices.add(hostVertex);
			}
		}
		
		
		return matching;
	}
}
