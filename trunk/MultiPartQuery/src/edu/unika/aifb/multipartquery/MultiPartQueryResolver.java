package edu.unika.aifb.multipartquery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.StructuredQuery;

public class MultiPartQueryResolver {
	
	private Map<String, List<String>> dsProperties = null;
	private Map<String, Integer> ranking = new HashMap<String, Integer>();
	private List<String> keys = null;
	
	public MultiPartQueryResolver(Map<String, List<String>> dsProps) {
		this.dsProperties = dsProps;
	} 
	
	public Map<String, StructuredQuery> resolve(StructuredQuery query) {
		Map<String, StructuredQuery> map = new HashMap<String, StructuredQuery>();
		Set<QueryEdge> pQuery = query.getQueryGraph().edgeSet();
		//Map<String, Integer> ranking = new HashMap<String, Integer>();
		
		for(Iterator<Entry<String, List<String>>> it = dsProperties.entrySet().iterator();it.hasNext();) {
			Entry<String, List<String>> e = it.next();
			List<String> p = e.getValue();
			ranking.put(e.getKey(), Integer.valueOf(0));
			
			for (Iterator<QueryEdge> itQ = pQuery.iterator(); itQ.hasNext();) {
				QueryEdge qe = itQ.next();
				if (p.contains(qe.getProperty())) {
					ranking.put(e.getKey(), ranking.get(e.getKey()).intValue()+1);
					System.out.println("Match: " + qe.getProperty());
					//System.out.println(ranking.get(e.getKey()));
				}
			}
			
			//ranking.put(e.getKey(), Integer.valueOf(125+(int)(Math.random()*100)));
		}
		
		keys = new ArrayList<String>(ranking.keySet()); 
		Collections.sort(keys, new Comparator() { 
		    public int compare(Object left, Object right) { 
		        return ranking.get((String)right).compareTo(ranking.get((String)left)); 
		    } 
		});
		
		System.out.println("Ranking:");
		/*for (Iterator<Entry<String, Integer>> it = ranking.entrySet().iterator(); it.hasNext();) {
			Entry<String, Integer> e = it.next();
			System.out.println(e.getKey() + " " + e.getValue());
		}*/
		
		Set<QueryEdge> visitedEdges = new HashSet<QueryEdge>(); 
		
		for (String k : keys) { 
			System.out.println(k + " " + ranking.get(k)); 
		}
		
		for(Iterator<String> itRanking = keys.listIterator(); itRanking.hasNext();) {
			String ds = itRanking.next();
			List<String> p = dsProperties.get(ds);
			StructuredQuery sq = new StructuredQuery("");
			
			for (Iterator<QueryEdge> itQ = pQuery.iterator(); itQ.hasNext();) {
				QueryEdge qe = itQ.next();
				if (!visitedEdges.contains(qe) && p.contains(qe.getProperty())) {
					sq.addEdge(qe.getSource(), qe.getProperty(), qe.getTarget());
					visitedEdges.add(qe);
				}
			}
			
			if (sq.getQueryGraph().edgeCount() > 0) {
				map.put(ds, sq);
				System.out.println("Added.");
			}
			
		}
		
		return map;
	}
}
