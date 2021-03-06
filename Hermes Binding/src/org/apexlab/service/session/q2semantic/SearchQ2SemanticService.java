package org.apexlab.service.session.q2semantic;
//
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.PriorityQueue;
//import java.util.Scanner;
//import java.util.Set;
//
//import org.apexlab.service.session.datastructure.Concept;
//import org.apexlab.service.session.datastructure.ConceptSuggestion;
//import org.apexlab.service.session.datastructure.Facet;
//import org.apexlab.service.session.datastructure.GraphEdge;
//import org.apexlab.service.session.datastructure.Literal;
//import org.apexlab.service.session.datastructure.QueryGraph;
//import org.apexlab.service.session.datastructure.Relation;
//import org.apexlab.service.session.datastructure.RelationSuggestion;
//import org.apexlab.service.session.datastructure.Source;
//import org.apexlab.service.session.datastructure.Suggestion;
//import org.team.xxplore.core.service.impl.Literal;
//import org.team.xxplore.core.service.impl.NamedConcept;
//import org.team.xxplore.core.service.impl.Property;
//import org.team.xxplore.core.service.mapping.Mapping;
//import org.team.xxplore.core.service.q2semantic.SummaryGraphEdge;
//import org.team.xxplore.core.service.q2semantic.SummaryGraphElement;
//import org.team.xxplore.core.service.q2semantic.SummaryGraphUtil;
//import org.team.xxplore.core.service.q2semantic.search.Q2SemanticService;
//import org.team.xxplore.core.service.q2semantic.search.QueryInterpretationService;
//import org.team.xxplore.core.service.q2semantic.search.Subgraph;
//
//
///**
// * This class is the Q2Semantic service API that can be called by the search engine interface
// * @author tpenin
// */
//
//public class SearchQ2SemanticService {
//
//	Q2SemanticService q2semanticservice ;
//	
//    /**
//     * TO DO
//     * @param concept
//     * @param ds
//     * @return
//     * @throws Exception
//     */
//    public Collection<Suggestion> getSuggestion(List<Concept> concept, String ds, int topK) throws Exception {      
//            List<String> con = new ArrayList<String>();
//            for(Concept c: concept)
//                    con.add(c.getURI());
//            
//            Set<String> sugg = q2semanticservice.getInter().getSuggestion(con, ds, q2semanticservice.getInter().mis);
//            PriorityQueue<Suggestion> res = new PriorityQueue<Suggestion>();
//            for(String str: sugg)
//            {
////                  System.out.println(str);
//                    String[] part = str.split("\t");
//                    if(part.length!=4) continue;
//                    String label = SummaryGraphUtil.getLocalName(SummaryGraphUtil.removeNum(part[0]));
//                    if(part[3].equals(QueryInterpretationService.ConceptMark))
//                            res.add(new ConceptSuggestion(label, new Source(part[1],null, 0), "<"+part[0]+">", Double.parseDouble(part[2])));
//                    else if(part[3].equals(QueryInterpretationService.PredicateMark))
//                            res.add(new RelationSuggestion(label, new Source(part[1],null, 0), "<"+part[0]+">", Double.parseDouble(part[2])));
//            }
////          System.out.println("Total Suggestion: "+res.size());
//            int conceptK = topK, relationK = topK;
//            LinkedList<Suggestion> ress = new LinkedList<Suggestion>();
//            for(Suggestion sug: res)
//            {
//                    if(sug instanceof ConceptSuggestion && conceptK>0)
//                    {
//                            System.out.println("ConceptSuggestion: "+sug.getURI());
//                            ress.add(sug);
//                            conceptK--;
//                    }
//                    else if(sug instanceof RelationSuggestion && relationK>0)
//                    {
//                            System.out.println("RelationSuggestion: "+sug.getURI());
//                            ress.add(sug);
//                            relationK--;
//                    }
//            }
//            
//            return ress;
//    }
//	
//	public SearchQ2SemanticService(String fn) {
//		q2semanticservice = new Q2SemanticService(fn);
//	}
//	
//	public LinkedList<QueryGraph> getPossibleGraphs(LinkedList<String> keywordList, int topNbGraphs) {
//		//	LinkedList<String> tmp = new LinkedList<String>();
//			q2semanticservice.param.topNbGraphs = topNbGraphs;
//			LinkedList<Subgraph> ret = q2semanticservice.getPossibleGraphs(keywordList);
//			LinkedList<QueryGraph> graphs = this.getQueryGraphFromTopKResult(ret);
//			
//			int count = 0;
//			for(QueryGraph graph : graphs) {
//				System.out.println("==============" +  "graph" + (++count) + "==========");
//				graph.print();
//				
//				System.out.println();
//			}
//			return graphs;
//	}
//		
//	private LinkedList<QueryGraph> getQueryGraphFromTopKResult(LinkedList<Subgraph> graphs) {
//		LinkedList<QueryGraph> result = new LinkedList<QueryGraph>();
//		if(graphs == null) return result;
//		
//		for(Subgraph qg: graphs)
//		{
//			Set<SummaryGraphEdge> edges = qg.edgeSet();
//			SummaryGraphElement from, to;
//			Map<Facet, Set<Facet>> con2rel = new HashMap<Facet, Set<Facet>>();
//			Map<Facet, Set<Facet>> con2attr = new HashMap<Facet, Set<Facet>>();
//			HashMap<Facet, Contain> attr2lit = new HashMap<Facet, Contain>();
//			HashMap<Facet, Contain> rel2con = new HashMap<Facet, Contain>();
//			for(SummaryGraphEdge edge: edges)
//			{
//				from = edge.getSource();
//				to = edge.getTarget();
//				collectEdge(from, to, con2rel, con2attr, rel2con, attr2lit);
//			}
//			
//			LinkedList<GraphEdge> graphEdges = new LinkedList<GraphEdge>();
//			LinkedList<Facet> graphVertexes = new LinkedList<Facet>();
//			
//			for(Facet f: con2rel.keySet()) {
//				for(Facet r: con2rel.get(f)) {
//					if(rel2con.get(r) != null) {
//						rel2con.get(r).isVisited = true;
//						for(Facet t: rel2con.get(r).sf) {
//							GraphEdge edge = new GraphEdge(f, t, r);
//							graphEdges.add(edge);
//						}
//					}
//					else {
//						Concept top_concept = new Concept("","<TOP_Category>",r.getSource());
//						graphVertexes.add(top_concept);
//						GraphEdge edge = new GraphEdge(f, top_concept, r);
//						graphEdges.add(edge);
//					}
//				}
//			}
//			
//			for(Facet f: con2attr.keySet()) {
//				for(Facet a: con2attr.get(f)) {
//					if(attr2lit.get(a) != null) {
//						attr2lit.get(a).isVisited = true;
//						for(Facet t: attr2lit.get(a).sf) {
//							GraphEdge edge = new GraphEdge(f, t, a);
//							graphEdges.add(edge);
//						}
//					}
//					else {
//						Concept top_concept = new Concept("","<TOP_Category>",a.getSource());
//						graphVertexes.add(top_concept);
//						GraphEdge edge = new GraphEdge(f, top_concept, a);
//						graphEdges.add(edge);
//					}
//				}
//			}
//			
//			for(Facet fac : rel2con.keySet()) {
//				if(!rel2con.get(fac).isVisited) {
//					for(Facet con : rel2con.get(fac).sf) {
//						Concept top_concept = new Concept("","<TOP_Category>",fac.getSource());
//						graphVertexes.add(top_concept);
//						graphEdges.add(new GraphEdge(top_concept,con,fac));
//					}
//				}
//			}
//			
//			for(Facet fac : attr2lit.keySet()) {
//				if(!attr2lit.get(fac).isVisited) {
//					for(Facet lit : attr2lit.get(fac).sf) {
//						Concept top_concept = new Concept("","<TOP_Category>",fac.getSource());
//						graphVertexes.add(top_concept);
//						graphEdges.add(new GraphEdge(top_concept,lit,fac));
//					}
//				}
//			}
//			
//			
//			for(SummaryGraphElement elem : qg.vertexSet()) {
//				if( elem.getType() == SummaryGraphElement.VALUE ||
//						elem.getType() == SummaryGraphElement.CONCEPT) {
//					graphVertexes.add(getFacet(elem));
//				}
//			}
//			
//			for(GraphEdge edge : graphEdges) {
//				edge.decorationElement.URI = SummaryGraphUtil.removeNum(edge.decorationElement.URI);
//				edge.decorationElement.label = SummaryGraphUtil.removeNum(edge.decorationElement.label);
//			}
////			================ by kaifengxu
//			LinkedList<GraphEdge> mappingEdges = new LinkedList<GraphEdge>();
//			for(SummaryGraphEdge mappingEdge: qg.edgeSet()){
//				if(mappingEdge.getEdgeLabel().equals(SummaryGraphEdge.MAPPING_EDGE))
//					mappingEdges.add(new GraphEdge(getFacet(mappingEdge.getSource()), getFacet(mappingEdge.getTarget()), null));
//			}
//			result.add(new QueryGraph(null, graphVertexes, graphEdges, mappingEdges));
//		}
//		this.addVariable(result);
//		return result;
//	}
//	
//	private void addVariable(LinkedList<QueryGraph> result) {
//		for (QueryGraph qg : result) {
//			char current_char = 'a';
//			HashMap<String, String> letter_hm = new HashMap<String, String>();
//			for (GraphEdge mapping : qg.mappingList) {
//				if ((mapping.fromElement instanceof Concept)
//						&& mapping.toElement instanceof Concept) {
//					Concept con1 = (Concept) mapping.fromElement;
//					Concept con2 = (Concept) mapping.toElement;
//					String l = letter_hm.get(con1.URI);
//					if(l == null) l = letter_hm.get(con2.URI);
//					if(l == null) l = String.valueOf(current_char++);
//					con1.variableLetter = l;
//					con2.variableLetter = l;
//					letter_hm.put(con1.URI, l);
//					letter_hm.put(con2.URI, l);
//				}
//			}
//			for (GraphEdge ge : qg.edgeList) {
//				if (ge.getFromElement() instanceof Concept) {
//					Concept con = (Concept) ge.getFromElement();
//					if(con.variableLetter == null)
//					{
//						if( letter_hm.get(con.URI) == null ) {
//							con.variableLetter = String.valueOf(current_char++);
//							letter_hm.put(con.URI, con.variableLetter);
//						}					
//						else {
//							con.variableLetter = letter_hm.get(con.URI);
//						}
//					}
//				}
//				if (ge.getToElement() instanceof Concept) {
//					Concept con = (Concept) ge.getToElement();
//					if(con.variableLetter == null)
//					{
//						if( letter_hm.get(con.URI) == null ) {
//							con.variableLetter = String.valueOf(current_char++);
//							letter_hm.put(con.URI, con.variableLetter);
//						}					
//						else {
//							con.variableLetter = letter_hm.get(con.URI);
//						}
//					}
//				}
//			}
//			for(Facet con: qg.vertexList)
//				if(con instanceof Concept)
//					((Concept)con).variableLetter = letter_hm.get(con.URI);
//		}
//	}
//	
//
//
//	/**
//	 * use to add a member value isVistited.
//	 * @author jqchen
//	 *
//	 */
//	class Contain{
//		Set<Facet> sf;
//		boolean isVisited;
//		
//		public Contain() {
//			this.sf = new HashSet<Facet>();
//			isVisited = false;
//		}
//		
//		public Contain(Set<Facet> sf) {
//			this.sf = sf;
//			isVisited = false;
//		}
//	}
//	
//	
//	
//	private void collectEdge(SummaryGraphElement from, SummaryGraphElement to, Map<Facet, Set<Facet>> c2r, Map<Facet, Set<Facet>> c2a, HashMap<Facet, Contain> rel2con, HashMap<Facet, Contain> attr2lit)
//	{
//		Facet f = getFacet(from);
//		Facet t = getFacet(to);
//		// == chenjunquan ==
//		if(from.getType() == SummaryGraphElement.ATTRIBUTE && to.getType() == SummaryGraphElement.VALUE)
//		{
//			
//			Contain contain = attr2lit.get(f);
//			if(contain == null) contain = new Contain();
//			contain.sf.add(t);
//			attr2lit.put(f, contain);
//		}
//		else if(from.getType() == SummaryGraphElement.CONCEPT && to.getType() == SummaryGraphElement.RELATION)
//		{
//			Set<Facet> set = c2r.get(f);
//			if(set == null) set = new HashSet<Facet>();
//			set.add(t);
//			c2r.put(f, set);
//		}
//		else if(from.getType() == SummaryGraphElement.RELATION && to.getType() == SummaryGraphElement.CONCEPT)
//		{
//			Contain contain = rel2con.get(f);
//			if(contain == null) contain = new Contain();
//			contain.sf.add(t);
//			rel2con.put(f, contain);
//		}
//		else if(from.getType() == SummaryGraphElement.CONCEPT && to.getType() == SummaryGraphElement.ATTRIBUTE)
//		{
//			Set<Facet> set = c2a.get(f);
//			if(set == null) set = new HashSet<Facet>();
//			set.add(t);
//			c2a.put(f, set);
//		}
//	}
//	
//	private Facet getFacet(SummaryGraphElement elem)
//	{
//		if(elem.getType() == SummaryGraphElement.ATTRIBUTE)
//		{
//			String uri = ((Property)elem.getResource()).getUri();
//			return new Relation(SummaryGraphUtil.getLocalName(uri), "<"+uri+">", new Source(elem.getDatasource(),null,0));
//		}
//		else if(elem.getType() == SummaryGraphElement.RELATION)
//		{
//			String uri = ((Property)elem.getResource()).getUri();
//			return new Relation(SummaryGraphUtil.getLocalName(uri), "<"+uri+">", new Source(elem.getDatasource(),null,0));
//		}
//		else if(elem.getType() == SummaryGraphElement.CONCEPT)
//		{
//			String uri = ((NamedConcept)elem.getResource()).getUri();
//			return new Concept(SummaryGraphUtil.getLocalName(uri), "<"+uri+">", new Source(elem.getDatasource(),null,0));
//		}
//		else if(elem.getType() == SummaryGraphElement.VALUE)
//		{
//			String uri = ((Literal)elem.getResource()).getLabel();
//			return new Literal(uri, uri, new Source(elem.getDatasource(),null,0));
//		}
//		System.out.println("Miss matching: "+elem.getType());
//		return null;
//	}
//	
//	public static void main(String[] args) {
//		SearchQ2SemanticService service = new SearchQ2SemanticService(args[0]);
//		int topk = Integer.parseInt(args[1]);
//		Scanner scanner = new Scanner(System.in);
//		while(true) {
//			System.out.println("Please input the keywords:");
//			String line = scanner.nextLine();
//			String tokens [] = line.split(" ");
//			LinkedList<String> keywordList = new LinkedList<String>();
//			for(int i=0;i<tokens.length;i++) {
//				keywordList.add(tokens[i]);
//			}
//			service.getPossibleGraphs(keywordList,topk);
//			
//		} 
//	}
//}
