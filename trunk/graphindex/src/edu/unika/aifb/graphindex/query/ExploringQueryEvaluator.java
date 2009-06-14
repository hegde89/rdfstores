package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.exploring.EdgeElement;
import edu.unika.aifb.graphindex.query.exploring.ExploringIndexMatcher;
import edu.unika.aifb.graphindex.query.exploring.GraphElement;
import edu.unika.aifb.graphindex.query.exploring.NodeElement;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexGraphMatcher;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.KeywordSegement;
import edu.unika.aifb.keywordsearch.TransformedGraph;
import edu.unika.aifb.keywordsearch.TransformedGraphNode;
import edu.unika.aifb.keywordsearch.search.ApproximateStructureMatcher;
import edu.unika.aifb.keywordsearch.search.EntitySearcher;
import edu.unika.aifb.keywordsearch.search.KeywordSearcher;

public abstract class ExploringQueryEvaluator  {

	private StructureIndex m_schemaIndex, m_directIndex;
	private KeywordSearcher m_schemaKS, m_directKS;
	private ExploringIndexMatcher m_schemaMatcher, m_directMatcher;
	private IndexMatchesValidator m_schemaValidator, m_directValidator;
	private int m_cutoff;
	private boolean m_direct = true;
	
	private static final Logger log = Logger.getLogger(ExploringQueryEvaluator.class);
	
	public ExploringQueryEvaluator() throws StorageException {
	}
	
	public void setDirectEvaluation(boolean direct) {
		m_direct = direct;
	}
	
	public void setCutoff(int cutoff) {
		m_cutoff = cutoff;
	}
	
	protected void searchAndExplore(String query, ExploringIndexMatcher matcher, KeywordSearcher searcher, List<GTable<String>> indexMatches,
			List<Query> queries, List<Map<String,KeywordSegement>> selectMappings, Map<KeywordSegement,List<GraphElement>> segment2elements,
			Map<String,Set<String>> ext2entities, Timings timings, Counters counters) throws StorageException {
		timings.start(Timings.STEP_KWSEARCH);
		Map<KeywordSegement,Collection<KeywordElement>> entities = searcher.searchKeywordElements(KeywordSearcher.getKeywordList(query));
		timings.end(Timings.STEP_KWSEARCH);

		
		timings.start(Timings.STEP_EXPLORE);
		
		for (KeywordSegement ks : entities.keySet()) {
			Set<String> nodes = new HashSet<String>();
			Set<String> edges = new HashSet<String>();
			for (KeywordElement ele : entities.get(ks)) {
				if (ele.getType() == KeywordElement.CONCEPT || ele.getType() == KeywordElement.ENTITY) {
					String ext = ele.getExtensionId();
					
					Set<String> extEntities = ext2entities.get(ext + ks.toString());
					if (extEntities == null) {
						extEntities = new HashSet<String>(50);
						ext2entities.put(ext + ks.toString(), extEntities);
					}
					extEntities.add(ele.getUri());
					log.debug(ext + " " + ks.toString() + " " + extEntities);
					nodes.add(ext);
				}
				else if (ele.getType() == KeywordElement.RELATION || ele.getType() == KeywordElement.ATTRIBUTE) {
					edges.add(ele.getUri());
				}
				else
					log.error("unknown type...");
			}

			List<GraphElement> elements = new ArrayList<GraphElement>(nodes.size() + edges.size());
			for (String node : nodes) {
				elements.add(new NodeElement(node));
			}
			
			for (String uri : edges) {
				elements.add(new EdgeElement(null, uri, null));
			}
			
			segment2elements.put(ks, elements);
			
			log.debug("segment: " + ks + ", elements: " + elements);
		}
		

		matcher.setKeywords(segment2elements);
		matcher.match();
		
		matcher.indexMatches(indexMatches, queries, selectMappings, true);
	}
	
	public abstract void evaluate(String query) throws StorageException;
}
