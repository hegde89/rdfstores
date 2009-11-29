package edu.unika.aifb.multipartquery;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.unika.aifb.MappingIndex.MappingIndex;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.structured.QueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.StructuredQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;

public class MultiPartQueryEvaluator {
	
	private MultiPartQuery mpquery;
	private Map<String, IndexReader> idxReaders;
	
	public MultiPartQueryEvaluator (Map<String, IndexReader> idxReaders) {
		this.idxReaders = idxReaders;
	}
	
	public Table<String> evaluate(MultiPartQuery q) {
		this.mpquery = q;
		
		// Get subqueries for each dataset
		Map<String, StructuredQuery> dsQuery = q.getDatasetQueries();
		
		// Subquery execution
		Map<String, Table<String>> result = new HashMap<String, Table<String>>();
		
		for (Iterator<Entry<String, StructuredQuery>> it = dsQuery.entrySet().iterator();it.hasNext();) {
			Entry<String, StructuredQuery> e = it.next();
			
			// Run the query
			try {
				//IndexReader indexReader = new IndexReader(new IndexDirectory(e.getKey()));
				
				VPEvaluator qe = new VPEvaluator(idxReaders.get(e.getKey()));
				//VPEvaluator qe = new VPEvaluator(indexReader);
				//QueryEvaluator qe = new QueryEvaluator(indexReader);
				result.put(e.getKey(), qe.evaluate(e.getValue()));
				
			} catch (StorageException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		
		Set<String> datasets = new HashSet<String>();
		
		// Iterate the map
		for (Iterator<Entry<String, StructuredQuery>> it1 = dsQuery.entrySet().iterator();it1.hasNext();){
			Entry<String, StructuredQuery> e1 = it1.next();
			
			datasets.add(e1.getKey());
			
			// Iterate the map to get the other entries
			for (Iterator<Entry<String, StructuredQuery>> it2 = dsQuery.entrySet().iterator();it2.hasNext();){
				Entry<String, StructuredQuery> e2 = it2.next();
				
				// Only different entries are interesting
				if (!e1.getKey().equals(e2.getKey()) && datasets.add(e2.getKey())) {
					for (Iterator<QNode> node1_it = e1.getValue().getQueryGraph().vertexSet().iterator();node1_it.hasNext();) {
						
						QNode n1 = node1_it.next();
						
						// Iterate nodes
						for (Iterator<QNode> node2_it = e2.getValue().getQueryGraph().vertexSet().iterator();node2_it.hasNext();) {
							QNode n2 = node2_it.next();
							
							if (n1.equals(n2)) {
								//TODO: Join the result of these two subqueries								
								join(result, e1.getKey(), e2.getKey(), n1.getLabel());
								System.out.println("Found node " + n1.getLabel() + " in subquerie for " + e1.getKey() + " and " + e2.getKey());
							}
						}
					}
					
				}
				
			}
		}
		Set<Table<String>> joinedResult = new HashSet<Table<String>>();
		for (Iterator<Table<String>> it = result.values().iterator();it.hasNext();) {
			joinedResult.add(it.next());
		}
		
		System.out.println("joinResult: " + joinedResult.size());
		return joinedResult.iterator().next();

	}
	
	private void join(Map<String, Table<String>> result, String ds1, String ds2, String node) {
		MappingIndex midx = mpquery.getMappingIndex();
		// Get the mapping
		try {
			// Get mapping between ds1 and ds2
			Table<String> mtable = midx.getStoTMapping(ds1, ds2);
			// Set column names of the mapping result
			mtable.setColumnName(0, node);
			mtable.setColumnName(1, node+"tmp");
			// Sort mapping table to prepare for merge join
			mtable.sort(node);
			// Sort result of query of ds1 to prepare for merge join
			result.get(ds1).sort(node);
			// Join the result of ds1 with the mapping result
			Table<String> tmpResult = Tables.mergeJoin(result.get(ds1), mtable, node);
			// Sort for the second join with ds2
			tmpResult.sort(node+"tmp");
			// Sort result of ds2
			result.get(ds2).sort(node);
			// Rename variable of ds2 to match temporary variable of the mapping
			result.get(ds2).setColumnName(result.get(ds2).getColumn(node), node+"tmp");
			// Join the result of the previous join with ds2
			Table<String> joinResult = Tables.mergeJoin(tmpResult, result.get(ds2), node+"tmp");
			// Rename the variable to the variable name without the leading question mark
			joinResult.setColumnName(joinResult.getColumn(node+"tmp"), node.substring(1));
			// Remove the results of ds1 and ds2 to replace them with the joined result
			result.remove(ds1);
			result.remove(ds2);
			//result.put(ds1+ds2, Tables.mergeJoin(tmpResult, result.get(ds2), node+"tmp"));
			// Save the result under both keys to get easy
			result.put(ds1, joinResult);
			result.put(ds2, joinResult);
			
			//result.get(ds2).addRow(new String[]{"test", "test", "test", "test"});
			//System.out.println(mtable.toDataString());
			//System.out.println(result.get(ds1).toDataString());
			System.out.println(result.get(ds1).toDataString());
			//System.out.println(tmpResult.toDataString());
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
