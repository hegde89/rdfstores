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
	
	private MultiPartQuery query;

	public MultiPartQueryEvaluator (MultiPartQuery mpquery) {
		query = mpquery;
	}
	
	public void evaluate() {
		Map<QueryEdge, String> map = query.getMap();
		
		// Build subqueries for each dataset
		Map<String, StructuredQuery> dsQuery = new HashMap<String, StructuredQuery>();
		
		for (Iterator<Entry<QueryEdge, String>> it = map.entrySet().iterator();it.hasNext();){
			Entry<QueryEdge, String> e = it.next();
			
			if (dsQuery.containsKey(e.getValue())) {
				dsQuery.get(e.getValue()).addEdge(e.getKey().getSource(), e.getKey().getProperty(), e.getKey().getTarget());
			} else {
				StructuredQuery sq = new StructuredQuery(e.getValue());
				sq.addEdge(e.getKey().getSource(), e.getKey().getProperty(), e.getKey().getTarget());
				dsQuery.put(e.getValue(), sq);
			}
		}
		
		// Subquery execution
		Map<String, Table<String>> result = new HashMap<String, Table<String>>();
		
		for (Iterator<Entry<String, StructuredQuery>> it = dsQuery.entrySet().iterator();it.hasNext();) {
			Entry<String, StructuredQuery> e = it.next();
			
			// Run the query
			try {
				IndexReader indexReader = new IndexReader(new IndexDirectory(e.getKey()));
				VPEvaluator qe = new VPEvaluator(indexReader);
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

	}
	
	private void join(Map<String, Table<String>> result, String ds1, String ds2, String node) {
		MappingIndex midx = query.getMappingIndex();
		// Get the mapping
		try {
			Table<String> mtable = midx.getStoTMapping(ds1, ds2);
			mtable.setColumnName(0, node);
			mtable.setColumnName(1, node+"tmp");
			mtable.sort(node);
			
			result.get(ds1).sort(node);
			
			Table<String> tmpResult = Tables.mergeJoin(result.get(ds1), mtable, node);
			tmpResult.sort(node+"tmp");
			
			result.get(ds2).sort(node);
			result.get(ds2).setColumnName(result.get(ds2).getColumn(node), node+"tmp");
			
			result.put(ds1+ds2, Tables.mergeJoin(tmpResult, result.get(ds2), node+"tmp"));
			result.remove(ds1);
			result.remove(ds2);
			
			//System.out.println(mtable.toDataString());
			//System.out.println(result.get(ds1).toDataString());
			System.out.println(result.get(ds1+ds2).toDataString());
			//System.out.println(tmpResult.toDataString());
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*public Table<String> evaluate(){
		// The result that will be returned
		Table<String> result = null;
		// Identifier of the data source used as source in mapping index
		String ds1;
		// Identifier of the data source used as destination in mapping index
		String ds2;
		// Value of the entity out of the source data source
		String e1;

		
		try {
			// Get iterator to parse the map and retrieve the queries
			Iterator<Entry<String, StructuredQuery>> it = query.getMap().entrySet().iterator();
			// Get first query
			Entry<String, StructuredQuery> e = it.next();
			// Identifier of the source data source is stored as key
			ds1 = e.getKey();
			// Get the index of this data source
			IndexReader indexReader = new IndexReader(new IndexDirectory(ds1));
			
			// Run the query
			VPEvaluator qe = new VPEvaluator(indexReader);
			result = qe.evaluate(e.getValue());
			// Value of the entity in the source data source is stored as query name
			e1 = e.getValue().getName();
			
			// If there are more than one query, retrieve the mapping of the entity between both data sources and join
			// the result with the mapping and then join the extended result with the result of the next query.
			while (it.hasNext()) {
				// Sort the result
				result.sort(e1);
	
				//System.out.print(indexReader.getDataIndex().getTriples(null, "http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary", "http://www4.wiwiss.fu-berlin.de/factbook/resource/Argentina").toDataString());
				//System.out.print(indexReader.getDataIndex().getTriples(null, "http://dbpedia.org/ontology/birthplace", "http://dbpedia.org/resource/Germany").toDataString());
				
				// Get next query
				e = it.next();
				// Identifier of the target data source is also stored as key
				ds2 = e.getKey();
				// Get the index of this data source
				indexReader = new IndexReader(new IndexDirectory(ds2));
				// Run the query
				qe = new VPEvaluator(indexReader);
				Table<String> result2 = qe.evaluate(e.getValue());
				
				//System.out.println(result[i].toDataString());
				
				// Value of the entity in the target data source is stored as query name
				String e2 = e.getValue().getName();

				// Get the mapping index between both data sources
				MappingIndex midx = query.getMappingIndex();
				// Get the mapping
				Table<String> mtable = midx.getStoTMapping(ds1, ds2, e1);
				
				//System.out.println(ds1 + " " + ds2 + " " + e1);
				
				// Assign the entity values to the corresponding column as column name, needed for the mergeJoin operation.
				// The columns in the result have the same name so that we know which columns are responsible for joining.
				mtable.setColumnName(0, e1);
				mtable.setColumnName(1, e2);
				
				//System.out.println(mtable.toDataString());
				
				// Sort the mapping
				mtable.sort(e1);
				// Sort the result of the second query
				result2.sort(e2);

				// Join the result of the first query with the mapping result, to know which entities in both
				// data sources are the same
				result = Tables.mergeJoin(result, mtable, e1);
				// Sort the result
				result.sort(e2);
				//System.out.println(joinresult.toDataString());

				// Join the extended result with the result of the second query
				result = Tables.mergeJoin(result, result2, e2);
				
				// Set the target data source and entity as source data source and entity
				// for the next mapping if there are another query
				e1 = e2;
				ds1 = ds2;

				System.out.println(result.toDataString(10));
			}
			
		} catch (StorageException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		
		return result;
		
	}*/
}
