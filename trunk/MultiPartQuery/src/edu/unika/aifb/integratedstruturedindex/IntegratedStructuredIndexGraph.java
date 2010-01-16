package edu.unika.aifb.integratedstruturedindex;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.MappingIndex.MappingIndex;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public class IntegratedStructuredIndexGraph extends
		DirectedMultigraph<IntegratedExtension, IntegratedEdge> {
	
	private Map<String, IndexReader> structuredIndexes;
	private Map<String, IntegratedExtension> iExts = new HashMap<String, IntegratedExtension>();
	private Map<String, IntegratedEdge> iEdges = new HashMap<String, IntegratedEdge>();
	MappingIndex mIdx;

	public IntegratedStructuredIndexGraph(Map<String, IndexReader> stdIdx, MappingIndex mIdx) {
		super(IntegratedEdge.class);
		this.structuredIndexes = stdIdx;
		this.mIdx = mIdx;
		createIExt();
		getGraph();
	}
	
	public void createIExt() {
		Set<String> processed = new HashSet<String>();
		
		for (Iterator<String> it1 = structuredIndexes.keySet().iterator(); it1.hasNext();) {
			String ds1 = it1.next();
			
			for (Iterator<String> it2 = structuredIndexes.keySet().iterator(); it2.hasNext();) {
				String ds2 = it2.next();
				
				if (processed.add(ds1+ds2) && processed.add(ds2+ds1) && !ds1.equals(ds2)) {
					try {
						Table<String> mapping = mIdx.getStoTExtMapping(ds1, ds2);
						
						if (mapping.rowCount() == 0) {
							mapping = mIdx.getStoTExtMapping(ds2, ds1);
						}
						
						if (mapping.rowCount() > 0) {
							
							for (Iterator<String[]> rowIt = mapping.iterator(); rowIt.hasNext();) {
								String[] row = rowIt.next();
								if (iExts.containsKey(row[0]) && iExts.containsKey(row[1])) {
//									IntegratedExtension iExt = iExts.get(row[0]);
//									iExt.addExt(row[1]);
									// Both extensions already integrated. Merge IExts, if they are not equal.
									if (iExts.get(row[0]) != iExts.get(row[1])) {
										IntegratedExtension iExt = iExts.get(row[0]);
										for (Iterator<String> listIt = iExts.get(row[1]).iterator(); listIt.hasNext();) {
											String s = listIt.next();
											iExt.addExt(s);
											iExts.remove(s);
											iExts.put(s, iExt);
										}
										
									} else {
										System.out.println("Mapping " + row[0] + " -> " + row[1] + " already in the same IExt.");
									}
									
								} else if (iExts.containsKey(row[0]) && !iExts.containsKey(row[1])) {
									IntegratedExtension iExt = iExts.get(row[0]);
									iExt.addExt(row[1]);
									iExts.put(row[1], iExt);
								} else if (iExts.containsKey(row[1]) && !iExts.containsKey(row[0])) {
									IntegratedExtension iExt = iExts.get(row[1]);
									iExt.addExt(row[0]);
									iExts.put(row[0], iExt);
								} else {
									IntegratedExtension iExt = new IntegratedExtension();
									iExt.addExt(row[0]);
									iExt.addExt(row[1]);
									iExts.put(row[0], iExt);
									iExts.put(row[1], iExt);
								}
							}
						}
					} catch (StorageException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		//DEBUG
		Set<IntegratedExtension> nodes = new HashSet<IntegratedExtension>();
		for (Iterator<IntegratedExtension> it = iExts.values().iterator(); it.hasNext();) {
			IntegratedExtension iExt = it.next();
			if (nodes.add(iExt)) {
				System.out.println("\n");
				for (Iterator<String> extIt = iExt.iterator(); extIt.hasNext();) {
					System.out.print(extIt.next() + " ");
				}
				System.out.println("\n");
			}
		}
		
	}
	
	public void getGraph() {
		for (Iterator<Entry<String, IndexReader>> it = structuredIndexes.entrySet().iterator();it.hasNext();) {
			Entry<String, IndexReader> e = it.next();
			String ds = e.getKey();
			
			IndexReader m_idxReader = e.getValue();
//			Map<String, Table<String>> m_p2to = new HashMap<String, Table<String>>();
			Map<String, Table<String>> m_p2ts = new HashMap<String, Table<String>>();
			
			try {
				IndexStorage gs = m_idxReader.getStructureIndex().getGraphIndexStorage();
				
				for (String property : m_idxReader.getObjectProperties()) {
					m_p2ts.put(property, gs.getIndexTable(IndexDescription.POS, DataField.SUBJECT, DataField.OBJECT, property));
//					m_p2to.put(property, gs.getIndexTable(IndexDescription.PSO, DataField.SUBJECT, DataField.OBJECT, property));
				}
			
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (StorageException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			for (Table<String> t : m_p2ts.values()) {
				t.sort(0);
			}
//			for (Table<String> t : m_p2to.values()) {
//				t.sort(1);
//			}
			
			for (Iterator<Entry<String, Table<String>>> tit = m_p2ts.entrySet().iterator(); tit.hasNext();) {
				Entry<String, Table<String>> te = tit.next();
				String p = te.getKey();
				Table<String> t = te.getValue();
				
				System.out.println(t.toDataString());
				
				
				// Iterate through the rows and add the new edge for each row to the ISIG. We check
				// if the extension is already known we use the IntegratedExtension where extension was
				// integrated into. If it is unknown, we create a new IntegratedExtension for this extension
				// and add it to the list of known ones.
				for (Iterator<String[]> rowIt = t.iterator(); rowIt.hasNext();) {
					String[] row = rowIt.next();
					
					String sExt = row[0];
					String oExt = row[1];
					
					IntegratedExtension iSub;
					IntegratedExtension iObj;
					
					if (iExts.containsKey(sExt)) {
						iSub = iExts.get(sExt);
					} else {
						iSub = new IntegratedExtension();
						iSub.addExt(row[0]);
						iExts.put(row[0], iSub);
					}
					
					if (iExts.containsKey(oExt)) {
						iObj = iExts.get(row[1]);
					} else {
						iObj = new IntegratedExtension();
						iObj.addExt(row[1]);
						iExts.put(row[1], iObj);
					}
//					System.out.println(this.addVertex(iSub) ? "true" : "false");
//					System.out.println(this.addVertex(iObj) ? "true" : "false");
					this.addVertex(iSub);
					this.addVertex(iObj);
					
					//TODO: Check if property is already known. In this case the extensions
					// have to be merged, if they are not already integrated into the same.
					
					IntegratedEdge iEdge = null;
					
//					System.out.println("Multiple Edges? " + (this.isAllowingMultipleEdges() ? "true" : "false"));
					for (Iterator<IntegratedEdge> edgeIt = this.getAllEdges(iSub, iObj).iterator(); edgeIt.hasNext();) {
						IntegratedEdge edge = edgeIt.next();
						if (edge.getLabel() == p) {
							System.out.println("Edge " + p + " already exists for " + sExt + "->" + oExt +"!");
							iEdge = edge;
						}
					}
					
					if (iEdge != null) {
						System.out.println("Add DS " + ds + " to edge " + p + " for " + sExt + "->" + oExt);
						iEdge.addDS(ds);
					} else {
						System.out.println("Add new edge " + p + " to graph for " + sExt + "->" + oExt +"!");
						iEdge = this.addEdge(iSub, iObj);
						iEdge.setLabel(p);
						iEdge.addDS(ds);
					}
				}
			}
		}
	}

}
