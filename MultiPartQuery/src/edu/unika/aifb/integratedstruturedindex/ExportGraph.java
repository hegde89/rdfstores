package edu.unika.aifb.integratedstruturedindex;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public class ExportGraph {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			exportGraph("Arbeitgeber", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\Arbeitgeber\\index")));
			exportGraph("Example", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\Example\\index")));
			exportGraph("Kuehlschrank", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\Kuehlschrank\\index")));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	

	}
	
	private static void exportGraph(String filename, IndexReader m_idxReader) {
		String path = "C:\\Users\\Christoph\\Desktop\\AIFB\\" + filename + ".dot";
		
		try {
			  FileWriter outFile = new FileWriter(path);
			  PrintWriter out = new PrintWriter(outFile);
			  
			  out.println("digraph G {");
			  System.out.println("digraph G {");
					
//					IndexReader m_idxReader = e.getValue();
//					Map<String, Table<String>> m_p2to = new HashMap<String, Table<String>>();
			Map<String, Table<String>> m_p2ts = new HashMap<String, Table<String>>();
			
			try {
				IndexStorage gs = m_idxReader.getStructureIndex().getGraphIndexStorage();
				
				for (String property : m_idxReader.getObjectProperties()) {
					m_p2ts.put(property, gs.getIndexTable(IndexDescription.POS, DataField.SUBJECT, DataField.OBJECT, property));
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
			
			for (Iterator<Entry<String, Table<String>>> tit = m_p2ts.entrySet().iterator(); tit.hasNext();) {
				Entry<String, Table<String>> te = tit.next();
				
				String p = te.getKey();
				Table<String> t = te.getValue();
				
				System.out.println(p);
				System.out.println(t.toDataString());
				
				
				// Iterate through the rows and add the new edge for each row to the ISIG. We check
				// if the extension is already known we use the IntegratedExtension where extension was
				// integrated into. If it is unknown, we create a new IntegratedExtension for this extension
				// and add it to the list of known ones.
				
				for (Iterator<String[]> rowIt = t.iterator(); rowIt.hasNext();) {
					String[] row = rowIt.next();
		  
					out.println(row[0] + " -> " + row[1] + " [label=\"" + p + "\"]");
					System.out.println(row[0] + " -> " + row[1] + " [label=\"" + p + "\"]");
				}
			}
			out.println("}");
			System.out.println("}");
			out.close();
		} catch (IOException e){
		   e.printStackTrace();
		}	
		
	}

}
