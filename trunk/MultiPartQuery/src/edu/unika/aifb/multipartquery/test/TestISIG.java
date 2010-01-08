package edu.unika.aifb.multipartquery.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import edu.unika.aifb.MappingIndex.MappingIndex;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.integratedstruturedindex.IntegratedStructuredIndexGraph;

public class TestISIG {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Map<String, IndexReader> stdIdx = new HashMap<String, IndexReader>();

		String path = "C:\\Users\\Christoph\\Desktop\\AIFB\\Mappings";
		
		MappingIndex mIdx = null;
		
		try {
			mIdx = new MappingIndex(path, new IndexReader(new IndexDirectory(path)).getIndexConfiguration());
			
			stdIdx.put("C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index")));
			stdIdx.put("C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index")));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		IntegratedStructuredIndexGraph isig = new IntegratedStructuredIndexGraph(stdIdx, mIdx);
//		isig.createIExt();
//		isig.getGraph();
	}

}
