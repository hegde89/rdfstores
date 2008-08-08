package edu.unika.aifb.graphindex;

import java.io.File;

import edu.unika.aifb.graphindex.graph.GraphManager;
import edu.unika.aifb.graphindex.graph.GraphStorageEngine;
import edu.unika.aifb.graphindex.graph.MySQLGraphStorage;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;

public class Runner {

	private static Importer getImporter(String dataset) {
		Importer importer = null;
		
		if (dataset.equals("simple")) {
			importer = new NTriplesImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/simple.nt");
		}
		else if (dataset.equals("wordnet")) {
			importer = new NTriplesImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/wordnet/wordnet_1m.nt");
		}
		else if (dataset.equals("lubm")) {
			importer = new OntologyImporter();
			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm/").listFiles()) {
				if (f.getName().startsWith("University")) {
					importer.addImport(f.getAbsolutePath());
				}
			}
//			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm/more").listFiles())
//				if (f.getName().startsWith("University"))
//					importer.addImport(f.getAbsolutePath());
		}
		else if (dataset.equals("swrc")) {
			importer = new OntologyImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/swrc/swrc_updated_v0.7.1.owl");
		}
		else if (dataset.equals("dbpedia")) {
			importer = new NTriplesImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/dbpedia/infobox_500k.nt");
		}
		else if (dataset.equals("dblp")) {
			importer = new NTriplesImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/swetodblp");
		}
		return importer;
	}
	
	/**
	 * @param args
	 * @throws StorageException 
	 */
	public static void main(String[] args) throws StorageException {
		if (args.length != 3) {
			System.out.println("Usage:\nRunner partition <prefix> <dataset>\nRunner query <prefix>");
			return;
		}
		
		ExtensionStorage es = new LuceneExtensionStorage("/Users/gl/Studium/diplomarbeit/workspace/graphindex/index/" + args[1]);
		ExtensionManager manager = new LuceneExtensionManager();
		manager.setExtensionStorage(es);
		
		StorageManager.getInstance().setExtensionManager(manager);
		
		if (args[0].equals("create")) {
			manager.initialize(true);
			
			GraphStorageEngine gmstorage = new MySQLGraphStorage(true);
			GraphManager.getInstance().setStorageEngine(gmstorage);
			
			gmstorage.setPrefix(args[1]);
			gmstorage.init();
			
			GraphBuilder gb = new GraphBuilder(false);
			Importer importer = getImporter(args[2]);
			
			importer.setGraphBuilder(gb);
			importer.doImport();
			
			IndexBuilder ib = new IndexBuilder(gb.getGraph());
			ib.buildIndex();
		}
		else if(args[0].equals("query")) {
			
		}
		
		manager.close();
	}

}
