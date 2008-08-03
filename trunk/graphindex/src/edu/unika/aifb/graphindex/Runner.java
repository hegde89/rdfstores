package edu.unika.aifb.graphindex;

import java.io.File;

import edu.unika.aifb.graphindex.extensions.ExtensionManager;
import edu.unika.aifb.graphindex.extensions.MySQLExtensionStorage;
import edu.unika.aifb.graphindex.graph.GraphManager;
import edu.unika.aifb.graphindex.graph.GraphStorageEngine;
import edu.unika.aifb.graphindex.graph.MySQLGraphStorage;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;

public class Runner {

	private static Importer getImporter(String dataset) {
		Importer importer = null;
		
		if (dataset.equals("simple")) {
			importer = new NTriplesImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/simple.nt");
		}
		else if (dataset.equals("wordnet")) {
			importer = new NTriplesImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/wordnet/wordnet_100k.nt");
		}
		else if (dataset.equals("lubm")) {
			importer = new OntologyImporter();
			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm/").listFiles()) {
				if (f.getName().startsWith("University")) {
					importer.addImport(f.getAbsolutePath());
				}
			}
			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm/more").listFiles())
				if (f.getName().startsWith("University"))
					importer.addImport(f.getAbsolutePath());
		}
		else if (dataset.equals("swrc")) {
			importer = new OntologyImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/swrc/swrc_updated_v0.7.1.owl");
		}
		
		return importer;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage:\nRunner partition <prefix> <dataset>\nRunner merge <prefix>\nRunner query <prefix>");
			return;
		}
		
		if (args[0].equals("partition")) {
			MySQLExtensionStorage emstorage = new MySQLExtensionStorage(true);
			ExtensionManager.getInstance().setStorageEngine(emstorage);
			GraphStorageEngine gmstorage = new MySQLGraphStorage(true);
			GraphManager.getInstance().setStorageEngine(gmstorage);
			
			emstorage.setPrefix(args[1]);
			gmstorage.setPrefix(args[1]);
			
			emstorage.init();
			gmstorage.init();
			
			GraphBuilder gb = new GraphBuilder(false);
			Importer importer = getImporter(args[2]);
			
			importer.setGraphBuilder(gb);
			importer.doImport();
			
			IndexBuilder ib = new IndexBuilder(gb.getGraph());
			ib.buildIndex(1);
		}
		else if (args[0].equals("merge")) {
			MySQLExtensionStorage emstorage = new MySQLExtensionStorage(false);
			ExtensionManager.getInstance().setStorageEngine(emstorage);
			GraphStorageEngine gmstorage = new MySQLGraphStorage(false);
			GraphManager.getInstance().setStorageEngine(gmstorage);
			
			emstorage.setPrefix(args[1]);
			gmstorage.setPrefix(args[1]);
			
			emstorage.init();
			gmstorage.init();
			
			IndexBuilder ib = new IndexBuilder(null);
			ib.buildIndex(2);
		}
		else if(args[0].equals("query")) {
			
		}
	}

}
