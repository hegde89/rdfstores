package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;

import org.jgrapht.DirectedGraph;

import edu.unika.aifb.graphindex.algorithm.NaiveSubgraphMatcher;
import edu.unika.aifb.graphindex.algorithm.SubgraphMatcher;
import edu.unika.aifb.graphindex.extensions.ExtensionManager;
import edu.unika.aifb.graphindex.extensions.ExtensionStorageEngine;
import edu.unika.aifb.graphindex.extensions.MySQLExtensionStorage;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphManager;
import edu.unika.aifb.graphindex.graph.GraphStorageEngine;
import edu.unika.aifb.graphindex.graph.MySQLGraphStorage;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.query.Individual;
import edu.unika.aifb.graphindex.query.Literal;
import edu.unika.aifb.graphindex.query.Predicate;
import edu.unika.aifb.graphindex.query.Query;
import edu.unika.aifb.graphindex.query.Variable;

public class IndexTest {
	private static final String dotOut = "out.dot";
	private static final String partOut = "partitions.dot";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		MySQLExtensionStorage emstorage = new MySQLExtensionStorage(true);
		ExtensionManager.getInstance().setStorageEngine(emstorage);
		GraphStorageEngine gmstorage = new MySQLGraphStorage(true);
		GraphManager.getInstance().setStorageEngine(gmstorage);
		
		GraphBuilder gb = new GraphBuilder(false);
		Importer importer = null;
		
		int test = 2;
		
		if (test == 0) {
			emstorage.setPrefix("simple");
			gmstorage.setPrefix("simple");
			importer = new NTriplesImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/simple.nt");
		}
		else if (test == 1) {
			emstorage.setPrefix("wn");
			gmstorage.setPrefix("wn");
			importer = new NTriplesImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/wordnet/wordnet_100k.nt");
		}
		else if (test == 2) {
			emstorage.setPrefix("lubm");
			gmstorage.setPrefix("lubm");
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
		else if (test == 3) {
			emstorage.setPrefix("swrc");
			gmstorage.setPrefix("swrc");
			importer = new OntologyImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/swrc/swrc_updated_v0.7.1.owl");
		}
		
		emstorage.init();
		gmstorage.init();
		
		importer.setGraphBuilder(gb);
		importer.doImport();
		
		IndexBuilder ib = new IndexBuilder(gb.getGraph());
		ib.buildIndex();
	}
}
