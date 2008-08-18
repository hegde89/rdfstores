package edu.unika.aifb.graphindex;

import java.io.File;

import edu.unika.aifb.graphindex.algorithm.NaiveSubgraphMatcher;
import edu.unika.aifb.graphindex.algorithm.SubgraphMatcher;
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
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;

public class Runner {

	private static Query getQuery(String dataset) {
		Query q = new Query(new String[] {"?x", "?y", "?z"});
		
		if (dataset.equals("simple")) {
			Individual p1 = new Individual("http://example.org/simple#P1");
			q.addLiteral(new Literal(new Predicate("http://example.org/simple#a"), new Variable("?x"), p1));
			q.addLiteral(new Literal(new Predicate("http://example.org/simple#f"), new Variable("?x"), new Variable("?z")));
			q.addLiteral(new Literal(new Predicate("http://example.org/simple#p"), p1, new Variable("?a")));

//			q.addLiteral(new Literal(new Predicate("http://example.org/simple#m"), new Variable("?x"), new Variable("?y")));
//			q.addLiteral(new Literal(new Predicate("http://example.org/simple#o"), new Variable("?x"), new Variable("?z")));
//			q.addLiteral(new Literal(new Predicate("http://example.org/simple#k"), new Variable("?y"), new Variable("?z")));
			
//			q.addLiteral(new Literal(new Predicate("http://example.org/simple#f"), new Variable("?x"), new Variable("?y")));
//			q.addLiteral(new Literal(new Predicate("http://example.org/simple#f"), new Variable("?y"), new Variable("?z")));
//			q.addLiteral(new Literal(new Predicate("http://example.org/simple#f"), new Variable("?z"), new Variable("?y")));
			
//			q.addLiteral(new Literal(new Predicate("http://example.org/simple#subClassOf"), new Variable("?x"), new Variable("?y")));
		}
		else if (dataset.equals("lubm")) {
//			q.addLiteral(new Literal(new Predicate("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor"), new Variable("?x"), new Variable("?y")));
//			q.addLiteral(new Literal(new Predicate("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor"), new Variable("?y"), new Variable("?z")));
//			q.addLiteral(new Literal(new Predicate("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf"), new Variable("?y"), new Variable("?a")));
			
			q.addLiteral(new Literal(new Predicate("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), new Variable("?x"), new Individual("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent")));
			q.addLiteral(new Literal(new Predicate("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse"), new Variable("?x"), new Individual("http://www.Department0.University0.edu/GraduateCourse0")));

//			q.addLiteral(new Literal(new Predicate("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), new Variable("?x"), new Individual("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent")));
//			q.addLiteral(new Literal(new Predicate("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), new Variable("?y"), new Individual("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University")));
//			q.addLiteral(new Literal(new Predicate("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), new Variable("?z"), new Individual("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department")));
//			q.addLiteral(new Literal(new Predicate("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf"), new Variable("?x"), new Variable("?z")));
//			q.addLiteral(new Literal(new Predicate("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf"), new Variable("?z"), new Variable("?y")));
//			q.addLiteral(new Literal(new Predicate("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom"), new Variable("?x"), new Variable("?y")));
			
//			Individual prof0 = new Individual("http://www.Department0.University0.edu/FullProfessor0");
//			q.addLiteral(new Literal(new Predicate("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor"), prof0, new Variable("?x")));
//			q.addLiteral(new Literal(new Predicate("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf"), prof0, new Variable("?y")));
//			q.addLiteral(new Literal(new Predicate("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor"), prof0, new Variable("?x")));
		}
		else {
			return null;
		}
		
		return q;
	}
	
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
			System.out.println("Usage:\nRunner partition <prefix> <dataset>\nRunner query <prefix> <dataset>");
			return;
		}
		
		ExtensionStorage es = new LuceneExtensionStorage("/Users/gl/Studium/diplomarbeit/workspace/graphindex/index/" + args[1]);
		ExtensionManager manager = new LuceneExtensionManager();
		manager.setExtensionStorage(es);
		
		StorageManager.getInstance().setExtensionManager(manager);
		
		if (args[0].equals("create")) {
			manager.initialize(true, false);
			
			GraphStorageEngine gmstorage = new MySQLGraphStorage(true);
			GraphManager.getInstance().setStorageEngine(gmstorage);
			
			gmstorage.setPrefix(args[1]);
			gmstorage.init();
			
			GLGraphBuilder gb = new GLGraphBuilder();
			Importer importer = getImporter(args[2]);
			
			importer.setGraphBuilder(gb);
			importer.doImport();
			
			IndexBuilder ib = new IndexBuilder(gb.getGraph());
			ib.buildIndex();
		}
		else if(args[0].equals("query")) {
			manager.initialize(false, true);
			
			GraphStorageEngine gmstorage = new MySQLGraphStorage(false);
			GraphManager.getInstance().setStorageEngine(gmstorage);
			
			gmstorage.setPrefix(args[1]);
			gmstorage.init();
			
			Index index = new Index();
			index.load();
			
			SubgraphMatcher sm = new NaiveSubgraphMatcher();
			
			QueryEvaluator qe = new QueryEvaluator(sm);
			qe.evaluate(getQuery(args[2]), index);
		}
		
		manager.close();
	}

}
