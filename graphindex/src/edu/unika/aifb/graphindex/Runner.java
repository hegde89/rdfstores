package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.LVertex;
import edu.unika.aifb.graphindex.data.LVertexM;
import edu.unika.aifb.graphindex.data.ListVertexCollection;
import edu.unika.aifb.graphindex.data.MapVertexCollection;
import edu.unika.aifb.graphindex.data.VertexFactory;
import edu.unika.aifb.graphindex.data.VertexListBuilder;
import edu.unika.aifb.graphindex.data.VertexListProvider;
import edu.unika.aifb.graphindex.importer.ComponentImporter;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.indexing.FastIndexBuilder;
import edu.unika.aifb.graphindex.preprocessing.DatasetAnalyzer;
import edu.unika.aifb.graphindex.preprocessing.TriplesPartitioner;
import edu.unika.aifb.graphindex.query.Individual;
import edu.unika.aifb.graphindex.query.Literal;
import edu.unika.aifb.graphindex.query.Predicate;
import edu.unika.aifb.graphindex.query.Query;
import edu.unika.aifb.graphindex.query.Variable;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.GraphManagerImpl;
import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;
import edu.unika.aifb.graphindex.storage.lucene.LuceneGraphStorage;

public class Runner {

	private static final Logger log = Logger.getLogger(Runner.class);

	private static Query getQuery(String dataset) {
		Query q = new Query(new String[] {"?x", "?y", "?z"});
		
		if (dataset.equals("simple")) {
			Individual p1 = new Individual("http://example.org/simple#P1");
//			q.addLiteral(new Literal(new Predicate("http://example.org/simple#a"), new Variable("?x"), p1));
//			q.addLiteral(new Literal(new Predicate("http://example.org/simple#f"), new Variable("?x"), new Variable("?z")));
//			q.addLiteral(new Literal(new Predicate("http://example.org/simple#p"), p1, new Variable("?a")));
			
			Individual a1 = new Individual("http://example.org/simple#A2");
			q.addLiteral(new Literal(new Predicate("http://example.org/simple#f"), a1, new Variable("?y")));
			q.addLiteral(new Literal(new Predicate("http://example.org/simple#a"), new Variable("?y"), new Variable("?z")));

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
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/wordnet/wordnet.nt");
		}
		else if (dataset.equals("freebase")) {
			importer = new NTriplesImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/freebase/freebase_1m.nt");
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
		else if (dataset.equals("sweto")) {
			importer = new NTriplesImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/swetodblp/swetodblp_april_2008-mod.nt");
		}
		else if (dataset.equals("simple_components")) {
			importer = new ComponentImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/components/simple");
		}
		return importer;
	}
	
	/**
	 * @param args
	 * @throws StorageException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws StorageException, IOException {
		if (args.length != 3) {
			System.out.println("Usage:\nRunner partition <prefix> <dataset>\nRunner query <prefix> <dataset>");
			return;
		}
		
		ExtensionStorage es = new LuceneExtensionStorage("/Users/gl/Studium/diplomarbeit/workspace/graphindex/index/" + args[1]);
		ExtensionManager em = new LuceneExtensionManager();
		em.setExtensionStorage(es);
		
		GraphStorage gs = new LuceneGraphStorage("/Users/gl/Studium/diplomarbeit/workspace/graphindex/graph/" + args[1]);
		GraphManager gm = new GraphManagerImpl();
		gm.setGraphStorage(gs);
		
		StorageManager.getInstance().setExtensionManager(em);
		StorageManager.getInstance().setGraphManager(gm);
		
		long start = System.currentTimeMillis();
		if (args[0].equals("analyze") || args[0].equals("analyse")) {
			DatasetAnalyzer da = new DatasetAnalyzer();

			Importer importer = getImporter(args[2]);
			importer.setTripleSink(da);
			importer.doImport();
			
			da.printAnalysis();
		}
		else if (args[0].equals("partition")) {
			TriplesPartitioner tp = new TriplesPartitioner("components/" + args[1]);
			
			Importer importer = getImporter(args[2]);
			importer.setTripleSink(tp);
			importer.doImport();
			
			tp.write("components/" + args[1]);

		}
		else if (args[0].equals("transform")) {
			VertexFactory.setCollectionClass(MapVertexCollection.class);
			VertexFactory.setVertexClass(LVertexM.class);
			
			Importer importer = getImporter(args[2]);
			
			VertexListBuilder vb = new VertexListBuilder(importer, "components/" + args[1]);
			vb.write("components/" + args[1]);
		}
		else if (args[0].equals("create")) {
			VertexFactory.setCollectionClass(ListVertexCollection.class);
			VertexFactory.setVertexClass(LVertexM.class);

			em.initialize(true, false);
			gm.initialize(true, false);
			
			VertexListProvider vlp = new VertexListProvider("components/" + args[1]);
			HashValueProvider hvp = new HashValueProvider("components/" + args[1] + ".hashes");
			
			FastIndexBuilder ib = new FastIndexBuilder(vlp, hvp);
			ib.buildIndex();
			
//			IndexBuilder ib = new IndexBuilder(gb.getGraph());
//			ib.buildIndex();
		}
		else if(args[0].equals("query")) {
			em.initialize(false, true);
			gm.initialize(false, true);
			
			StructureIndex index = new StructureIndex();
			index.load();
			
			QueryEvaluator qe = new QueryEvaluator();
			qe.evaluate(getQuery(args[2]), index);
		}
		
		em.close();
		gm.close();
		
		log.info("total time: " + (System.currentTimeMillis() - start) / 60000.0 + " minutes");
	}

}
