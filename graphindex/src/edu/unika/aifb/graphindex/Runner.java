package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.LVertex;
import edu.unika.aifb.graphindex.data.LVertexM;
import edu.unika.aifb.graphindex.data.LVertexM2;
import edu.unika.aifb.graphindex.data.ListVertexCollection;
import edu.unika.aifb.graphindex.data.MapVertexCollection;
import edu.unika.aifb.graphindex.data.SortedVertexListBuilder;
import edu.unika.aifb.graphindex.data.VertexFactory;
import edu.unika.aifb.graphindex.data.VertexListBuilder;
import edu.unika.aifb.graphindex.data.VertexListProvider;
import edu.unika.aifb.graphindex.importer.ComponentImporter;
import edu.unika.aifb.graphindex.importer.HashingTripleConverter;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.importer.ParsingTripleConverter;
import edu.unika.aifb.graphindex.importer.RDFImporter;
import edu.unika.aifb.graphindex.importer.TriplesImporter;
import edu.unika.aifb.graphindex.indexing.FastIndexBuilder;
import edu.unika.aifb.graphindex.preprocessing.DatasetAnalyzer;
import edu.unika.aifb.graphindex.preprocessing.TripleConverter;
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
	
	private static String getQueryString(String dataset) {
		if (dataset.equals("simple")) {
			String q1 = "?x http://example.org/simple#a ?p\n" +
				"?y http://example.org/simple#a ?p\n" +
				"?x http://example.org/simple#f ?y";
			
			String q2 = "?x http://example.org/simple#is_a ?y\n ?y http://example.org/simple#subClassOf ?z \n " +
				"?a http://example.org/simple#is_a ?b\n ?b http://example.org/simple#subClassOf ?z";
			
			String q3 = "?x http://example.org/simple#subClassOf ?z\n ?y http://example.org/simple#subClassOf ?z";
			
			String q4 = "?x http://example.org/simple#f ?y";
			
			return q1;  
		}
		else if (dataset.equals("lubm")) {
			String q1 = "?x http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone ?y";

			String q2 = "?x http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone ?y\n" + 
				"?x http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent\n" +
				"?x http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse http://www.Department0.University0.edu/GraduateCourse0";

			return q2;
		}
		else if (dataset.equals("wordnet")) {
			String q1 = "http://www.w3.org/2006/03/wn/wn20/schema/AdjectiveSatelliteSynset http://www.w3.org/2000/01/rdf-schema#subClassOf http://www.w3.org/2006/03/wn/wn20/schema/AdjectiveSynset";
			
			String q2 = "?x http://www.w3.org/2000/01/rdf-schema#subClassOf http://www.w3.org/2006/03/wn/wn20/schema/AdjectiveSynset";
			
			String q3 = "?x http://www.w3.org/2000/01/rdf-schema#subClassOf ?y";
			
			return q2;
		}
		else if (dataset.equals("sweto")) {
			String q1 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#isbn ?y";
				
			String q2 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?y";
			
			String q3 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#isbn ?y\n?x http://www.w3.org/1999/02/22-rdf-syntax-ns#type ?z";
			
			String q4 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#isbn '3-540-22116-6'\n?x http://www.w3.org/1999/02/22-rdf-syntax-ns#type ?z";
			
			String q5 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#journal_name 'Pattern Recognition'\n?x http://www.w3.org/1999/02/22-rdf-syntax-ns#type ?y";
			
			String q6 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#journal_name ?n";
			
			String q7 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#journal_name 'Pattern Recognition'\n?x http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://lsdis.cs.uga.edu/projects/semdis/opus#Article";
			
			String q8 = "?x http://xmlns.com/foaf/0.1/name 'Anurag Garg'";
			
			String q9 = "?x http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://xmlns.com/foaf/0.1/Agent";
			
			String q10 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#pages '253-254'";
			
			String q11 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#number '1'\n?x http://lsdis.cs.uga.edu/projects/semdis/opus#pages '39-48'\n?x http://www.w3.org/2000/01/rdf-schema#label 'Parametric characterization of the form of the human pupil from blurred noisy images.'";
			
			String q12 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?y\n ?x http://lsdis.cs.uga.edu/projects/semdis/opus#in_series ?z\n?z http://lsdis.cs.uga.edu/projects/semdis/opus#book_title 'WWW'";

			String q13 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#in_series ?y";
			
			String q14 = "?y http://xmlns.com/foaf/0.1/name 'Tarmo Uustalu'\n?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?y\n?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?z\n?a http://lsdis.cs.uga.edu/projects/semdis/opus#author ?z\n?a http://lsdis.cs.uga.edu/projects/semdis/opus#number '1'";

			return q11; 
		}
		
		return null;
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
//					break;
				}
			}
			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm/more").listFiles())
				if (f.getName().startsWith("University"))
					importer.addImport(f.getAbsolutePath());
			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm/more/muchmore").listFiles())
				if (f.getName().startsWith("University"))
					importer.addImport(f.getAbsolutePath());
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
		else if (dataset.equals("chefmoz")) {
			importer = new RDFImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/chefmoz.rest.rdf");
		}
		return importer;
 	}
	
	/**
	 * @param args
	 * @throws StorageException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ExecutionException 
	 */
	public static void main(String[] args) throws StorageException, IOException, InterruptedException, ExecutionException {
		if (args.length < 3) {
			System.out.println("Usage:\nRunner [convert|partition|transform|create]+ <prefix> <dataset>");
			return;
		}
		
		Set<String> stages = new HashSet<String>();
		for (int i = 0; i < args.length - 2; i++)
			stages.add(args[i]);
		String prefix = args[args.length - 2];
		String dataset = args[args.length - 1];
		
		log.info("stages: " + stages);
		log.info("prefix: " + prefix);
		log.info("dataset: " + dataset);
		
		String outputDirectory = "/Users/gl/Studium/diplomarbeit/workspace/graphindex/output/" + prefix;
		
		ExtensionStorage es = new LuceneExtensionStorage(outputDirectory + "/index");
		ExtensionManager em = new LuceneExtensionManager();
		em.setExtensionStorage(es);
		
		GraphStorage gs = new LuceneGraphStorage(outputDirectory + "/graph");
		GraphManager gm = new GraphManagerImpl();
		gm.setGraphStorage(gs);
		
		StorageManager.getInstance().setExtensionManager(em);
		StorageManager.getInstance().setGraphManager(gm);
		
		long start = System.currentTimeMillis();
		if (stages.contains("analyze") || stages.contains("analyse")) {
			DatasetAnalyzer da = new DatasetAnalyzer();

			Importer importer = getImporter(dataset);
			importer.setTripleSink(da);
			importer.doImport();
			
			da.printAnalysis();
		}
		
		if (stages.contains("convert")) {
			log.info("stage: CONVERT");
			TripleConverter tc = new TripleConverter(outputDirectory);
			
			Importer importer = getImporter(dataset);
			importer.setTripleSink(tc);
			importer.doImport();
			
			tc.write();
			
			log.info("sorting...");
			Util.sortFile(outputDirectory + "/input.ht", outputDirectory + "/input_sorted.ht");
			log.info("sorting complete");
		}
		
		if (stages.contains("partition")) {
			log.info("stage: PARTITION");
			TriplesPartitioner tp = new TriplesPartitioner(outputDirectory + "/components");
			
			Importer importer = new TriplesImporter();
			importer.addImport(outputDirectory + "/input_sorted.ht");
			importer.setTripleSink(new ParsingTripleConverter(tp));
			importer.doImport();
			
			tp.write();
		} 
		
		if (stages.contains("transform")) {
			log.info("stage: TRANSFORM");
			VertexFactory.setCollectionClass(MapVertexCollection.class);
			VertexFactory.setVertexClass(LVertexM.class);
			
			Importer importer = new TriplesImporter();
			importer.addImport(outputDirectory + "/input_sorted.ht");
			
			SortedVertexListBuilder vb = new SortedVertexListBuilder(importer, outputDirectory + "/components");
			vb.write();
		}
		
		if (stages.contains("create")) {
			log.info("stage: CREATE");
			VertexFactory.setCollectionClass(ListVertexCollection.class);
			VertexFactory.setVertexClass(LVertexM.class);

			em.initialize(true, false);
			gm.initialize(true, false);
			
			VertexListProvider vlp = new VertexListProvider(outputDirectory + "/components/");
			HashValueProvider hvp = new HashValueProvider(outputDirectory + "/hashes", outputDirectory + "/propertyhashes");
			
			FastIndexBuilder ib = new FastIndexBuilder(vlp, hvp);
			ib.buildIndex();
			
			((LuceneExtensionStorage)es).mergeExtensions();
		}
		
		if(stages.contains("query")) {
			em.initialize(false, true);
			gm.initialize(false, true);
			
			StructureIndex index = new StructureIndex();
			index.load();
			
			QueryParser qp = new QueryParser();
			Query q = qp.parseQuery(getQueryString(args[2]));
			
			QueryEvaluator qe = new QueryEvaluator(index);
			qe.evaluate(q);
		}
		
		if (stages.contains("optimize")) {
			em.initialize(false, false);
			gm.initialize(false, false);
			
			em.getExtensionStorage().optimize();
			gm.getGraphStorage().optimize();
		}
		
		em.close();
		gm.close();
		
		log.info("total time: " + (System.currentTimeMillis() - start) / 60000.0 + " minutes");
	}
}
