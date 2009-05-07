package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.LVertex;
import edu.unika.aifb.graphindex.data.LVertexM;
import edu.unika.aifb.graphindex.data.ListVertexCollection;
import edu.unika.aifb.graphindex.data.MapVertexCollection;
import edu.unika.aifb.graphindex.data.VertexFactory;
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
import edu.unika.aifb.graphindex.preprocessing.FileHashValueProvider;
import edu.unika.aifb.graphindex.preprocessing.SortedVertexListBuilder;
import edu.unika.aifb.graphindex.preprocessing.TripleConverter;
import edu.unika.aifb.graphindex.preprocessing.TriplesPartitioner;
import edu.unika.aifb.graphindex.preprocessing.VertexListBuilder;
import edu.unika.aifb.graphindex.preprocessing.VertexListProvider;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
import edu.unika.aifb.graphindex.query.QueryParser;
import edu.unika.aifb.graphindex.query.model.Individual;
import edu.unika.aifb.graphindex.query.model.Literal;
import edu.unika.aifb.graphindex.query.model.Predicate;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.query.model.Variable;
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
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.Util;

public class Runner {

	private static final Logger log = Logger.getLogger(Runner.class);

	private static Query getQuery(String dataset) {
		Query q = new Query(Arrays.asList("?x", "?y", "?z"));
		
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
			
			String q4 = "?x http://example.org/simple#f http://example.org/simple#A1";

			String q5 = "?x http://example.org/simple#f ?y\n?y http://example.org/simple#f ?z";

			String q6 = "?x http://example.org/simple#f ?y\n?x http://example.org/simple#a ?z";
			
			String q7 = "?x http://example.org/simple#f ?y\n?y http://example.org/simple#f ?z\n?x http://example.org/simple#f ?f";
			
			String q8 = "?x http://example.org/simple#f http://example.org/simple#A2\n?x http://example.org/simple#f ?y\n?y http://example.org/simple#a ?z";
			
			return q4;
		}
		else if (dataset.equals("lubm")) {
			String q1 = "?x http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone ?y";

			String q2 = "?x http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone ?y\n" + 
				"?x http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent\n" +
				"?x http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse http://www.Department0.University0.edu/GraduateCourse0";

			String q3 = "?x http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone ?y\n" + 
				"?x http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent\n" +
				"?x http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse http://www.Department0.University0.edu/GraduateCourse0\n" +
				"?x http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse ?z";

			String q4 = "?x http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse ?y\n" +
				"?x http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse http://www.Department0.University0.edu/GraduateCourse0";
			
			return q4;
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

			String q15 = "?y http://xmlns.com/foaf/0.1/name 'Tarmo Uustalu'\n?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?y";//\n?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?z\n?a http://lsdis.cs.uga.edu/projects/semdis/opus#author ?z\n?a http://lsdis.cs.uga.edu/projects/semdis/opus#number '1'";
			
			String q16 = "?y http://xmlns.com/foaf/0.1/name 'Tarmo Uustalu'\n?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?y\n?x http://www.w3.org/1999/02/22-rdf-syntax-ns#type ?z";
			
			String q17 = "?y http://xmlns.com/foaf/0.1/name 'Tarmo Uustalu'\n?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?y\n?x http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://lsdis.cs.uga.edu/projects/semdis/opus#Article\n?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?z\n?z http://xmlns.com/foaf/0.1/name ?n";
			
			String q18 = "http://dblp.uni-trier.de/rec/bibtex/journals/iandc/CaprettaUV06 http://lsdis.cs.uga.edu/projects/semdis/opus#author ?x";
			
			String q19 = "?y http://xmlns.com/foaf/0.1/name 'Tarmo Uustalu'";
			
			String q20 = "?y http://xmlns.com/foaf/0.1/name 'Tarmo Uustalu'\n?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?y\n?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?z\n?z http://xmlns.com/foaf/0.1/name ?n";

			String q21 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?y\n ?y http://xmlns.com/foaf/0.1/name ?z";
			
			String q22 = "?x http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://xmlns.com/foaf/0.1/Person\n?y http://lsdis.cs.uga.edu/projects/semdis/opus#author ?x\n?y http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://xmlns.com/foaf/0.1/Document\n?y http://lsdis.cs.uga.edu/projects/semdis/opus#volume 'cs.AI/0003028'";
			
			String q23 = "?x http://lsdis.cs.uga.edu/projects/semdis/opus#author ?y\n?y http://xmlns.com/foaf/0.1/name ?z";
			
			return q19; 
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
			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm1/").listFiles()) {
				if (f.getName().startsWith("University")) {
					importer.addImport(f.getAbsolutePath());
//					break;
				}
			}
//			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm/more").listFiles())
//				if (f.getName().startsWith("University"))
//					importer.addImport(f.getAbsolutePath());
//			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm/more/muchmore").listFiles())
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
		else if (dataset.equals("chefmoz")) {
			importer = new RDFImporter();
			importer.addImport("/Users/gl/Studium/diplomarbeit/datasets/chefmoz.rest.rdf");
		}
		return importer;
 	}
	
	private static void calculateCardinalities(StructureIndexReader ir) throws StorageException, IOException, InterruptedException, ExecutionException {
		QueryEvaluator qe = ir.getQueryEvaluator();
		QueryParser qp = new QueryParser();
		Map<String,Integer> cmap = new HashMap<String,Integer>();
		
		for (String edge : ir.getIndex().getBackwardEdges()) {
			Query q = qp.parseQuery("?x " + edge + " ?y");
			q.createQueryGraph(ir.getIndex());
			List<String[]> result = qe.evaluate(q);
			log.debug(edge + ": " + result.size());
			Set<String> values = new HashSet<String>();
			for (String[] row : result)
				values.add(row[q.getSelectVariables().indexOf("?y")]);
			log.debug(edge + ": " + result.size() + " => " + values.size());
			cmap.put(edge, values.size());
		}
		log.debug(cmap);
	}
	
	/**
	 * @param args
	 * @throws StorageException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ExecutionException 
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws StorageException, IOException, InterruptedException, ExecutionException {
		OptionParser op = new OptionParser();
		op.accepts("a", "action to perform, comma separated list of: convert, partition, transform, index or query")
			.withRequiredArg().ofType(String.class).describedAs("action").withValuesSeparatedBy(',');
		op.accepts("p", "prefix, determines index directory")
			.withRequiredArg().ofType(String.class).describedAs("prefix");
		op.accepts("d", "dataset, either lubm or dblp, determines queries")
			.withRequiredArg().ofType(String.class).describedAs("dataset");
		op.accepts("q", "optional, name of query")
			.withRequiredArg().ofType(String.class).describedAs("query name");
		op.accepts("nodstes");
		op.accepts("nosrces");

		OptionSet os = op.parse(args);
		
		if (!os.has("a") || !os.has("p") || !os.has("d")) {
			op.printHelpOn(System.out);
			return;
		}
		
		Set<String> stages = new HashSet<String>((Collection<? extends String>)os.valuesOf("a"));
		String prefix = (String)os.valueOf("p");
		String dataset = (String)os.valueOf("d");
		String queryName = (String)os.valueOf("q");
		boolean dstUnmappedES = !os.has("nodstes");
		boolean srcUnmappedES = !os.has("nosrces");

		log.info("stages: " + stages);
		log.info("prefix: " + prefix);
		log.info("dataset: " + dataset);
		log.info("query name: " + queryName);
		
		String outputDirectory = "/Users/gl/Studium/diplomarbeit/workspace/graphindex/output/" + prefix;
		
		long start = System.currentTimeMillis();
		if (stages.contains("convert") || stages.contains("partition") || stages.contains("transform") || stages.contains("index")) {
			StructureIndexWriter iw = new StructureIndexWriter(outputDirectory, true);
			iw.setForwardEdgeSet(Util.readEdgeSet("/Users/gl/Studium/diplomarbeit/datasets/" + dataset + ".fw.txt"));
			iw.setBackwardEdgeSet(Util.readEdgeSet("/Users/gl/Studium/diplomarbeit/datasets/" + dataset + ".bw.txt"));
			iw.setImporter(getImporter(dataset));
			iw.create(stages);
//			iw.removeTemporaryFiles();
			iw.close();
		}
		
		if (stages.contains("temp")) {
			StructureIndexWriter iw = new StructureIndexWriter(outputDirectory, false);
			iw.removeTemporaryFiles();
		}
		
		if (stages.contains("query")) {
			String queriesFile, queryOutputDirectory;
			List<String> queryFiles = new ArrayList<String>();
			if (dataset.equals("sweto")) {
				queriesFile = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/dblpeva.txt";
				queryOutputDirectory = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/dblpqueries/";
				String dir = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/vldb2/dblp/";
//				queryFiles.add(dir + "AtomQuery_noResult.txt");
//				queryFiles.add(dir + "AtomQuery.txt");
//				queryFiles.add(dir + "StarQuery_noResult.txt");
				queryFiles.add(dir + "StarQuery.txt");
//				queryFiles.add(dir + "EntityQuery_noResult.txt");
//				queryFiles.add(dir + "EntityQuery.txt");
//				queryFiles.add(dir + "PathQuery_noResult.txt");
//				queryFiles.add(dir + "PathQuery.txt");
//				queryFiles.add(dir + "GraphQuery_noResult.txt");
//				queryFiles.add(dir + "GraphQuery.txt");
			}
			else if (dataset.equals("lubm")) {
				queriesFile = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/lubmeva.txt";
//				queriesFile = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/vldb2/lubm/AtomQuery.txt";
//				queriesFile = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/vldb2/lubm/EntityQuery.txt";
//				queriesFile = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/vldb2/lubm/PathQuery.txt";
//				queriesFile = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/vldb2/lubm/StarQuery.txt";
				queriesFile = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/vldb2/lubm/PathQuery.txt";
				queryOutputDirectory = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/lubmqueries/";
			}
			else {
				log.error("unknown dataset");
				return;
			}
			
			StructureIndexReader index = new StructureIndexReader(outputDirectory);
			index.setNumEvalThreads(2);
			index.getIndex().setTableCacheSize(1);
			index.getIndex().setDocumentCacheSize(1000);
			
//			calculateCardinalities(index);
//			System.exit(-1);
			QueryLoader ql = index.getQueryLoader();
			List<Query> queries = new ArrayList<Query>();
			if (queryFiles != null && queryFiles.size() > 0) {
				for (String file : queryFiles)
					queries.addAll(ql.loadQueryFile(file));
			}
			else
				queries.addAll(ql.loadQueryFile(queriesFile));
			log.debug(queries.size());
			log.debug("bw: " + ql.getBackwardEdgeSet().size() + " " + ql.getBackwardEdgeSet());
			log.debug("fw: " + ql.getForwardEdgeSet().size() + " " +ql.getForwardEdgeSet());
			
//			for (Query q : queries) {
//				PrintWriter out = new PrintWriter(new FileWriter(queryOutputDirectory + q.getName() + ".txt"));
//				out.print(q.toSPARQL());
//				out.close();
//			}
			
			QueryEvaluator qe = index.getQueryEvaluator();
			qe.getMLV().setDstExtSetup(dstUnmappedES, srcUnmappedES);
			
			String sizes = "";
			for (Query q : queries) {
				if (queryName != null && !queryName.equals(q.getName()))
					continue;
				log.debug("--------------------------------------------");
				log.debug("query: " + q.getName());
				log.debug(q);
//				q.createQueryGraph(index.getIndex());
				sizes += q.getName() + "\t" + qe.evaluate(q).size() + "\n";
				qe.clearCaches();
//				break;
			}
			System.out.println(sizes);
			index.close();
		}
		log.info("total time: " + (System.currentTimeMillis() - start) / 60000.0 + " minutes");
	}
}
