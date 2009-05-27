package edu.unika.aifb.graphindex.vp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.importer.ComponentImporter;
import edu.unika.aifb.graphindex.importer.HashedTripleSink;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.importer.ParsingTripleConverter;
import edu.unika.aifb.graphindex.importer.RDFImporter;
import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.importer.TriplesImporter;
import edu.unika.aifb.graphindex.preprocessing.FileHashValueProvider;
import edu.unika.aifb.graphindex.query.QueryParser;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

public class VPRunner {
	
	private static final Logger log = Logger.getLogger(VPRunner.class);

	private static class HashedTriplesImporter extends Importer {
		private HashValueProvider m_hash;

		public HashedTriplesImporter(String hashes, String propertyHashes) throws IOException {
			m_hash = new FileHashValueProvider(hashes, propertyHashes);
		}

		@Override
		public void doImport() {
			final Importer i = new TriplesImporter();
			for (String file : m_files)
				i.addImport(file);
			i.setTripleSink(new ParsingTripleConverter(new HashedTripleSink() {
				public void triple(long s, long p, long o, String objectType) {
					m_sink.triple(m_hash.getValue(s), m_hash.getValue(p), m_hash.getValue(o), objectType);
				}
			}));
			i.doImport();
		}
	}
	
	private static Importer getImporter(String dataset) throws IOException {
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
			String dir = "/Users/gl/Studium/diplomarbeit/datasets/lubm50/hashed_triples";
			importer = new HashedTriplesImporter(dir + "/hashes", dir + "/propertyhashes"); 
			importer.addImport(dir + "/input.ht");

//			importer = new OntologyImporter();
//			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm1/").listFiles()) {
//				if (f.getName().startsWith("University")) {
//					importer.addImport(f.getAbsolutePath());
//				}
//			}
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

	public static void main(String[] args) throws Exception {
		OptionParser op = new OptionParser();
		op.accepts("a", "action to perform, comma separated list of: import")
			.withRequiredArg().ofType(String.class).describedAs("action").withValuesSeparatedBy(',');
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");
		op.accepts("sp", "remove bn");
		op.accepts("qf", "query file")
			.withRequiredArg().ofType(String.class);
		op.accepts("q", "query name")
			.withRequiredArg().ofType(String.class);
		
		OptionSet os = op.parse(args);
		
		if (!os.has("a") || !os.has("o")) {
			op.printHelpOn(System.out);
			return;
		}
		
		String action = (String)os.valueOf("a");
		String outputDirectory = (String)os.valueOf("o");

		if (action.equals("import")) {
			final LuceneStorage ls = new LuceneStorage(outputDirectory);
			ls.initialize(true, false);

			List<String> files = os.nonOptionArguments();
			if (files.size() == 1) {
				// check if file is a directory, if yes, import all files in the directory
				File f = new File(files.get(0));
				if (f.isDirectory()) {
					files = new ArrayList<String>();	
					for (File file : f.listFiles())
						files.add(file.getAbsolutePath());
				}
			}
			
			Importer importer;
			if (files.get(0).contains(".nt"))
				importer = new NTriplesImporter(false);
			else if (files.get(0).contains(".owl"))
				importer = new OntologyImporter();
			else if (files.get(0).contains(".rdf") || files.get(0).contains(".xml"))
				importer = new RDFImporter();
			else
				throw new Exception("file type unknown");
			
			importer.addImports(files);

			final Util.Counter c = new Util.Counter();

			importer.setTripleSink(new TripleSink() {
				public void triple(String s, String p, String o, String objectType) {
					ls.addTriple(s, p, o);
					c.val++;
					if (c.val % 100000 == 0)
						log.debug(c.val);
				}
			});
			importer.doImport();
			ls.flush();
			ls.optimize();
			
			importer = null;
			System.gc();
			log.debug(Util.memory());
			
			ls.merge();
			
			ls.close();
		}
		else if (action.equals("query")) {
			String queryFile = (String)os.valueOf("qf");
			String queryName = (String)os.valueOf("q");
			
			if (queryFile == null) {
				log.error("no query file specified");
			}
			
			QueryLoader loader = new QueryLoader();
			List<Query> queries = loader.loadQueryFile(queryFile);

			final LuceneStorage ls = new LuceneStorage(outputDirectory);
			ls.initialize(false, true);
			VPQueryEvaluator qe = new VPQueryEvaluator(ls);
			
			for (Query q : queries) {
				if (queryName != null && !q.getName().equals(queryName))
					continue;
				
				log.debug("--------------------------------------------");
				log.debug("query: " + q.getName());
				log.debug(q);
				List<String[]> results = qe.evaluate(q);
				log.info("query " + q.getName() + ": " + results.size() + " results");
			}
			
			Timings t = qe.getT();
			StatisticsCollector sc = new StatisticsCollector();
			sc.addTimings(t);
			sc.logStats();
			
			ls.close();
		}
	}
}
