package edu.unika.aifb.graphindex.vp;

import java.io.File;
import java.io.IOException;
import java.util.List;

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
				public void triple(long s, long p, long o) {
					m_sink.triple(m_hash.getValue(s), m_hash.getValue(p), m_hash.getValue(o));
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
//			String dir = "/Users/gl/Studium/diplomarbeit/datasets/lubm50/hashed_triples";
//			importer = new HashedTriplesImporter(dir + "/hashes", dir + "/propertyhashes"); 
//			importer.addImport(dir + "/input.ht");

			importer = new OntologyImporter();
			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm1/").listFiles()) {
				if (f.getName().startsWith("University")) {
					importer.addImport(f.getAbsolutePath());
				}
			}
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

	public static void main(String[] args) throws StorageException, IOException {
		String dir = "/Users/gl/Studium/diplomarbeit/workspace/graphindex/output/vp/" + args[1];
		String dataset = args[2];
		
		final LuceneStorage ls = new LuceneStorage(dir);
		
		if (args[0].equals("import")) {
			ls.initialize(true, false);
			
			final Util.Counter c = new Util.Counter();
			Importer importer = getImporter(args[2]);
			importer.setTripleSink(new TripleSink() {
				public void triple(String s, String p, String o) {
					ls.addTriple(s, p, o);
					c.val++;
					if (c.val % 50000 == 0)
						log.debug(c.val);
				}
			});
			importer.doImport();
			ls.flush();
			ls.merge();
			ls.optimize();
		}
		else if (args[0].equals("query")) {
			ls.initialize(false, true);
			VPQueryEvaluator qe = new VPQueryEvaluator(ls);
			
			if (dataset.equals("sweto")) {
				QueryLoader ql = new QueryLoader();
				List<Query> queries = ql.loadQueryFile("/Users/gl/Studium/diplomarbeit/graphindex evaluation/dblp queries.txt");
				
				for (Query q : queries) {
					if (!q.getName().equals("q10"))
						continue;
					log.debug("--------------------------------------------");
					log.debug("query: " + q.getName());
					log.debug(q);
					qe.evaluate(q);
//					break;
				}
			}
			else if (dataset.equals("lubm")) {
				QueryLoader ql = new QueryLoader();
				List<Query> queries = ql.loadQueryFile("/Users/gl/Studium/diplomarbeit/graphindex evaluation/lubm queries.txt");
				
				for (Query q : queries) {
//					if (!q.getName().equals("lq4"))
//						continue;
					log.debug("--------------------------------------------");
					log.debug("query: " + q.getName());
					log.debug(q);
					qe.evaluate(q);
//					break;
				}
			}
			
//			GTable<String> res = ls.getTable(null, "http://example.org/simple#k", null);
//			for (String[] row : res) {
//				for (String s : row)
//					System.out.print(s + " ");
//				System.out.println();
//			}
		}
		
		ls.close();
	}
}
