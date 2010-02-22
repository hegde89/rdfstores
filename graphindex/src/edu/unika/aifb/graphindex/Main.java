package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.N3Importer;
import edu.unika.aifb.graphindex.importer.NxImporter;
import edu.unika.aifb.graphindex.importer.RDFImporter;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class Main {
	
	private static final Logger log = Logger.getLogger(Main.class);

	public static void main(String[] args) throws Exception {
		OptionParser op = new OptionParser();
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");
		op.accepts("sk", "structure index path length")
			.withRequiredArg().ofType(Integer.class).describedAs("structure index path length, default: 0");
		op.accepts("nk", "neighborhood size")
			.withRequiredArg().ofType(Integer.class).describedAs("neighborhood size, default: 0");
		op.accepts("kw", "keyword index");
		op.accepts("sd", "data extensions");
		op.accepts("resume", "resume from analyzse, structure or keyword").withRequiredArg().ofType(String.class);
		op.accepts("triples", "triples only");
		op.accepts("bwonly", "backward only");
		op.accepts("fwonly", "forward only");
		op.accepts("dt", "do not ignore datatypes");
		op.accepts("re", "eliminate reflexive edges");
		
		OptionSet os = op.parse(args);
		
		if (!os.has("o") || os.nonOptionArguments().size() == 0) {
			op.printHelpOn(System.out);
			return;
		}

		String directory = (String)os.valueOf("o");
		int sk = os.has("sk") ? (Integer)os.valueOf("sk") : 0;
		int nk = os.has("nk") ? (Integer)os.valueOf("nk") : 0;
		boolean triplesOnly = os.has("triples");
		boolean backwardOnly = os.has("bwonly");
		boolean forwardOnly = os.has("fwonly");
		boolean eliminateRE = os.has("re");
		
		if (backwardOnly && forwardOnly) {
			log.error("only one of bwonly and fwonly can be specified");
			return;
		}
			
		
		int startFrom = IndexCreator.STEP_DATA;
		if (os.has("resume")) {
			String from = (String)os.valueOf("resume");
			if (from.equals("a"))
				startFrom = IndexCreator.STEP_ANALYZE;
			if (from.equals("s"))
				startFrom = IndexCreator.STEP_STRUCTURE;
			if (from.equals("p"))
				startFrom = IndexCreator.STEP_PARTITION;
			if (from.equals("w"))
				startFrom = IndexCreator.STEP_KEYWORD_PREPARE;
			if (from.equals("k"))
				startFrom = IndexCreator.STEP_KEYWORD;
			if (from.equals("e"))
				startFrom = IndexCreator.STEP_KEYWORD_RESUME;
		}
		
		List<String> files = os.nonOptionArguments();
		
		if (files.size() == 1) {
			// check if file is a directory, if yes, import all files in the directory
			File f = new File(files.get(0));
			if (f.isDirectory()) {
				files = new ArrayList<String>();	
				for (File file : f.listFiles())
					if (!file.getName().startsWith("."))
						files.add(file.getAbsolutePath());
			}
		}

		Importer importer;
		if (files.get(0).contains(".nt"))
			importer = new NxImporter();
		else if (files.get(0).contains(".n3"))
			importer = new N3Importer();
		else if (files.get(0).endsWith(".rdf") || files.get(0).endsWith(".xml"))
			importer = new RDFImporter();	
		else {
			log.warn("unknown extension, assuming n-triples format");
			importer = new NxImporter();
		}
		
		importer.addImports(files);

		IndexCreator ic = new IndexCreator(new IndexDirectory(directory));
		importer.setIgnoreDataTypes(!os.has("dt"));
		ic.setImporter(importer);
		ic.setCreateDataIndex(true);
		ic.setCreateStructureIndex(sk > 0);
		ic.setCreateKeywordIndex(os.has("kw"));
		ic.setKWNeighborhoodSize(nk);
		ic.setSIPathLength(sk);
		ic.setStructureBasedDataPartitioning(os.has("sd"));
		ic.setSICreateDataExtensions(os.has("sd"));
		ic.setOption(IndexConfiguration.TRIPLES_ONLY, triplesOnly);
		ic.setOption(IndexConfiguration.SP_BACKWARD_ONLY, backwardOnly);
		ic.setOption(IndexConfiguration.SP_FORWARD_ONLY, forwardOnly);
		ic.setOption(IndexConfiguration.SP_ELIMINATE_REFLEXIVE_EDGES, eliminateRE);
		
		ic.create(startFrom);
	}

}
