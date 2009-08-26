package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NxImporter;
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

		OptionSet os = op.parse(args);
		
		if (!os.has("o") || os.nonOptionArguments().size() == 0) {
			op.printHelpOn(System.out);
			return;
		}

		String directory = (String)os.valueOf("o");
		int sk = os.has("sk") ? (Integer)os.valueOf("sk") : 0;
		int nk = os.has("nk") ? (Integer)os.valueOf("nk") : 0;
		
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
		else  {
			log.warn("unknown extension, assuming n-triples format");
			importer = new NxImporter();
		}
		
		importer.addImports(files);

		IndexCreator ic = new IndexCreator(new IndexDirectory(directory));
		
		ic.setImporter(importer);
		ic.setCreateDataIndex(true);
		ic.setCreateStructureIndex(sk > 0);
		ic.setCreateKeywordIndex(os.has("kw"));
		ic.setKWNeighborhoodSize(nk);
		ic.setSIPathLength(sk);
		ic.setStructureBasedDataPartitioning(false);
		
		ic.create();
	}

}
