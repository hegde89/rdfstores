package edu.unika.aifb.atwrank;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.N3Importer;
import edu.unika.aifb.graphindex.importer.NxImporter;
import edu.unika.aifb.graphindex.importer.RDFImporter;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.storage.StorageException;

public class WikiIndexCreatorTest {
	private static final Logger log = Logger.getLogger(WikiIndexCreatorTest.class);
	
	public static void main(String[] args) throws IOException, StorageException, InterruptedException {
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
		else if (files.get(0).contains(".n3"))
			importer = new N3Importer();
		else if (files.get(0).endsWith(".rdf") || files.get(0).endsWith(".xml"))
			importer = new RDFImporter();
		else  {
			log.warn("unknown extension, assuming n-triples format");
			importer = new NxImporter();
		}
		
		importer.addImports(files);

		WikiIndexCreator ic = new WikiIndexCreator(new IndexDirectory(directory), "http://semanticweb.org");
		ic.setCreateKeywordIndex(true);
		ic.setKWNeighborhoodSize(0);
		ic.setCreateStructureIndex(false);
		ic.setImporter(importer);
		
		ic.create();
	}
}
