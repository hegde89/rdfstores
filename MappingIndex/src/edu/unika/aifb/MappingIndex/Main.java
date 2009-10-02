package edu.unika.aifb.MappingIndex;
import java.util.List;

import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NxImporter;
import edu.unika.aifb.graphindex.index.IndexDirectory;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class Main {

	/**
	 * @param args mapping file(s)
	 */	
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

			OptionParser op = new OptionParser();
			
			// Define option for output directory
			op.accepts("o", "output directory").withRequiredArg().ofType(String.class).describedAs("directory");
			// Define option for the data source used as source in the mapping
			op.accepts("s", "source datasource").withRequiredArg().ofType(String.class).describedAs("directory");
			// Define option for the data source used as destination in the mapping
			op.accepts("t", "target datasource").withRequiredArg().ofType(String.class).describedAs("directory");
			
			// Parse arguments with option parser
			OptionSet os = op.parse(args);
			
			// Check for output directory in option set
			if (!os.has("o") || !os.has("s") || !os.has("t")|| os.nonOptionArguments().size() == 0) {
				op.printHelpOn(System.out);
				return;
			}
			
			// Get output directory
			String directory = (String)os.valueOf("o");
			// Get source
			String source = (String)os.valueOf("s");
			// Get destination
			String destination = (String)os.valueOf("t");
			
			// Get filename
			List<String> files = os.nonOptionArguments();

			Importer importer = new NxImporter();
			importer.addImports(files);

			MappingIndexCreator mic = new MappingIndexCreator(directory, source, destination);
			
			mic.setImporter(importer);
			mic.create();
	}

}
