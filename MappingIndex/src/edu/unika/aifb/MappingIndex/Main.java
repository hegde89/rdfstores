package edu.unika.aifb.MappingIndex;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
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
			//op.accepts("s", "source datasource").withRequiredArg().ofType(String.class).describedAs("directory");
			// Define option for the data source used as destination in the mapping
			//op.accepts("t", "target datasource").withRequiredArg().ofType(String.class).describedAs("directory");
			
			// Parse arguments with option parser
			OptionSet os = op.parse(args);
			
			// Check for output directory in option set
			//if (!os.has("o") || !os.has("s") || !os.has("t")|| os.nonOptionArguments().size() == 0) {
			if (!os.has("o") || os.nonOptionArguments().size() == 0) {
				op.printHelpOn(System.out);
				return;
			}
			
			// Get output directory
			String directory = (String)os.valueOf("o");
			// Get source
			//String source = (String)os.valueOf("s");
			// Get destination
			//String destination = (String)os.valueOf("t");
			
			// Get filename
			List<String> files = os.nonOptionArguments();
			//Importer importer = new NxImporter();
			//importer.addImports(files);
			
			//List<Mapping> maps = new LinkedList();
			//Mapping m = new Mapping(source, destination, files.get(0));
			//maps.add(m);
			
			//MappingIndexCreator mic = new MappingIndexCreator(directory, source, destination);
			//MappingIndexCreator mic = new MappingIndexCreator(directory, maps);
			MappingIndexCreator mic = new MappingIndexCreator(directory, getMappingFromFile(files.get(0)));
			
			//mic.setImporter(importer);
			mic.create();
	}
	
	private static List<Mapping> getMappingFromFile(String filename) {
		List<Mapping> maps = new LinkedList();
		BufferedReader reader;
		String zeile=null;
	

		try {
			// Get reader
			reader = new BufferedReader(new FileReader(filename));
			// Read first line
			zeile = reader.readLine();			
			
			// Read all lines of file
			while (zeile != null) {			
				// [0] == Origian Datasource, [1] == Target Datasource, [2] == Mappingfile
				String[] values = zeile.split(" ");
				// Create new Mapping information
				Mapping m = new Mapping(values[0], values[1], values[2]);
				// Add to list
				maps.add(m);
				// Read next line
				zeile = reader.readLine();
			}

		} catch (IOException e) {
			System.err.println("Error2 :"+e);
		}
		
		// Return list
		return maps;

	}

}
