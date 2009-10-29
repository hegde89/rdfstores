package edu.unika.aifb.MappingIndex;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
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
			
			// Parse arguments with option parser
			OptionSet os = op.parse(args);
			
			// Check for output directory in option set
			if (!os.has("o") || os.nonOptionArguments().size() == 0) {
				op.printHelpOn(System.out);
				return;
			}
			
			// Get output directory
			String directory = (String)os.valueOf("o");;
			
			// Get filename
			List<String> files = os.nonOptionArguments();
			
			// Get list of mappings from file(s)
			List<Mapping> maps = new LinkedList();
			for (Iterator<String> file = files.listIterator(); file.hasNext(); ) {
				String f = file.next();
				getMappingFromFile(maps, f);
			}
			
			// New MappingIndex
			//MappingIndexCreator mic = new MappingIndexCreator(directory, getMappingFromFile(files.get(0)));
			MappingIndexCreator mic = new MappingIndexCreator(directory, maps);
			
			// Create MappingIndex
			mic.create();
	}
	
	private static void getMappingFromFile(List<Mapping> maps, String filename) {
		//List<Mapping> maps = new LinkedList();
		BufferedReader reader;
		String zeile=null;
	

		try {
			// Get reader
			reader = new BufferedReader(new FileReader(filename));
			// Read first line
			zeile = reader.readLine();			
			
			// Read all lines of file
			while (zeile != null) {			
				// [0] == Source Datasource, [1] == Destination Datasource, [2] == Mappingfile
				String[] values = zeile.split(" ");
				// Create new Mapping information
				Mapping m = new Mapping(values[0], values[1], values[2]);
				// Add to list
				maps.add(m);
				// Read next line
				zeile = reader.readLine();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Return list
		//return maps;

	}

}
