package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.util.Util;

public class Owl2NTriples {
	public static void main(String[] args) throws IOException {
		OptionParser op = new OptionParser();
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");
		op.accepts("f", "ntriples output file")
			.withRequiredArg().ofType(String.class);
		
		OptionSet os = op.parse(args);
		
		if (!os.has("f")) {
			op.printHelpOn(System.out);
			return;
		}
		
//		String outputDirectory = (String)os.valueOf("o");
		String outputFile = (String)os.valueOf("f");
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
		
		final PrintWriter pw = new PrintWriter(new FileWriter(outputFile));

		Importer importer = new OntologyImporter();
		importer.addImports(files);
		importer.setTripleSink(new TripleSink() {

			public void triple(String s, String p, String o, String objectType) {
				if (Util.isEntity(o))
					o = "<" + o + ">";
				else
					o = "\"" + o + "\"";
				s = "<" + s + ">";
				p = "<" + p + ">";
				
				pw.println(s + " " + p + " " + o + " .");
			}
			
		});
		
		importer.doImport();
		
		pw.close();
	}
}
