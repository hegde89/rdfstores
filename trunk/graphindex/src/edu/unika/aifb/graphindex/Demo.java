package edu.unika.aifb.graphindex;

import java.io.IOException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.graphindex.index.IndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.storage.StorageException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class Demo {
	public static void main(String[] args) throws IOException, EnvironmentLockedException, StorageException, DatabaseException, InterruptedException {
		OptionParser op = new OptionParser();
		op.accepts("a", "action to perform, comma separated list of: import")
			.withRequiredArg().ofType(String.class).describedAs("action").withValuesSeparatedBy(',');
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");

		OptionSet os = op.parse(args);
		
		if (!os.has("a") || !os.has("o")) {
			op.printHelpOn(System.out);
			return;
		}

		String action = (String)os.valueOf("a");
		String directory = (String)os.valueOf("o");

		if (action.equals("create")) {
			IndexCreator ic = new IndexCreator(new IndexDirectory(directory));
			ic.setCreateDataIndex(true);
			ic.setCreateStructureIndex(true);
			ic.setCreateKeywordIndex(true);
			ic.setKWNeighborhoodSize(2);
			ic.setSIPathLength(1);
			ic.setSICreateDataExtensions(false);
			
			ic.create();
		}
		
		if (action.equals("test")) {
			
		}
	}
}
