package edu.unika.aifb.graphindex;

import java.io.IOException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.index.DataIndex;
import edu.unika.aifb.graphindex.index.IndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.storage.StorageException;

public class Import {
	public static void main(String[] args) throws IOException, EnvironmentLockedException, StorageException, DatabaseException, InterruptedException {
		if (args[0].equals("import")) {
			IndexCreator ic = new IndexCreator(new IndexDirectory(args[1]));
			ic.setCreateStructureIndex(false);
			ic.setCreateKeywordIndex(false);
			
			ic.setImporter(new Importer() {
				@Override
				public void doImport() {
					m_sink.triple("subject", "property", "object", "context");
				}
			});
			
			ic.create();
		}
		else if (args[0].equals("query")) {
			IndexReader ir = new IndexReader(new IndexDirectory(args[1]));
			DataIndex di = ir.getDataIndex();
			
			Table<String> table = di.getQuads("subject1", "property", null, null);
		}
	}
}
