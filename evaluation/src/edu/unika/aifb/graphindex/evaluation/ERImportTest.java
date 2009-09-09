package edu.unika.aifb.graphindex.evaluation;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.semanticweb.yars.nx.Node;

import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.index.IndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
import edu.unika.aifb.graphindex.util.Util;

public class ERImportTest {
	private static class Value<T> {
		T value;
	}

	public static void main(String[] args) throws IOException, StorageException, InterruptedException {
		
//		LuceneIndexStorage is = new LuceneIndexStorage(new File("/data/sp/indexes/ertest/vp/poc"));
//		is.initialize(false, false);
//		System.out.println(Util.memory());
//		is.mergeIndex(IndexDescription.POCS);
//		is.close();
		
		IndexCreator ic = new IndexCreator(new IndexDirectory(args[0]));
		ic.setCreateKeywordIndex(false);
		ic.setCreateStructureIndex(false);
		
		final ErdosRenyi er = new ErdosRenyi(Integer.parseInt(args[1]), 100, 0.1f);
		
		final Value<Integer> count = new Value<Integer>();
		count.value = 0;
		ic.setImporter(new Importer() {
			@Override
			public void doImport() {
				for (Iterator<Node[]> i = er; i.hasNext(); ) {
					Node[] nodes = er.next();
					m_sink.triple(nodes[0].toN3(), nodes[1].toN3(), nodes[2].toN3(), nodes[3].toN3());
					count.value++;
					if (count.value % 200000 == 0)
						System.out.println(count.value);
				}
			}
		});
		
		ic.create();
	}
}
