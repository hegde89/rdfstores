package edu.unika.aifb.graphindex.storage.lucene;

import java.io.IOException;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.LockObtainFailedException;

public class LuceneTest {
	public static void main(String[] args) throws CorruptIndexException, LockObtainFailedException, IOException {
		IndexWriter iw = new IndexWriter("/Users/gl/Studium/diplomarbeit/workspace/graphindex/index/", new WhitespaceAnalyzer());
		
		Document d1 = new Document();
		d1.add(new Field("ext", "ext2//type//xyz", Field.Store.YES, Field.Index.TOKENIZED));
		d1.add(new Field("subject", "subject1", Field.Store.YES, Field.Index.NO));
		
		Document d2 = new Document();
		d2.add(new Field("ext", "ext1//type//xyz", Field.Store.YES, Field.Index.TOKENIZED));
		d2.add(new Field("subject", "subject1", Field.Store.YES, Field.Index.NO));

		Document d3 = new Document();
		d3.add(new Field("ext", "ext1//type//xyz ext2//type//xyz", Field.Store.YES, Field.Index.TOKENIZED));
		d3.add(new Field("subject", "subject3", Field.Store.YES, Field.Index.NO));

		iw.addDocument(d1);
		iw.addDocument(d2);
		iw.addDocument(d3);
		iw.flush();
		
		iw.close();
	}
}
