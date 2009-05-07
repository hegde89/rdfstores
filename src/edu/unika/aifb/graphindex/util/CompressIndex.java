package edu.unika.aifb.graphindex.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;

public class CompressIndex {
	
	private final String FIELD_EXT = "ext";
	public final String FIELD_SUBJECT = "subject";
	private final String FIELD_PROPERTY = "property";
	private final String FIELD_OBJECT = "object";
	private final String TYPE_EXTLIST = "extension_list";
	private final String EXT_PATH_SEP = "__";

	public void compressIndex(String directory, String output) throws CorruptIndexException, IOException {
		IndexReader ir = IndexReader.open(directory);
		IndexWriter iw = new IndexWriter(FSDirectory.getDirectory(output), false, new WhitespaceAnalyzer(), true);
		IndexSearcher is = new IndexSearcher(ir);

		iw.setMergeFactor(15);
		iw.setRAMBufferSizeMB(1024);
		iw.setInfoStream(System.out);

		int x = 0;
		int total = ir.numDocs();
		
		TermEnum te = ir.terms();
		while (te.next()) {
			Term t = te.term();
			if (t.field().equals(FIELD_EXT)) {
				String ext = t.text();
				String[] path = ext.split(EXT_PATH_SEP.replaceAll("\\|", "\\\\|"));
				
				final List<Integer> docIds = new ArrayList<Integer>(); 
				
				TermQuery tq = new TermQuery(t);
				is.search(tq, new HitCollector() {
					@Override
					public void collect(int docId, float score) {
						docIds.add(docId);
					}
				});
				
				for (int docId : docIds) {
					Document doc = ir.document(docId);
					
					byte[] input = doc.getField(FIELD_SUBJECT).stringValue().getBytes();
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					OutputStream cos = new GZIPOutputStream(baos);
					cos.write(input);
					cos.flush();
					cos.close();
					byte[] compressed = baos.toByteArray();
//					System.out.println(input.length + " " + compressed.length);
					
					Document docNew = new Document();
					docNew.add(new Field(FIELD_EXT, doc.getField(FIELD_EXT).stringValue(), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
//					docNew.add(new Field(FIELD_SUBJECT, doc.getField(FIELD_SUBJECT).stringValue(), Field.Store.COMPRESS, Field.Index.NO, Field.TermVector.NO));
					docNew.add(new Field(FIELD_SUBJECT, compressed, Field.Store.YES));
					docNew.add(new Field(FIELD_OBJECT, doc.getField(FIELD_OBJECT).stringValue(), Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
					iw.addDocument(docNew);
					
					x++;
					
					if (x % 10000 == 0)
						System.out.println(x + "/" + total);
				}
			}
		}
		
		is.close();
		ir.close();
		
		iw.flush();
		iw.optimize();
		iw.close();
	}
	
	public static void main(String[] args) throws CorruptIndexException, IOException {
		CompressIndex ci = new CompressIndex();
		ci.compressIndex("/Users/gl/Studium/diplomarbeit/workspace/graphindex/output/lubm50/index", "/Users/gl/Studium/diplomarbeit/workspace/graphindex/output/lubm50/index_compressed");
		
		
	}
}
