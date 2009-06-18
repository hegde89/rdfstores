package edu.unika.aifb.graphindex;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;

import edu.unika.aifb.keywordsearch.Constant;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class KeywordIndexConverter {
	private static final Logger log = Logger.getLogger(KeywordIndexConverter.class);
	/**
	 * @param args
	 * @throws IOException 
	 * @throws CorruptIndexException 
	 */
	public static void main(String[] args) throws CorruptIndexException, IOException {
		OptionParser op = new OptionParser();
		op.accepts("i", "input directory")
			.withRequiredArg().ofType(String.class);
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class);

		OptionSet os = op.parse(args);
		
		if (!os.has("i") || !os.has("o")) {
			op.printHelpOn(System.out);
			return;
		}

		String inputDirectory = (String)os.valueOf("i");
		String outputDirectory = (String)os.valueOf("o");
		
		IndexReader ir = IndexReader.open(inputDirectory);
		IndexWriter iw = new IndexWriter(FSDirectory.getDirectory(outputDirectory), true, new WhitespaceAnalyzer(), true);
		IndexWriter compressed = new IndexWriter(FSDirectory.getDirectory(outputDirectory + "_compressed"), true, new WhitespaceAnalyzer(), true);
		
		int entities = 0;
		for (int i = 0; i < ir.numDocs(); i++) {
			Document in = ir.document(i);
			
			if (in.getField(Constant.NEIGHBORHOOD_FIELD) == null)
				continue;
			
			Document out = new Document();
			out.add(new Field(Constant.URI_FIELD, in.getField(Constant.URI_FIELD).stringValue(), Field.Store.NO, Field.Index.UN_TOKENIZED));
			out.add(new Field(Constant.NEIGHBORHOOD_FIELD, in.getField(Constant.NEIGHBORHOOD_FIELD).binaryValue(), Field.Store.YES));
			iw.addDocument(out);
			
			Document out2 = new Document();
			out2.add(new Field(Constant.URI_FIELD, in.getField(Constant.URI_FIELD).stringValue(), Field.Store.NO, Field.Index.UN_TOKENIZED));
			out2.add(new Field(Constant.NEIGHBORHOOD_FIELD, in.getField(Constant.NEIGHBORHOOD_FIELD).binaryValue(), Field.Store.COMPRESS));
			compressed.addDocument(out2);
			
			entities++;
			if (entities % 500000 == 0)
				log.debug("entities converted: " + entities);
		}
		
		ir.close();
		iw.optimize();
		iw.close();
		
		compressed.optimize();
		compressed.close();
	}

}
