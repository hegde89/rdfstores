package edu.unika.aifb.graphindex.searcher.keyword.analyzer;

import java.io.File;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class TestCompoundWordStandardAnalyzer {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		HyphenationCompoundWordStandardAnalyzer analyzer = 
			new HyphenationCompoundWordStandardAnalyzer("./res/en_hyph_US.xml", "./res/en_US.dic", "./res/en_stopWords");
		// Store the index on disk:
		File dir = new File("./res/test/lucene");
		if(!dir.exists())
			dir.mkdirs();
		else {
			for(File file : dir.listFiles()) {
				file.delete();
			}
		}	
		Directory directory = FSDirectory.getDirectory(dir);
		IndexWriter iwriter = new IndexWriter(directory, analyzer, true, new IndexWriter.MaxFieldLength(25000));
		
		Document doc = new Document();
		String text1 = "birthplace populationtotal areacode areatotal leadertitle populationasof postalcode";
		String text2 = "dictionarycompoundwordtokenfilter hyphenationcompoundwordtokenfilter";
		doc.add(new Field("field1", text1, Field.Store.YES, Field.Index.ANALYZED));
		doc.add(new Field("field2", text2, Field.Store.YES, Field.Index.ANALYZED));
		
		iwriter.addDocument(doc);
		iwriter.close();
		directory.close();
	}

}
