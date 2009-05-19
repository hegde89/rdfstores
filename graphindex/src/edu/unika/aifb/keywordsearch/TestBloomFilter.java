package edu.unika.aifb.keywordsearch;

import it.unimi.dsi.util.BloomFilter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;

public class TestBloomFilter {
	public static void main(String[] args) throws Exception{
		//indexDir is the directory that hosts Lucene's index files
        File indexDir = new File("D:/Lucene/src/index");
        Analyzer luceneAnalyzer = new StandardAnalyzer();
        IndexWriter indexWriter = new IndexWriter(indexDir,luceneAnalyzer,true);
        
        BloomFilter bf = new BloomFilter(10);
        bf.add("http://www.aifb.uni-karlsruhe.de/id0instance");
        bf.add("http://www.aifb.uni-karlsruhe.de/id2instance");
        bf.add("http://www.aifb.uni-karlsruhe.de/id4instance");
        bf.add("http://www.aifb.uni-karlsruhe.de/id6instance");
        bf.add("http://www.aifb.uni-karlsruhe.de/id8instance");
        bf.add("http://www.aifb.uni-karlsruhe.de/id10instance");
        
        ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
		ObjectOutputStream objectOut = new ObjectOutputStream(byteArrayOut);
		objectOut.writeObject(bf);
		byte[] bytes = byteArrayOut.toByteArray();

        Document document = new Document();
        document.add(new Field("word", "key", Field.Store.NO, Field.Index.TOKENIZED));
        document.add(new Field("content", bytes, Field.Store.YES));
        indexWriter.addDocument(document);
        System.out.println("indexing");
        
        indexWriter.optimize();
        
        
        FSDirectory directory = FSDirectory.getDirectory(indexDir,false);
        IndexReader reader = IndexReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        if(!indexDir.exists()){
        	System.out.println("The Lucene index is not exist");
        	return;
        }

        Term term = new Term("word","key");
        Query query = new TermQuery(term);
        
        Hits hits = searcher.search(query);
        
        System.out.println("hits.length(): " + hits.length());
        for(int i = 0; i < hits.length(); i++){
            System.out.println("Document " + (i+1));
        	Document document_ = hits.doc(i);
        	byte[] bytes_ = document_.getBinaryValue("content");
			ByteArrayInputStream byteArrayInput = new ByteArrayInputStream(bytes_);
			ObjectInputStream objectInput = new ObjectInputStream(byteArrayInput);
			BloomFilter bf_ = (BloomFilter)objectInput.readObject(); 
			
			System.out.println("Contain 0:" + bf_.contains("http://www.aifb.uni-karlsruhe.de/id0instance"));
			System.out.println("Contain 1:" + bf_.contains("http://www.aifb.uni-karlsruhe.de/id1instance"));
			System.out.println("Contain 2:" + bf_.contains("http://www.aifb.uni-karlsruhe.de/id2instance"));
			System.out.println("Contain 3:" + bf_.contains("http://www.aifb.uni-karlsruhe.de/id3instance"));
			System.out.println("Contain 4:" + bf_.contains("http://www.aifb.uni-karlsruhe.de/id4instance"));
			System.out.println("Contain 5:" + bf_.contains("http://www.aifb.uni-karlsruhe.de/id5instance"));
			System.out.println("Contain 6:" + bf_.contains("http://www.aifb.uni-karlsruhe.de/id6instance"));
			System.out.println("Contain 7:" + bf_.contains("http://www.aifb.uni-karlsruhe.de/id7instance"));
			System.out.println("Contain 8:" + bf_.contains("http://www.aifb.uni-karlsruhe.de/id8instance"));
			System.out.println("Contain 9:" + bf_.contains("http://www.aifb.uni-karlsruhe.de/id9instance"));
			System.out.println("Contain 10:" + bf_.contains("http://www.aifb.uni-karlsruhe.de/id10instance"));
			

        	System.out.println();
        }
        
        System.out.println("---------------------------------------------------------------------\n");
        
        indexWriter.close();
        
        
	}
}