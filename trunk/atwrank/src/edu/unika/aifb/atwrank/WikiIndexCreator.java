package edu.unika.aifb.atwrank;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.htmlparser.beans.StringBean;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.index.KeywordIndexBuilder;
import edu.unika.aifb.graphindex.storage.StorageException;

public class WikiIndexCreator extends IndexCreator {

	private String m_wikiUrl;
	
	private class WikiKeywordIndexBuilder extends KeywordIndexBuilder {

		private int m_pagesSkipped = 0, m_pagesImported = 0;

		private final String[] STOP_TEXTS = new String[] {
			"There is currently no text in this page, you can search",
			"This is an outdated testpage that should be deleted"
		};
		
		public WikiKeywordIndexBuilder(IndexReader idxReader) throws IOException, StorageException {
			super(idxReader, false);
		}
		
		private boolean containsStopText(String content) {
			for (String stopText : STOP_TEXTS)
				if (content.contains(stopText))
					return true;
			return false;
		}
		
		@Override
		protected List<Field> getFieldsForEntity(String uri) throws IOException, StorageException {
			List<Field> fields = super.getFieldsForEntity(uri);
			
			if (uri.startsWith(m_wikiUrl)) {
				Table<String> table = dataIndex.getTriples(uri, "http://semantic-mediawiki.org/swivt/1.0#page", null);
				
				if (table.rowCount() > 0) {
					String wikiPageUrl = table.getRow(0)[2];
					
					StringBean sb = new StringBean();
					sb.setURL(wikiPageUrl + "?action=render");
					String content = sb.getStrings();
					
					if (containsStopText(content)) {
//						System.out.println("skipping " + wikiPageUrl);
						
						m_pagesSkipped++;
					}
					else {
						System.out.println(uri + " => " + wikiPageUrl);
						Field f = new Field("http://aifb.kit.edu/atw/page_content", content, Store.NO, Index.ANALYZED);
						fields.add(f);
						
						m_pagesImported++;
						
						PrintWriter pw = new PrintWriter(new FileWriter(m_idxDirectory.getTempFile("wikidump.txt", false), true));
						pw.println(">>> " + uri);
						pw.println(content);
						pw.println("<<< " + uri);
						pw.println();
						pw.close();
					}

					if (m_pagesImported + m_pagesSkipped % 100 == 0) 
						System.out.println("pages imported: " + m_pagesImported + ", skipped: " + m_pagesSkipped);
				}
			}
			
			return fields;
		}
	}
	
	public WikiIndexCreator(IndexDirectory indexDirectory, String wikiURL) throws IOException {
		super(indexDirectory);
		m_wikiUrl = wikiURL;
	}

	@Override
	protected void createKWIndex(boolean resume) throws IOException, StorageException {
		prepareKeywordIndex();
		
		IndexReader reader = new IndexReader(m_idxDirectory);
		WikiKeywordIndexBuilder ib = new WikiKeywordIndexBuilder(reader);
		ib.indexKeywords();
		
		System.out.println("pages imported: " + ib.m_pagesImported + ", skipped: " + ib.m_pagesSkipped);
	}
}
