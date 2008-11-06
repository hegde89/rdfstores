package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.LVertexM;
import edu.unika.aifb.graphindex.data.ListVertexCollection;
import edu.unika.aifb.graphindex.data.VertexFactory;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.ParsingTripleConverter;
import edu.unika.aifb.graphindex.importer.TriplesImporter;
import edu.unika.aifb.graphindex.indexing.FastIndexBuilder;
import edu.unika.aifb.graphindex.preprocessing.FileHashValueProvider;
import edu.unika.aifb.graphindex.preprocessing.SortedVertexListBuilder;
import edu.unika.aifb.graphindex.preprocessing.TripleConverter;
import edu.unika.aifb.graphindex.preprocessing.TriplesPartitioner;
import edu.unika.aifb.graphindex.preprocessing.VertexListProvider;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Util;

public class StructureIndexWriter {
	private String m_directory;
	private StructureIndex m_index;
	private Importer m_importer;
	
	private static final Logger log = Logger.getLogger(StructureIndexWriter.class);
	
	public StructureIndexWriter(String dir, boolean clean) throws StorageException {
		m_directory = dir;
		m_index = new StructureIndex(dir, clean, false);
	}
	
	public void setImporter(Importer importer) {
		m_importer = importer;
	}
	
	public void create() throws IOException, InterruptedException, NumberFormatException, StorageException {
		create(new HashSet<String>(Arrays.asList("convert", "partition", "transform", "index")));
	}
	
	public void create(Set<String> stages) throws IOException, InterruptedException, NumberFormatException, StorageException {
		VertexFactory.setCollectionClass(ListVertexCollection.class);
		VertexFactory.setVertexClass(LVertexM.class);
		
		if (stages.contains("convert"))
			convert();
		if (stages.contains("partition"))
			partition();
		if (stages.contains("transform"))
			transform();
		if (stages.contains("index"))
			index();
	}
	
	private void convert() throws IOException, InterruptedException {
		// CONVERT
		TripleConverter tc = new TripleConverter(m_directory);
		
		m_importer.setTripleSink(tc);
		m_importer.doImport();
		
		tc.write();
		
		log.debug("sorting...");
		Util.sortFile(m_directory + "/input.ht", m_directory + "/input_sorted.ht");
		log.debug("sorting complete");
	}
	
	private void partition() throws IOException {
		// PARTITION
		TriplesPartitioner tp = new TriplesPartitioner(m_directory + "/components");
		
		Importer importer = new TriplesImporter();
		importer.addImport(m_directory + "/input_sorted.ht");
		importer.setTripleSink(new ParsingTripleConverter(tp));
		importer.doImport();
		
		tp.write();
	}
	
	private void transform() throws NumberFormatException, IOException {
		// TRANSFORM
		Importer importer = new TriplesImporter();
		importer.addImport(m_directory + "/input_sorted.ht");
		
		SortedVertexListBuilder vb = new SortedVertexListBuilder(importer, m_directory + "/components");
		vb.write();
	}
	
	private void index() throws IOException, NumberFormatException, StorageException, InterruptedException {
		// BUILD INDEX
		VertexListProvider vlp = new VertexListProvider(m_directory + "/components/");
		HashValueProvider hvp = new FileHashValueProvider(m_directory + "/hashes", m_directory + "/propertyhashes");
		
		FastIndexBuilder ib = new FastIndexBuilder(m_index, vlp, hvp);
		ib.buildIndex();
		
		m_index.getExtensionManager().getExtensionStorage().mergeExtensions();

	}
	
	public void close() throws StorageException {
		m_index.close();
	}
	
	public void removeTemporaryFiles() {
		File componentDir = new File(m_directory + "/components");
		for (File f : componentDir.listFiles())
			f.delete();
		componentDir.delete();
		
		File f = new File(m_directory + "/propertyhashes");
		f.delete();
		
		f = new File(m_directory + "/input_sorted.ht");
		f.delete();

		f = new File(m_directory + "/input.ht");
		f.delete();
	}
}
