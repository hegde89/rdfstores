package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.LVertex;
import edu.unika.aifb.graphindex.data.LVertexM;
import edu.unika.aifb.graphindex.data.ListVertexCollection;
import edu.unika.aifb.graphindex.data.VertexFactory;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.ParsingTripleConverter;
import edu.unika.aifb.graphindex.importer.TripleSink;
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
	private Importer m_importer = null;
	
	private static final Logger log = Logger.getLogger(StructureIndexWriter.class);
	
	public StructureIndexWriter(String dir, boolean clean) throws StorageException {
		m_directory = dir;
		m_index = new StructureIndex(dir, clean, false);
	}
	
	public void setBackwardEdgeSet(Set<String> edgeSet) {
		m_index.setBackwardEdges(edgeSet);
	}
	
	public void setForwardEdgeSet(Set<String> edgeSet) {
		m_index.setForwardEdges(edgeSet);
	}
	
	@SuppressWarnings("unchecked")
	public void setOptions(Map options) throws FileNotFoundException {
		m_index.setOptions(options);
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
		// CONVERT: convert triples into hash values and store the hashed triples in 'input.ht',  
		// then sort the file 'input.ht' to generate file 'input_sorted.ht'.
		// The mapping of elements and hash values are stored in the files '/hash' and '/propertyhashes', 
		TripleConverter tc = new TripleConverter(m_directory);
		
		m_importer.setTripleSink(tc);
		m_importer.doImport();
		
		tc.write();
		
		log.debug("sorting...");
		Util.sortFile(m_directory + "/input.ht", m_directory + "/input_sorted.ht");
		log.debug("sorting complete");
		
		if (m_index.getBackwardEdges().size() == 0 && m_index.getForwardEdges().size() == 0) {
			log.debug("edge sets empty, setting to full");
			m_index.setForwardEdges(tc.getEdgeSet());
			m_index.setBackwardEdges(tc.getEdgeSet());
		}
	}
	
	private void partition() throws IOException {
		// PARTITION: compute the connected components and store the nodes of them in files '/components/componentID'.
		TriplesPartitioner tp = new TriplesPartitioner(m_directory + "/components");
		tp.disablePartitioning(true);
		
		Importer importer = new TriplesImporter();
		importer.addImport(m_directory + "/input_sorted.ht");
		importer.setTripleSink(new ParsingTripleConverter(tp));
		importer.doImport();
		
		tp.write();
	}
	
	private void transform() throws NumberFormatException, IOException {
		// TRANSFORM: generate files '/components/componentID..vertexlist'.
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
		
//		m_index.getExtensionManager().getExtensionStorage().mergeExtensions();
		m_index.getExtensionManager().getExtensionStorage().optimize();
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
		
		f = new File(m_directory + "/attributes");
		f.delete();
		
		f = new File(m_directory + "/relations");
		f.delete();
		
		f = new File(m_directory + "/concepts");
		f.delete();
		
		f = new File(m_directory + "/entities");
		f.delete();
		
		File dir = new File(m_directory + "/block");
		for (File fi : dir.listFiles())
			fi.delete();
		dir.delete();
		
		dir = new File(m_directory + "/data");
		for (File fi : dir.listFiles())
			fi.delete();
		dir.delete();
	}
}
