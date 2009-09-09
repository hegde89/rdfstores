package edu.unika.aifb.graphindex.evaluation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.DataIndex;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.query.PrunedQueryPart;
import edu.unika.aifb.graphindex.searcher.plan.QueryOperators;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public class QueryExecutor {
	private IndexReader m_idxReader;
	private DataIndex m_dataIndex;
	private StructureIndex m_structureIndex;
	private Map<IndexDescription,IndexStorage> m_indexes;
	private QueryOperators m_op;
	private Map<String,String> m_ns;
	
	private static final Logger log = Logger.getLogger(QueryExecutor.class);
	
	public QueryExecutor(IndexReader reader) throws IOException, StorageException {
		m_idxReader = reader;
		m_dataIndex = m_idxReader.getDataIndex();
		m_structureIndex = m_idxReader.getStructureIndex();
		
		m_indexes = new HashMap<IndexDescription,IndexStorage>();
		m_indexes.put(IndexDescription.CPSO, m_dataIndex.getIndexStorage(IndexDescription.CPSO));
		m_indexes.put(IndexDescription.SCOP, m_dataIndex.getIndexStorage(IndexDescription.SCOP));
		m_indexes.put(IndexDescription.OCPS, m_dataIndex.getIndexStorage(IndexDescription.OCPS));
		m_indexes.put(IndexDescription.PSOC, m_dataIndex.getIndexStorage(IndexDescription.PSOC));
		m_indexes.put(IndexDescription.POCS, m_dataIndex.getIndexStorage(IndexDescription.POCS));
		m_indexes.put(IndexDescription.SOPC, m_dataIndex.getIndexStorage(IndexDescription.SOPC));

		m_indexes.put(IndexDescription.SES, m_structureIndex.getSPIndexStorage());
		
		m_op = new QueryOperators(m_idxReader, m_indexes);
		
		m_ns = new HashMap<String,String>();
		m_ns.put("lubm", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#");
		m_ns.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
	}
	
	private DataField[] cols(DataField... cols) {
		return cols;
	}
	
	private String ns(String ns, String s) {
		return m_ns.get(ns) + s;
	}
	
	private String[] names(String... names) {
		return names;
	}
	
	private DataField[] SO = new DataField[] { DataField.SUBJECT, DataField.OBJECT };
	
	public void entity_query1() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Publication2"), DataField.PROPERTY, ns("lubm", "name"), DataField.OBJECT, "Publication2");
	}
	
	public void entity_query2() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent34@Department2.University0.edu"), DataField.PROPERTY, ns("lubm", "emailAddress"), DataField.OBJECT, "GraduateStudent34@Department2.University0.edu");
		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent34"), DataField.PROPERTY, ns("lubm", "name"), DataField.OBJECT, "GraduateStudent34");
		
		Table<String> t3 = m_op.mergeJoin(t1, t2, "?x1");
		
		Table<String> t4 = m_op.indexJoin(t3, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, ns("lubm", "publicationAuthor"));
	}

//	query:  q4
//	select: ?x1 ?x2 ?x6 
//	?x1 http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent
//	?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name "GraduateStudent107"
//	?x2 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest "Research16"
//	?x6 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name "Publication12"

//	?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor ?x2
//	?x6 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor ?x1
//	?x2 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor ?x3
//	?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf ?x3

//	?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom ?x4
//	?x5 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor ?x2
	
	// x1 -> x4
	// x5 -> x2

	public void graph_query4() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p1", null);
		p1.addEdge("?x1", ns("lubm", "undergraduateDegreeFrom"), "?x4");

		PrunedQueryPart p2 = new PrunedQueryPart("p2", null);
		p2.addEdge("?x5", ns("lubm", "publicationAuthor"), "?x2");
		
//		?x1 http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, 
			names("?x1", ns("lubm", "GraduateStudent")), 
			DataField.PROPERTY, ns("rdf", "type"), DataField.OBJECT, ns("lubm", "GraduateStudent"));

//		?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name "GraduateStudent107"
		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, 
			names("?x1", "GraduateStudent107"), 
			DataField.PROPERTY, ns("lubm", "name"), DataField.OBJECT, "GraduateStudent107");
		
//		?x2 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest "Research16"
		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, 
			names("?x2", "Research16"), 
			DataField.PROPERTY, ns("lubm", "researchInterest"), DataField.OBJECT, "Research16");

//		?x6 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name "Publication12"
		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, 
			names("?x6", "Publication12"), 
			DataField.PROPERTY, ns("lubm", "name"), DataField.OBJECT, "Publication12");

		Table<String> j1 = m_op.mergeJoin(t1, t2, "?x1");
		
//		?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor ?x2
		Table<String> j2 = m_op.indexJoin(j1, "?x1", IndexDescription.PSOC, SO,
			names("?x1", "?x2"), DataField.SUBJECT,
			DataField.PROPERTY, ns("lubm", "advisor"));
		
		Table<String> j3 = m_op.mergeJoin(j2, t3, "?x2");
		
//		?x6 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor ?x1
		Table<String> j4 = m_op.indexJoin(j3, "?x1", IndexDescription.POCS, SO, 
			names("?x6", "?x1"), DataField.OBJECT, 
			DataField.PROPERTY, ns("lubm", "publicationAuthor"));
		
		Table<String> j5 = m_op.mergeJoin(j4, t4, "?x6");

//		?x2 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor ?x3
		Table<String> j6 = m_op.indexJoin(j5, "?x2", IndexDescription.PSOC, SO, 
			names("?x2", "?x3"), DataField.SUBJECT, 
			DataField.PROPERTY, ns("lubm", "worksFor"));

//		?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf ?x3
		Table<String> j7 = m_op.indexJoin(j6, "?x1", "?x3", IndexDescription.PSOC, SO, 
			names("?x1", "?x3"), DataField.SUBJECT, 
			DataField.PROPERTY, ns("lubm", "memberOf"));
	
		Table<String> r1 = m_op.refineWithPrunedPart(p1, "?x1", j7);
		
		Table<String> r2 = m_op.refineWithPrunedPart(p2, "?x2", r1);
	}
	
	public void graph_query4_vp_generated() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x4");
		PrunedQueryPart p2 = new PrunedQueryPart("p", null);
		p2.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x2");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
		t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication12"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication12");

		Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent107"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent107");
		Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Research16"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research16");
		t4 = m_op.refineWithPrunedPart(p2, "?x2", t4);

		Table<String> t5_a = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x2");

		Table<String> t6_a = m_op.indexJoin(t2, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t6 = m_op.mergeJoin(t6_a, t5, "?x1");

		Table<String> t7 = m_op.indexJoin(t6, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");

		Table<String> t8 = m_op.indexJoin(t7, "?x1", "?x3", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

		m_op.compact(t8, Arrays.asList("?x1", "?x2", "?x6"));	}


	public void query_q10_spc() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x18", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x2");
		PrunedQueryPart p2 = new PrunedQueryPart("p", null);
		p2.addEdge("?x11", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x16");
		p2.addEdge("?x15", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf", "?x11");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");

		Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x8", "AssistantProfessor6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor6");
		Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x8");
		t4 = m_op.compact(t4, Arrays.asList("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"));

		Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");

		Table<String> t6_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent9@Department5.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent9@Department5.University0.edu");
		Table<String> t6 = m_op.mergeJoin(t6_a, t1, "?x1");
		t6 = m_op.compact(t6, Arrays.asList("?x1"));

		Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x7", "GraduateCourse39"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse39");

		Table<String> t8 = m_op.load(IndexDescription.POCS, SO, names("?x10", "FullProfessor7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor7");

		Table<String> t9 = m_op.load(IndexDescription.POCS, SO, names("?x12", "AssociateProfessor2"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor2");

		Table<String> t10_a = m_op.indexJoin(t6, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		Table<String> t10 = m_op.mergeJoin(t10_a, t7, "?x7");

		Table<String> t11_a = m_op.indexJoin(t4, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		Table<String> t11 = m_op.mergeJoin(t11_a, t10, "?x7");

		Table<String> t12_a = m_op.indexJoin(t2, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t12 = m_op.mergeJoin(t12_a, t11, "?x1");

		Table<String> t13_a = m_op.indexJoin(t12, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t13 = m_op.mergeJoin(t13_a, t8, "?x10");

		Table<String> t14 = m_op.indexJoin(t9, "?x12", IndexDescription.POCS, SO, names("?x13", "?x12"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t15 = m_op.indexJoin(t13, "?x1", IndexDescription.POCS, SO, names("?x14", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t16_a = m_op.indexJoin(t15, "?x14", IndexDescription.PSOC, SO, names("?x14", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t16 = m_op.mergeJoin(t16_a, t5, "?x5");
		t16 = m_op.compact(t16, Arrays.asList("?x10", "?x3", "?x1", "?x7", "?x8", "?x5"));

		Table<String> t17 = m_op.indexJoin(t16, "?x10", IndexDescription.PSOC, SO, names("?x10", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");

		Table<String> t18 = m_op.indexJoin(t17, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		t18 = m_op.refineWithPrunedPart(p1, "?x2", t18);

		Table<String> t19_a = m_op.indexJoin(t14, "?x12", IndexDescription.PSOC, SO, names("?x12", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
		Table<String> t19 = m_op.mergeJoin(t19_a, t18, "?x9");
		t19 = m_op.compact(t19, Arrays.asList("?x12", "?x13", "?x10", "?x3", "?x1", "?x2", "?x7", "?x8", "?x5"));

		Table<String> t20 = m_op.indexJoin(t19, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t21 = m_op.indexJoin(t20, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x15"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t21 = m_op.refineWithPrunedPart(p2, "?x15", t21);
		t21 = m_op.compact(t21, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x8", "?x5", "?x6", "?x12", "?x10"));

		Table<String> t22 = m_op.indexJoin(t21, "?x2", IndexDescription.POCS, SO, names("?x17", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		t22 = m_op.compact(t22, Arrays.asList("?x3", "?x1", "?x7", "?x8", "?x5", "?x6", "?x12", "?x10", "?x17"));

		Table<String> t23 = m_op.indexJoin(t22, "?x17", IndexDescription.POCS, SO, names("?x4", "?x17"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t23 = m_op.compact(t23, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x8", "?x5", "?x6", "?x12", "?x10"));

		Table<String> t24 = m_op.indexJoin(t23, "?x4", "?x6", IndexDescription.PSOC, SO, names("?x4", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t24 = m_op.compact(t24, Arrays.asList("?x3", "?x1", "?x7", "?x8", "?x5", "?x12", "?x10"));

		}	public void query_q10_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");

		Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x8", "AssistantProfessor6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor6");
		Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x8");
		t4 = m_op.compact(t4, Arrays.asList("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"));

		Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");

		Table<String> t6_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent9@Department5.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent9@Department5.University0.edu");
		Table<String> t6 = m_op.mergeJoin(t6_a, t1, "?x1");
		t6 = m_op.compact(t6, Arrays.asList("?x1"));

		Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x7", "GraduateCourse39"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse39");

		Table<String> t8 = m_op.load(IndexDescription.POCS, SO, names("?x10", "FullProfessor7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor7");

		Table<String> t9 = m_op.load(IndexDescription.POCS, SO, names("?x12", "AssociateProfessor2"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor2");

		Table<String> t10_a = m_op.indexJoin(t6, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		Table<String> t10 = m_op.mergeJoin(t10_a, t7, "?x7");

		Table<String> t11_a = m_op.indexJoin(t4, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		Table<String> t11 = m_op.mergeJoin(t11_a, t10, "?x7");

		Table<String> t12_a = m_op.indexJoin(t2, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t12 = m_op.mergeJoin(t12_a, t11, "?x1");

		Table<String> t13_a = m_op.indexJoin(t12, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t13 = m_op.mergeJoin(t13_a, t8, "?x10");

		Table<String> t14 = m_op.indexJoin(t13, "?x1", IndexDescription.POCS, SO, names("?x14", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t15_a = m_op.indexJoin(t14, "?x14", IndexDescription.PSOC, SO, names("?x14", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t15 = m_op.mergeJoin(t15_a, t5, "?x5");
		t15 = m_op.compact(t15, Arrays.asList("?x10", "?x3", "?x1", "?x7", "?x8", "?x5"));

		Table<String> t16 = m_op.indexJoin(t9, "?x12", IndexDescription.PSOC, SO, names("?x12", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

		Table<String> t17_a = m_op.indexJoin(t15, "?x10", IndexDescription.PSOC, SO, names("?x10", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
		Table<String> t17 = m_op.mergeJoin(t17_a, t16, "?x9");
		t17 = m_op.compact(t17, Arrays.asList("?x12", "?x10", "?x3", "?x1", "?x7", "?x8", "?x5"));

		Table<String> t18 = m_op.indexJoin(t17, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");

		Table<String> t19 = m_op.indexJoin(t18, "?x12", IndexDescription.POCS, SO, names("?x13", "?x12"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t20 = m_op.indexJoin(t19, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t21 = m_op.indexJoin(t20, "?x2", IndexDescription.POCS, SO, names("?x17", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

		Table<String> t22 = m_op.indexJoin(t21, "?x2", IndexDescription.POCS, SO, names("?x18", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
		t22 = m_op.compact(t22, Arrays.asList("?x12", "?x13", "?x10", "?x17", "?x3", "?x1", "?x7", "?x8", "?x5", "?x6"));

		Table<String> t23 = m_op.indexJoin(t22, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x15"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t23 = m_op.compact(t23, Arrays.asList("?x12", "?x10", "?x17", "?x15", "?x3", "?x1", "?x7", "?x8", "?x5", "?x6"));

		Table<String> t24 = m_op.indexJoin(t23, "?x15", IndexDescription.PSOC, SO, names("?x15", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		t24 = m_op.compact(t24, Arrays.asList("?x3", "?x1", "?x7", "?x8", "?x5", "?x6", "?x12", "?x10", "?x11", "?x17"));

		Table<String> t25 = m_op.indexJoin(t24, "?x17", IndexDescription.POCS, SO, names("?x4", "?x17"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t25 = m_op.compact(t25, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x8", "?x5", "?x6", "?x12", "?x10", "?x11"));

		Table<String> t26 = m_op.indexJoin(t25, "?x4", "?x6", IndexDescription.PSOC, SO, names("?x4", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t26 = m_op.compact(t26, Arrays.asList("?x3", "?x1", "?x7", "?x8", "?x5", "?x12", "?x10", "?x11"));

		Table<String> t27 = m_op.indexJoin(t26, "?x11", IndexDescription.PSOC, SO, names("?x11", "?x16"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
		t27 = m_op.compact(t27, Arrays.asList("?x3", "?x1", "?x7", "?x8", "?x5", "?x12", "?x10"));

		m_op.compact(t27, Arrays.asList("?x1", "?x10", "?x12", "?x3", "?x5", "?x7", "?x8"));
		}
	
	public void graph_query4_vp() throws StorageException {
//		?x1 http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, 
			names("?x1", ns("lubm", "GraduateStudent")), 
			DataField.PROPERTY, ns("rdf", "type"), DataField.OBJECT, ns("lubm", "GraduateStudent"));

//		?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name "GraduateStudent107"
		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, 
			names("?x1", "GraduateStudent107"), 
			DataField.PROPERTY, ns("lubm", "name"), DataField.OBJECT, "GraduateStudent107");
		
//		?x2 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest "Research16"
		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, 
			names("?x2", "Research16"), 
			DataField.PROPERTY, ns("lubm", "researchInterest"), DataField.OBJECT, "Research16");

//		?x6 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name "Publication12"
		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, 
			names("?x6", "Publication12"), 
			DataField.PROPERTY, ns("lubm", "name"), DataField.OBJECT, "Publication12");

		Table<String> j1 = m_op.mergeJoin(t1, t2, "?x1");
		
//		?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor ?x2
		Table<String> j2 = m_op.indexJoin(j1, "?x1", IndexDescription.PSOC, SO,
			names("?x1", "?x2"), DataField.SUBJECT,
			DataField.PROPERTY, ns("lubm", "advisor"));
		
		Table<String> j3 = m_op.mergeJoin(j2, t3, "?x2");
		
//		?x6 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor ?x1
		Table<String> j4 = m_op.indexJoin(j3, "?x1", IndexDescription.POCS, SO, 
			names("?x6", "?x1"), DataField.OBJECT, 
			DataField.PROPERTY, ns("lubm", "publicationAuthor"));
		
		Table<String> j5 = m_op.mergeJoin(j4, t4, "?x6");

//		?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom ?x4
		Table<String> j10 = m_op.indexJoin(j5, "?x1", IndexDescription.PSOC, SO,
			names("?x1", "?x4"), DataField.SUBJECT,
			DataField.PROPERTY, ns("lubm", "undergraduateDegreeFrom"));
		
//		?x2 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor ?x3
		Table<String> j6 = m_op.indexJoin(j10, "?x2", IndexDescription.PSOC, SO, 
			names("?x2", "?x3"), DataField.SUBJECT, 
			DataField.PROPERTY, ns("lubm", "worksFor"));

//		?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf ?x3
		Table<String> j7 = m_op.indexJoin(j6, "?x1", "?x3", IndexDescription.PSOC, SO, 
			names("?x1", "?x3"), DataField.SUBJECT, 
			DataField.PROPERTY, ns("lubm", "memberOf"));
		
//		?x5 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor ?x2
		Table<String> j11 = m_op.indexJoin(j7, "?x2", IndexDescription.POCS, SO,
			names("?x5", "?x2"), DataField.OBJECT,
			DataField.PROPERTY, ns("lubm", "publicationAuthor"));
	}

//	?x1 http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent
//	?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name "GraduateStudent131"
//	?x4 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name "Department10"
//	?x2 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name "Publication13"
//	?x3 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest "Research25"
//	?x5 http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course
//	?x6 http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent

//	?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf ?x4
//	?x2 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor ?x1
//	?x3 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor ?x4
//	?x3 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf ?x5
//	?x6 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf ?x5
//	?x6 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf ?x4
//	?x6 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse ?x7
	public void graph_q2_vp() throws StorageException {
//		?x1 http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, 
			names("?x1", ns("lubm", "GraduateStudent")), 
			DataField.PROPERTY, ns("rdf", "type"), DataField.OBJECT, ns("lubm", "GraduateStudent"));
		
//		?x1 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name "GraduateStudent131"
		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, 
			names("?x1", "GraduateStudent131"), 
			DataField.PROPERTY, ns("lubm", "name"), DataField.OBJECT, "GraduateStudent131");

//		?x4 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name "Department10"
		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, 
			names("?x4", "Department10"), 
			DataField.PROPERTY, ns("lubm", "name"), DataField.OBJECT, "Department10");

//		?x2 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name "Publication13"
//		?x3 http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest "Research25"
//		?x5 http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course
//		?x6 http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent
		
	}
	
	public static void main(String[] args) throws IOException, StorageException, InterruptedException {
		OptionParser op = new OptionParser();
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");
		op.accepts("qf", "query file")
			.withRequiredArg().ofType(String.class);
		op.accepts("q", "query name")
			.withRequiredArg().ofType(String.class);
		op.accepts("s", "system")
			.withRequiredArg().ofType(String.class);
		op.accepts("rf", "result file")
			.withRequiredArg().ofType(String.class);
		op.accepts("r", "repeats")
			.withRequiredArg().ofType(Integer.class);
		op.accepts("dc", "drop caches script")
			.withRequiredArg().ofType(String.class);
		
		OptionSet os = op.parse(args);
		
		if (!os.has("o") || !os.has("s")) {
			op.printHelpOn(System.out);
			return;
		}

		String dropCachesScript = (String)os.valueOf("dc");
		int repeats = os.has("r") ? (Integer)os.valueOf("r") : 3;

		List<Long> vp = new ArrayList<Long>();
		List<Long> spc = new ArrayList<Long>();
		for (int i = 0; i < repeats; i++) {
			System.gc();
			if (dropCachesScript != null && new File(dropCachesScript).exists()) {
				Process p = Runtime.getRuntime().exec(dropCachesScript);
				p.waitFor();
			}
			IndexReader reader = new IndexReader(new IndexDirectory((String)os.valueOf("o")));
			QueryExecutor qe = new QueryExecutor(reader);

			long start = System.currentTimeMillis();
			qe.query_q10_vp();
			vp.add(System.currentTimeMillis() - start);
			System.out.println("vp  " + (System.currentTimeMillis() - start));
			
			System.gc();
			if (dropCachesScript != null && new File(dropCachesScript).exists()) {
				Process p = Runtime.getRuntime().exec(dropCachesScript);
				p.waitFor();
			}
			reader = new IndexReader(new IndexDirectory((String)os.valueOf("o")));
			qe = new QueryExecutor(reader);
			
			start = System.currentTimeMillis();
			qe.query_q10_spc();
			spc.add(System.currentTimeMillis() - start);
			System.out.println("spc " + (System.currentTimeMillis() - start));
		}

		long vp_avg = 0, spc_avg = 0;
		for (int i = 0; i < vp.size(); i++) {
			vp_avg += vp.get(i);
			spc_avg += spc.get(i);
		}
		
		log.debug("vp: " + vp_avg / vp.size() + " " + vp);
		log.debug("spc: " + spc_avg / spc.size() + " " + spc);
	}
}
