package edu.unika.aifb.graphindex.evaluation;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.DataIndex;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.query.PrunedQueryPart;
import edu.unika.aifb.graphindex.searcher.plan.QueryOperators;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public class GraphQuery {
	private IndexReader m_idxReader;
	private DataIndex m_dataIndex;
	private StructureIndex m_structureIndex;
	private Map<IndexDescription,IndexStorage> m_indexes;
	private QueryOperators m_op;

	public GraphQuery(IndexReader reader) throws IOException, StorageException {
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
	}
	
	private String[] names(String... names) {
		return names;
	}
	
	private DataField[] SO = new DataField[] { DataField.SUBJECT, DataField.OBJECT };

	public void query_q1_spc_2() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent90@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent90@Department2.University0.edu");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x10", "University0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "University0");

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x12", "AssistantProfessor7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor7");

		Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication7");

		Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x13", "Publication8"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication8");

		Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x11", "Publication4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication4");

		Table<String> t8 = m_op.load(IndexDescription.POCS, SO, names("?x8", "Publication4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication4");

		Table<String> t9 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Department2"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department2");

		Table<String> t10_a = m_op.load(IndexDescription.POCS, SO, names("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");
		Table<String> t10 = m_op.mergeJoin(t10_a, t8, "?x8");
		t10 = m_op.compact(t10, Arrays.asList("?x8"));

		Table<String> t11_a = m_op.indexJoin(t9, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
		Table<String> t11 = m_op.mergeJoin(t11_a, t3, "?x10");

		Table<String> t12_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
		Table<String> t12 = m_op.mergeJoin(t12_a, t2, "?x9");

		Table<String> t13_a = m_op.indexJoin(t5, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t13 = m_op.mergeJoin(t13_a, t12, "?x1");

		Table<String> t14_a = m_op.indexJoin(t4, "?x12", IndexDescription.PSOC, SO, names("?x12", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		Table<String> t14 = m_op.mergeJoin(t14_a, t11, "?x5");

		Table<String> t15_a = m_op.indexJoin(t13, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		Table<String> t15 = m_op.mergeJoin(t15_a, t14, "?x5");

		Table<String> t16 = m_op.indexJoin(t15, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

		Table<String> t17 = m_op.indexJoin(t7, "?x11", IndexDescription.PSOC, SO, names("?x11", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t18_a = m_op.indexJoin(t17, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
		Table<String> t18 = m_op.mergeJoin(t18_a, t16, "?x9");
		t18 = m_op.compact(t18, Arrays.asList("?x1", "?x2", "?x5", "?x6", "?x12", "?x10", "?x11", "?x9"));

		Table<String> t19 = m_op.indexJoin(t18, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

		Table<String> t20 = m_op.indexJoin(t19, "?x12", "?x2", IndexDescription.PSOC, SO, names("?x12", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		t20 = m_op.compact(t20, Arrays.asList("?x4", "?x1", "?x5", "?x6", "?x12", "?x10", "?x11", "?x9"));

		Table<String> t21 = m_op.indexJoin(t10, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t22_a = m_op.indexJoin(t6, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t22 = m_op.mergeJoin(t22_a, t20, "?x4");
		t22 = m_op.compact(t22, Arrays.asList("?x1", "?x5", "?x6", "?x12", "?x13", "?x10", "?x11", "?x9"));

		Table<String> t23_a = m_op.indexJoin(t22, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t23 = m_op.mergeJoin(t23_a, t21, "?x7");
		t23 = m_op.compact(t23, Arrays.asList("?x1", "?x8", "?x5", "?x6", "?x12", "?x13", "?x10", "?x11", "?x9"));

		}
		public void query_q2_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x7");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Publication13"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication13");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent131"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent131");
		Table<String> t4 = m_op.mergeJoin(t4_a, t1, "?x1");
		t4 = m_op.compact(t4, Arrays.asList("?x1"));

		Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

		Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Department10"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department10");

		Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x3", "Research25"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research25");

		Table<String> t8_a = m_op.indexJoin(t7, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		Table<String> t8 = m_op.mergeJoin(t8_a, t6, "?x4");

		Table<String> t9_a = m_op.indexJoin(t4, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x4");

		Table<String> t10_a = m_op.indexJoin(t3, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x4");

		Table<String> t11_a = m_op.indexJoin(t10, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		Table<String> t11 = m_op.mergeJoin(t11_a, t5, "?x5");

		Table<String> t12_a = m_op.indexJoin(t2, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t12 = m_op.mergeJoin(t12_a, t11, "?x1");

		Table<String> t13 = m_op.indexJoin(t12, "?x6", "?x5", IndexDescription.PSOC, SO, names("?x6", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
		t13 = m_op.refineWithPrunedPart(p1, "?x6", t13);

		}
		public void query_q3_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x8");
		p1.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "AssociateProfessor4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor4");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x13", "UndergraduateStudent125@Department9.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "UndergraduateStudent125@Department9.University0.edu");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Lecturer"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Lecturer");

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x7", "GraduateStudent99@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent99@Department2.University0.edu");

		Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

		Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x2", "UndergraduateStudent240@Department9.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "UndergraduateStudent240@Department9.University0.edu");

		Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x12", "GraduateStudent41"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent41");

		Table<String> t8_a = m_op.load(IndexDescription.POCS, SO, names("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent");
		Table<String> t8 = m_op.mergeJoin(t8_a, t6, "?x2");
		t8 = m_op.compact(t8, Arrays.asList("?x2"));

		Table<String> t9 = m_op.load(IndexDescription.POCS, SO, names("?x11", "Publication5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication5");

		Table<String> t10_a = m_op.load(IndexDescription.POCS, SO, names("?x11", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");
		Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x11");
		t10 = m_op.compact(t10, Arrays.asList("?x11"));

		Table<String> t11_a = m_op.indexJoin(t2, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t11 = m_op.mergeJoin(t11_a, t1, "?x1");

		Table<String> t12_a = m_op.indexJoin(t5, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t12 = m_op.mergeJoin(t12_a, t11, "?x1");

		Table<String> t13_a = m_op.indexJoin(t8, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t13 = m_op.mergeJoin(t13_a, t12, "?x1");

		Table<String> t14_a = m_op.indexJoin(t10, "?x11", IndexDescription.PSOC, SO, names("?x11", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t14 = m_op.mergeJoin(t14_a, t4, "?x7");

		Table<String> t15 = m_op.indexJoin(t14, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

		Table<String> t16_a = m_op.indexJoin(t13, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
		Table<String> t16 = m_op.mergeJoin(t16_a, t15, "?x4");
		t16 = m_op.compact(t16, Arrays.asList("?x1", "?x2", "?x7", "?x5", "?x13", "?x11"));

		Table<String> t17 = m_op.indexJoin(t16, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");

		Table<String> t18 = m_op.indexJoin(t17, "?x13", "?x3", IndexDescription.PSOC, SO, names("?x13", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

		Table<String> t19_a = m_op.indexJoin(t3, "?x9", IndexDescription.PSOC, SO, names("?x9", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		Table<String> t19 = m_op.mergeJoin(t19_a, t18, "?x3");
		t19 = m_op.compact(t19, Arrays.asList("?x1", "?x2", "?x7", "?x5", "?x13", "?x11", "?x9"));

		Table<String> t20 = m_op.indexJoin(t7, "?x12", IndexDescription.POCS, SO, names("?x6", "?x12"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t21_a = m_op.indexJoin(t20, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t21 = m_op.mergeJoin(t21_a, t19, "?x1");
		t21 = m_op.compact(t21, Arrays.asList("?x1", "?x2", "?x7", "?x5", "?x12", "?x13", "?x11", "?x9"));
		t21 = m_op.refineWithPrunedPart(p1, "?x1", t21);

		}
		public void query_q4_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x4");
		PrunedQueryPart p2 = new PrunedQueryPart("p", null);
		p2.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x2");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication12"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication12");

		Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent107"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent107");
		Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Research16"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research16");

		Table<String> t5_a = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x2");

		Table<String> t6_a = m_op.indexJoin(t2, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t6 = m_op.mergeJoin(t6_a, t5, "?x1");

		Table<String> t7 = m_op.indexJoin(t6, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");

		Table<String> t8 = m_op.indexJoin(t7, "?x1", "?x3", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		t8 = m_op.compact(t8, Arrays.asList("?x1", "?x2", "?x6"));
		t8 = m_op.refineWithPrunedPart(p1, "?x1", t8);
		t8 = m_op.refineWithPrunedPart(p2, "?x2", t8);

		}
		public void query_q5_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf", "?x3");
		PrunedQueryPart p2 = new PrunedQueryPart("p", null);
		p2.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x4");
		p2.addEdge("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x4");
		PrunedQueryPart p3 = new PrunedQueryPart("p", null);
		p3.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
		p3.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent44@Department3.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent44@Department3.University0.edu");
		Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
		t2 = m_op.compact(t2, Arrays.asList("?x1"));

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x7", "Publication15"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication15");

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication0");

		Table<String> t5_a = m_op.indexJoin(t3, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t5 = m_op.mergeJoin(t5_a, t2, "?x1");

		Table<String> t6 = m_op.indexJoin(t5, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t7_a = m_op.indexJoin(t4, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t7 = m_op.mergeJoin(t7_a, t6, "?x2");

		Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

		Table<String> t9 = m_op.indexJoin(t8, "?x2", "?x3", IndexDescription.PSOC, SO, names("?x2", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		t9 = m_op.refineWithPrunedPart(p1, "?x3", t9);
		t9 = m_op.compact(t9, Arrays.asList("?x1", "?x7", "?x6"));
		t9 = m_op.refineWithPrunedPart(p3, "?x1", t9);
		t9 = m_op.refineWithPrunedPart(p2, "?x6", t9);

		}
		public void query_q6_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x2");
		PrunedQueryPart p2 = new PrunedQueryPart("p", null);
		p2.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
		p2.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x6");
		p2.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
		p2.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x4");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Publication12"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication12");

		Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent47"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent47");
		Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");

		Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

		Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x2", "GraduateCourse13"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse13");

		Table<String> t7_a = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		Table<String> t7 = m_op.mergeJoin(t7_a, t6, "?x2");

		Table<String> t8_a = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		Table<String> t8 = m_op.mergeJoin(t8_a, t5, "?x9");

		Table<String> t9_a = m_op.indexJoin(t4, "?x10", IndexDescription.PSOC, SO, names("?x10", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x9");

		Table<String> t10_a = m_op.indexJoin(t2, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x1");

		Table<String> t11 = m_op.indexJoin(t10, "?x1", "?x10", IndexDescription.PSOC, SO, names("?x1", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t11 = m_op.refineWithPrunedPart(p2, "?x1", t11);
		t11 = m_op.refineWithPrunedPart(p1, "?x2", t11);

		}
		public void query_q7_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x10");
		PrunedQueryPart p2 = new PrunedQueryPart("p", null);
		p2.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
		p2.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x11");
		PrunedQueryPart p3 = new PrunedQueryPart("p", null);
		p3.addEdge("?x12", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x4");
		PrunedQueryPart p4 = new PrunedQueryPart("p", null);
		p4.addEdge("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor", "?x3");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x6", "GraduateStudent34@Department1.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent34@Department1.University0.edu");
		Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x6");
		t4 = m_op.compact(t4, Arrays.asList("?x6"));

		Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x4", "GraduateStudent107"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent107");

		Table<String> t6_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent92@Department1.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent92@Department1.University0.edu");
		Table<String> t6 = m_op.mergeJoin(t6_a, t1, "?x1");
		t6 = m_op.compact(t6, Arrays.asList("?x1"));

		Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

		Table<String> t8 = m_op.load(IndexDescription.POCS, SO, names("?x7", "GraduateCourse6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse6");

		Table<String> t9_a = m_op.load(IndexDescription.POCS, SO, names("?x5", "FullProfessor1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor1");
		Table<String> t9 = m_op.mergeJoin(t9_a, t2, "?x5");
		t9 = m_op.compact(t9, Arrays.asList("?x5"));

		Table<String> t10_a = m_op.indexJoin(t6, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		Table<String> t10 = m_op.mergeJoin(t10_a, t8, "?x7");

		Table<String> t11_a = m_op.indexJoin(t7, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t11 = m_op.mergeJoin(t11_a, t4, "?x6");

		Table<String> t12_a = m_op.indexJoin(t10, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t12 = m_op.mergeJoin(t12_a, t9, "?x5");

		Table<String> t13_a = m_op.indexJoin(t5, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		Table<String> t13 = m_op.mergeJoin(t13_a, t12, "?x7");

		Table<String> t14_a = m_op.indexJoin(t11, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t14 = m_op.mergeJoin(t14_a, t13, "?x5");

		Table<String> t15 = m_op.indexJoin(t14, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		t15 = m_op.refineWithPrunedPart(p4, "?x3", t15);

		Table<String> t16 = m_op.indexJoin(t15, "?x4", "?x3", IndexDescription.PSOC, SO, names("?x4", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		t16 = m_op.compact(t16, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x5", "?x6"));

		t16 = m_op.refineWithPrunedPart(p2, "?x1", t16);
		t16 = m_op.refineWithPrunedPart(p1, "?x6", t16);
		t16 = m_op.refineWithPrunedPart(p3, "?x4", t16);

		}
		public void query_q8_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x3");
		p1.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x4");
		PrunedQueryPart p2 = new PrunedQueryPart("p", null);
		p2.addEdge("?x13", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
		PrunedQueryPart p3 = new PrunedQueryPart("p", null);
		p3.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x14");
		PrunedQueryPart p4 = new PrunedQueryPart("p", null);
		p4.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x12");
		PrunedQueryPart p5 = new PrunedQueryPart("p", null);
		p5.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x6");
		p5.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x11");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent13"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent13");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x5", "AssistantProfessor6@Department5.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssistantProfessor6@Department5.University0.edu");
		t2 = m_op.refineWithPrunedPart(p3, "?x5", t2);

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x3", "University0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "University0");
		t3 = m_op.refineWithPrunedPart(p1, "?x3", t3);

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Course55"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Course55");

		Table<String> t5_a = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");
		Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x6");
		t5 = m_op.compact(t5, Arrays.asList("?x6"));

		Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x7", "Publication11"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication11");

		Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x9", "GraduateCourse39"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse39");

		Table<String> t8_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		Table<String> t8 = m_op.mergeJoin(t8_a, t7, "?x9");

		Table<String> t9_a = m_op.indexJoin(t8, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t9 = m_op.mergeJoin(t9_a, t2, "?x5");

		Table<String> t10_a = m_op.indexJoin(t6, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x1");

		Table<String> t11_a = m_op.indexJoin(t10, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
		Table<String> t11 = m_op.mergeJoin(t11_a, t5, "?x6");

		Table<String> t12 = m_op.indexJoin(t11, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");

		Table<String> t13_a = m_op.indexJoin(t12, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
		Table<String> t13 = m_op.mergeJoin(t13_a, t3, "?x3");

		Table<String> t14 = m_op.indexJoin(t13, "?x1", "?x2", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		t14 = m_op.compact(t14, Arrays.asList("?x3", "?x1", "?x7", "?x5", "?x6", "?x9"));

		t14 = m_op.refineWithPrunedPart(p2, "?x1", t14);
		t14 = m_op.refineWithPrunedPart(p5, "?x6", t14);
		t14 = m_op.refineWithPrunedPart(p4, "?x7", t14);
		}
		public void query_q9_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x9");
		PrunedQueryPart p2 = new PrunedQueryPart("p", null);
		p2.addEdge("?x14", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x8");
		p2.addEdge("?x11", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x8");
		PrunedQueryPart p3 = new PrunedQueryPart("p", null);
		p3.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x3");
		p3.addEdge("?x12", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x3");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent39"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent39");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Publication0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication0");

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t5_a = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication1");
		Table<String> t5 = m_op.mergeJoin(t5_a, t2, "?x6");
		t5 = m_op.compact(t5, Arrays.asList("?x6"));

		Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x7", "AssociateProfessor3@Department7.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssociateProfessor3@Department7.University0.edu");

		Table<String> t7_a = m_op.indexJoin(t3, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t7 = m_op.mergeJoin(t7_a, t1, "?x1");

		Table<String> t8_a = m_op.indexJoin(t4, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t8 = m_op.mergeJoin(t8_a, t6, "?x7");

		Table<String> t9_a = m_op.indexJoin(t5, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t9 = m_op.mergeJoin(t9_a, t7, "?x1");

		Table<String> t10_a = m_op.indexJoin(t9, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t10 = m_op.mergeJoin(t10_a, t8, "?x7");

		Table<String> t11 = m_op.indexJoin(t10, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t11 = m_op.refineWithPrunedPart(p2, "?x11", t11);
		t11 = m_op.compact(t11, Arrays.asList("?x4", "?x1", "?x7", "?x5", "?x6"));

		Table<String> t12 = m_op.indexJoin(t11, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x13"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

		Table<String> t13 = m_op.indexJoin(t12, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t14 = m_op.indexJoin(t13, "?x13", IndexDescription.POCS, SO, names("?x10", "?x13"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		t14 = m_op.refineWithPrunedPart(p3, "?x10", t14);
		t14 = m_op.compact(t14, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x5", "?x6", "?x10"));

		Table<String> t15 = m_op.indexJoin(t14, "?x2", "?x10", IndexDescription.PSOC, SO, names("?x2", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t15 = m_op.compact(t15, Arrays.asList("?x4", "?x1", "?x7", "?x5", "?x6"));
		t15 = m_op.refineWithPrunedPart(p1, "?x1", t15);

		}
		public void query_q10_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x11", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x16");
		p1.addEdge("?x15", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf", "?x11");
		PrunedQueryPart p2 = new PrunedQueryPart("p", null);
		p2.addEdge("?x18", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x2");
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
		t18 = m_op.refineWithPrunedPart(p2, "?x2", t18);

		Table<String> t19_a = m_op.indexJoin(t14, "?x12", IndexDescription.PSOC, SO, names("?x12", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
		Table<String> t19 = m_op.mergeJoin(t19_a, t18, "?x9");
		t19 = m_op.compact(t19, Arrays.asList("?x12", "?x13", "?x10", "?x3", "?x1", "?x2", "?x7", "?x8", "?x5"));

		Table<String> t20 = m_op.indexJoin(t19, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t21 = m_op.indexJoin(t20, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x15"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t21 = m_op.refineWithPrunedPart(p1, "?x15", t21);
		t21 = m_op.compact(t21, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x8", "?x5", "?x6", "?x12", "?x10"));

		Table<String> t22 = m_op.indexJoin(t21, "?x2", IndexDescription.POCS, SO, names("?x17", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		t22 = m_op.compact(t22, Arrays.asList("?x3", "?x1", "?x7", "?x8", "?x5", "?x6", "?x12", "?x10", "?x17"));

		Table<String> t23 = m_op.indexJoin(t22, "?x17", IndexDescription.POCS, SO, names("?x4", "?x17"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t23 = m_op.compact(t23, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x8", "?x5", "?x6", "?x12", "?x10"));

		Table<String> t24 = m_op.indexJoin(t23, "?x4", "?x6", IndexDescription.PSOC, SO, names("?x4", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t24 = m_op.compact(t24, Arrays.asList("?x3", "?x1", "?x7", "?x8", "?x5", "?x12", "?x10"));

		}
		public void query_q1_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent90@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent90@Department2.University0.edu");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x10", "University0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "University0");

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x12", "AssistantProfessor7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor7");

			Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication7");

			Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x13", "Publication8"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication8");

			Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x11", "Publication4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication4");

			Table<String> t8 = m_op.load(IndexDescription.POCS, SO, names("?x8", "Publication4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication4");

			Table<String> t9 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Department2"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department2");

			Table<String> t10_a = m_op.load(IndexDescription.POCS, SO, names("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");
			Table<String> t10 = m_op.mergeJoin(t10_a, t8, "?x8");
			t10 = m_op.compact(t10, Arrays.asList("?x8"));

			Table<String> t11_a = m_op.indexJoin(t9, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
			Table<String> t11 = m_op.mergeJoin(t11_a, t3, "?x10");

			Table<String> t12_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			Table<String> t12 = m_op.mergeJoin(t12_a, t2, "?x9");

			Table<String> t13_a = m_op.indexJoin(t5, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t13 = m_op.mergeJoin(t13_a, t12, "?x1");

			Table<String> t14_a = m_op.indexJoin(t4, "?x12", IndexDescription.PSOC, SO, names("?x12", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			Table<String> t14 = m_op.mergeJoin(t14_a, t11, "?x5");

			Table<String> t15_a = m_op.indexJoin(t13, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			Table<String> t15 = m_op.mergeJoin(t15_a, t14, "?x5");

			Table<String> t16 = m_op.indexJoin(t15, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

			Table<String> t17 = m_op.indexJoin(t7, "?x11", IndexDescription.PSOC, SO, names("?x11", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t18_a = m_op.indexJoin(t17, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			Table<String> t18 = m_op.mergeJoin(t18_a, t16, "?x9");
			t18 = m_op.compact(t18, Arrays.asList("?x1", "?x2", "?x5", "?x6", "?x12", "?x10", "?x11", "?x9"));

			Table<String> t19 = m_op.indexJoin(t18, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

			Table<String> t20 = m_op.indexJoin(t19, "?x12", "?x2", IndexDescription.PSOC, SO, names("?x12", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
			t20 = m_op.compact(t20, Arrays.asList("?x4", "?x1", "?x5", "?x6", "?x12", "?x10", "?x11", "?x9"));

			Table<String> t21 = m_op.indexJoin(t10, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t22_a = m_op.indexJoin(t6, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t22 = m_op.mergeJoin(t22_a, t20, "?x4");
			t22 = m_op.compact(t22, Arrays.asList("?x1", "?x5", "?x6", "?x12", "?x13", "?x10", "?x11", "?x9"));

			Table<String> t23_a = m_op.indexJoin(t22, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t23 = m_op.mergeJoin(t23_a, t21, "?x7");
			t23 = m_op.compact(t23, Arrays.asList("?x1", "?x8", "?x5", "?x6", "?x12", "?x13", "?x10", "?x11", "?x9"));

			}
			public void query_q2_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Publication13"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication13");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

			Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent131"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent131");
			Table<String> t4 = m_op.mergeJoin(t4_a, t1, "?x1");
			t4 = m_op.compact(t4, Arrays.asList("?x1"));

			Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

			Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Department10"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department10");

			Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x3", "Research25"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research25");

			Table<String> t8_a = m_op.indexJoin(t7, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			Table<String> t8 = m_op.mergeJoin(t8_a, t6, "?x4");

			Table<String> t9_a = m_op.indexJoin(t3, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x4");

			Table<String> t10_a = m_op.indexJoin(t4, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x4");

			Table<String> t11_a = m_op.indexJoin(t10, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
			Table<String> t11 = m_op.mergeJoin(t11_a, t5, "?x5");

			Table<String> t12 = m_op.indexJoin(t11, "?x3", "?x5", IndexDescription.PSOC, SO, names("?x3", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");

			Table<String> t13_a = m_op.indexJoin(t2, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t13 = m_op.mergeJoin(t13_a, t12, "?x1");

			Table<String> t14 = m_op.indexJoin(t13, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			t14 = m_op.compact(t14, Arrays.asList("?x3", "?x4", "?x1", "?x2", "?x5", "?x6"));

			}
			public void query_q3_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "AssociateProfessor4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor4");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x7", "GraduateStudent99@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent99@Department2.University0.edu");

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x2", "UndergraduateStudent240@Department9.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "UndergraduateStudent240@Department9.University0.edu");

			Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x13", "UndergraduateStudent125@Department9.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "UndergraduateStudent125@Department9.University0.edu");

			Table<String> t6_a = m_op.load(IndexDescription.POCS, SO, names("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent");
			Table<String> t6 = m_op.mergeJoin(t6_a, t4, "?x2");
			t6 = m_op.compact(t6, Arrays.asList("?x2"));

			Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x12", "GraduateStudent41"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent41");

			Table<String> t8 = m_op.load(IndexDescription.POCS, SO, names("?x11", "Publication5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication5");

			Table<String> t9 = m_op.load(IndexDescription.POCS, SO, names("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Lecturer"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Lecturer");

			Table<String> t10_a = m_op.load(IndexDescription.POCS, SO, names("?x11", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");
			Table<String> t10 = m_op.mergeJoin(t10_a, t8, "?x11");
			t10 = m_op.compact(t10, Arrays.asList("?x11"));

			Table<String> t11_a = m_op.indexJoin(t2, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t11 = m_op.mergeJoin(t11_a, t1, "?x1");

			Table<String> t12_a = m_op.indexJoin(t6, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			Table<String> t12 = m_op.mergeJoin(t12_a, t11, "?x1");

			Table<String> t13_a = m_op.indexJoin(t10, "?x11", IndexDescription.PSOC, SO, names("?x11", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t13 = m_op.mergeJoin(t13_a, t3, "?x7");

			Table<String> t14_a = m_op.indexJoin(t5, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			Table<String> t14 = m_op.mergeJoin(t14_a, t12, "?x1");

			Table<String> t15 = m_op.indexJoin(t13, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

			Table<String> t16 = m_op.indexJoin(t7, "?x12", IndexDescription.POCS, SO, names("?x6", "?x12"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t17 = m_op.indexJoin(t14, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

			Table<String> t18 = m_op.indexJoin(t17, "?x1", IndexDescription.POCS, SO, names("?x10", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t18 = m_op.compact(t18, Arrays.asList("?x3", "?x1", "?x2", "?x5", "?x13"));

			Table<String> t19_a = m_op.indexJoin(t18, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
			Table<String> t19 = m_op.mergeJoin(t19_a, t15, "?x4");
			t19 = m_op.compact(t19, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x5", "?x13", "?x11"));

			Table<String> t20_a = m_op.indexJoin(t9, "?x9", IndexDescription.PSOC, SO, names("?x9", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			Table<String> t20 = m_op.mergeJoin(t20_a, t19, "?x3");

			Table<String> t21 = m_op.indexJoin(t20, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
			t21 = m_op.compact(t21, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x5", "?x13", "?x11", "?x9"));

			Table<String> t22 = m_op.indexJoin(t21, "?x1", "?x3", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			t22 = m_op.compact(t22, Arrays.asList("?x1", "?x2", "?x7", "?x5", "?x13", "?x11", "?x9"));

			Table<String> t23_a = m_op.indexJoin(t16, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t23 = m_op.mergeJoin(t23_a, t22, "?x1");
			t23 = m_op.compact(t23, Arrays.asList("?x1", "?x2", "?x7", "?x5", "?x12", "?x13", "?x11", "?x9"));

			}
			public void query_q4_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

			Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent107"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent107");
			Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
			t2 = m_op.compact(t2, Arrays.asList("?x1"));

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Research16"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research16");

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication12"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication12");

			Table<String> t5_a = m_op.indexJoin(t2, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x2");

			Table<String> t6_a = m_op.indexJoin(t4, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t6 = m_op.mergeJoin(t6_a, t5, "?x1");

			Table<String> t7 = m_op.indexJoin(t6, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			t7 = m_op.compact(t7, Arrays.asList("?x1", "?x2", "?x6"));

			Table<String> t8 = m_op.indexJoin(t7, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");

			Table<String> t9 = m_op.indexJoin(t8, "?x1", "?x3", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			t9 = m_op.compact(t9, Arrays.asList("?x1", "?x2", "?x6"));

			Table<String> t10 = m_op.indexJoin(t9, "?x2", IndexDescription.POCS, SO, names("?x5", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t10 = m_op.compact(t10, Arrays.asList("?x1", "?x2", "?x6"));

			}
			public void query_q5_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x7", "Publication15"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication15");

			Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent44@Department3.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent44@Department3.University0.edu");
			Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
			t3 = m_op.compact(t3, Arrays.asList("?x1"));

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication0");

			Table<String> t5_a = m_op.indexJoin(t2, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x1");

			Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.POCS, SO, names("?x5", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t6 = m_op.compact(t6, Arrays.asList("?x1", "?x7"));

			Table<String> t7 = m_op.indexJoin(t6, "?x1", IndexDescription.POCS, SO, names("?x8", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t7 = m_op.compact(t7, Arrays.asList("?x1", "?x7"));

			Table<String> t8 = m_op.indexJoin(t4, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t9 = m_op.indexJoin(t8, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t10_a = m_op.indexJoin(t7, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x2");

			Table<String> t11 = m_op.indexJoin(t10, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

			Table<String> t12 = m_op.indexJoin(t11, "?x4", IndexDescription.POCS, SO, names("?x9", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t12 = m_op.compact(t12, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x6"));

			Table<String> t13 = m_op.indexJoin(t12, "?x2", "?x3", IndexDescription.PSOC, SO, names("?x2", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			t13 = m_op.compact(t13, Arrays.asList("?x3", "?x1", "?x7", "?x6"));

			Table<String> t14 = m_op.indexJoin(t13, "?x3", IndexDescription.POCS, SO, names("?x10", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf");
			t14 = m_op.compact(t14, Arrays.asList("?x1", "?x7", "?x6"));

			}
			public void query_q6_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Publication12"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication12");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");

			Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent47"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent47");
			Table<String> t4 = m_op.mergeJoin(t4_a, t1, "?x1");
			t4 = m_op.compact(t4, Arrays.asList("?x1"));

			Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

			Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x2", "GraduateCourse13"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse13");

			Table<String> t7_a = m_op.indexJoin(t2, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t7 = m_op.mergeJoin(t7_a, t4, "?x1");

			Table<String> t8_a = m_op.indexJoin(t3, "?x10", IndexDescription.PSOC, SO, names("?x10", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			Table<String> t8 = m_op.mergeJoin(t8_a, t5, "?x9");

			Table<String> t9_a = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x10");

			Table<String> t10 = m_op.indexJoin(t9, "?x1", "?x9", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

			Table<String> t11_a = m_op.indexJoin(t10, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			Table<String> t11 = m_op.mergeJoin(t11_a, t6, "?x2");

			Table<String> t12 = m_op.indexJoin(t11, "?x1", IndexDescription.POCS, SO, names("?x3", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t12 = m_op.compact(t12, Arrays.asList("?x1", "?x2", "?x5", "?x10", "?x9"));

			Table<String> t13 = m_op.indexJoin(t12, "?x1", IndexDescription.POCS, SO, names("?x7", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t13 = m_op.compact(t13, Arrays.asList("?x1", "?x2", "?x5", "?x10", "?x9"));

			Table<String> t14 = m_op.indexJoin(t13, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			t14 = m_op.compact(t14, Arrays.asList("?x1", "?x2", "?x5", "?x10", "?x9"));

			Table<String> t15 = m_op.indexJoin(t14, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			t15 = m_op.compact(t15, Arrays.asList("?x1", "?x2", "?x5", "?x10", "?x9"));

			Table<String> t16 = m_op.indexJoin(t15, "?x2", IndexDescription.POCS, SO, names("?x8", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			t16 = m_op.compact(t16, Arrays.asList("?x1", "?x2", "?x5", "?x10", "?x9"));

			}
			public void query_q7_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "GraduateStudent107"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent107");

			Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent92@Department1.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent92@Department1.University0.edu");
			Table<String> t4 = m_op.mergeJoin(t4_a, t1, "?x1");
			t4 = m_op.compact(t4, Arrays.asList("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent", "?x1"));

			Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x5", "FullProfessor1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor1");

			Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x7", "GraduateCourse6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse6");

			Table<String> t7_a = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor");
			Table<String> t7 = m_op.mergeJoin(t7_a, t5, "?x5");
			t7 = m_op.compact(t7, Arrays.asList("?x5"));

			Table<String> t8 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

			Table<String> t9_a = m_op.load(IndexDescription.POCS, SO, names("?x6", "GraduateStudent34@Department1.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent34@Department1.University0.edu");
			Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x6");
			t9 = m_op.compact(t9, Arrays.asList("?x6"));

			Table<String> t10_a = m_op.indexJoin(t2, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x6");

			Table<String> t11_a = m_op.indexJoin(t4, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			Table<String> t11 = m_op.mergeJoin(t11_a, t6, "?x7");

			Table<String> t12_a = m_op.indexJoin(t3, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			Table<String> t12 = m_op.mergeJoin(t12_a, t11, "?x7");

			Table<String> t13_a = m_op.indexJoin(t10, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t13 = m_op.mergeJoin(t13_a, t7, "?x5");

			Table<String> t14_a = m_op.indexJoin(t12, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			Table<String> t14 = m_op.mergeJoin(t14_a, t13, "?x5");

			Table<String> t15 = m_op.indexJoin(t14, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			t15 = m_op.compact(t15, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x5", "?x6"));

			Table<String> t16 = m_op.indexJoin(t15, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			t16 = m_op.compact(t16, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x5", "?x6"));

			Table<String> t17 = m_op.indexJoin(t16, "?x1", IndexDescription.POCS, SO, names("?x8", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t17 = m_op.compact(t17, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x5", "?x6"));

			Table<String> t18 = m_op.indexJoin(t17, "?x4", IndexDescription.POCS, SO, names("?x12", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t18 = m_op.compact(t18, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x5", "?x6"));

			Table<String> t19 = m_op.indexJoin(t18, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

			Table<String> t20 = m_op.indexJoin(t19, "?x1", "?x3", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

			Table<String> t21 = m_op.indexJoin(t20, "?x3", IndexDescription.POCS, SO, names("?x9", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			t21 = m_op.compact(t21, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x5", "?x6"));

			}
			public void query_q8_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent13"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent13");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x5", "AssistantProfessor6@Department5.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssistantProfessor6@Department5.University0.edu");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Course55"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Course55");

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x7", "Publication11"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication11");

			Table<String> t5_a = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");
			Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x6");
			t5 = m_op.compact(t5, Arrays.asList("?x6"));

			Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x9", "GraduateCourse39"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse39");

			Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x3", "University0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "University0");

			Table<String> t8_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			Table<String> t8 = m_op.mergeJoin(t8_a, t6, "?x9");

			Table<String> t9_a = m_op.indexJoin(t8, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			Table<String> t9 = m_op.mergeJoin(t9_a, t2, "?x5");

			Table<String> t10_a = m_op.indexJoin(t4, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x1");

			Table<String> t11_a = m_op.indexJoin(t10, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
			Table<String> t11 = m_op.mergeJoin(t11_a, t5, "?x6");

			Table<String> t12 = m_op.indexJoin(t11, "?x6", IndexDescription.POCS, SO, names("?x8", "?x6"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");

			Table<String> t13 = m_op.indexJoin(t12, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");

			Table<String> t14 = m_op.indexJoin(t13, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x12"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t14 = m_op.compact(t14, Arrays.asList("?x9", "?x1", "?x2", "?x7", "?x8", "?x5", "?x6"));

			Table<String> t15 = m_op.indexJoin(t14, "?x1", IndexDescription.POCS, SO, names("?x13", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t15 = m_op.compact(t15, Arrays.asList("?x1", "?x2", "?x7", "?x8", "?x5", "?x6", "?x9"));

			Table<String> t16_a = m_op.indexJoin(t15, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
			Table<String> t16 = m_op.mergeJoin(t16_a, t7, "?x3");

			Table<String> t17 = m_op.indexJoin(t16, "?x3", IndexDescription.POCS, SO, names("?x4", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");

			Table<String> t18 = m_op.indexJoin(t17, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x14"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
			t18 = m_op.compact(t18, Arrays.asList("?x3", "?x4", "?x1", "?x2", "?x7", "?x8", "?x5", "?x6", "?x9"));

			Table<String> t19 = m_op.indexJoin(t18, "?x1", "?x2", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			t19 = m_op.compact(t19, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x8", "?x5", "?x6", "?x9"));

			Table<String> t20 = m_op.indexJoin(t19, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
			t20 = m_op.compact(t20, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x5", "?x6", "?x9"));

			Table<String> t21 = m_op.indexJoin(t20, "?x4", IndexDescription.POCS, SO, names("?x10", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
			t21 = m_op.compact(t21, Arrays.asList("?x3", "?x1", "?x7", "?x5", "?x6", "?x9"));

			}
			public void query_q9_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent39"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent39");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x7", "AssociateProfessor3@Department7.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssociateProfessor3@Department7.University0.edu");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

			Table<String> t5_a = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication1");
			Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x6");
			t5 = m_op.compact(t5, Arrays.asList("?x6"));

			Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Publication0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication0");

			Table<String> t7_a = m_op.indexJoin(t5, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t7 = m_op.mergeJoin(t7_a, t1, "?x1");

			Table<String> t8_a = m_op.indexJoin(t3, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			Table<String> t8 = m_op.mergeJoin(t8_a, t2, "?x7");

			Table<String> t9_a = m_op.indexJoin(t6, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x7");

			Table<String> t10_a = m_op.indexJoin(t9, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t10 = m_op.mergeJoin(t10_a, t7, "?x1");

			Table<String> t11 = m_op.indexJoin(t10, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x13"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

			Table<String> t12 = m_op.indexJoin(t11, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

			Table<String> t13 = m_op.indexJoin(t12, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			t13 = m_op.compact(t13, Arrays.asList("?x13", "?x11", "?x4", "?x1", "?x7", "?x5", "?x6"));

			Table<String> t14 = m_op.indexJoin(t13, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t15 = m_op.indexJoin(t14, "?x13", IndexDescription.POCS, SO, names("?x10", "?x13"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			t15 = m_op.compact(t15, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x5", "?x6", "?x10", "?x11"));

			Table<String> t16 = m_op.indexJoin(t15, "?x11", IndexDescription.PSOC, SO, names("?x11", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
			t16 = m_op.compact(t16, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x8", "?x5", "?x6", "?x10"));

			Table<String> t17 = m_op.indexJoin(t16, "?x2", "?x10", IndexDescription.PSOC, SO, names("?x2", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t17 = m_op.compact(t17, Arrays.asList("?x4", "?x1", "?x7", "?x8", "?x5", "?x6", "?x10"));

			Table<String> t18 = m_op.indexJoin(t17, "?x10", IndexDescription.PSOC, SO, names("?x10", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
			t18 = m_op.compact(t18, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x8", "?x5", "?x6"));

			Table<String> t19 = m_op.indexJoin(t18, "?x8", IndexDescription.POCS, SO, names("?x14", "?x8"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			t19 = m_op.compact(t19, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x5", "?x6"));

			Table<String> t20 = m_op.indexJoin(t19, "?x3", IndexDescription.POCS, SO, names("?x12", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			t20 = m_op.compact(t20, Arrays.asList("?x4", "?x1", "?x7", "?x5", "?x6"));

			}
			public void query_q10_vp() throws StorageException {
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

			}
			public void query_q1_spc_1() throws StorageException {
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent90@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent90@Department2.University0.edu");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x10", "University0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "University0");

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x12", "AssistantProfessor7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor7");

				Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication7");

				Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x13", "Publication8"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication8");

				Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x11", "Publication4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication4");

				Table<String> t8 = m_op.load(IndexDescription.POCS, SO, names("?x8", "Publication4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication4");

				Table<String> t9 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Department2"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department2");

				Table<String> t10_a = m_op.load(IndexDescription.POCS, SO, names("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");
				Table<String> t10 = m_op.mergeJoin(t10_a, t8, "?x8");
				t10 = m_op.compact(t10, Arrays.asList("?x8"));

				Table<String> t11_a = m_op.indexJoin(t9, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
				Table<String> t11 = m_op.mergeJoin(t11_a, t3, "?x10");

				Table<String> t12_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
				Table<String> t12 = m_op.mergeJoin(t12_a, t2, "?x9");

				Table<String> t13_a = m_op.indexJoin(t5, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t13 = m_op.mergeJoin(t13_a, t12, "?x1");

				Table<String> t14_a = m_op.indexJoin(t4, "?x12", IndexDescription.PSOC, SO, names("?x12", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				Table<String> t14 = m_op.mergeJoin(t14_a, t11, "?x5");

				Table<String> t15_a = m_op.indexJoin(t13, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t15 = m_op.mergeJoin(t15_a, t14, "?x5");

				Table<String> t16 = m_op.indexJoin(t15, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

				Table<String> t17 = m_op.indexJoin(t7, "?x11", IndexDescription.PSOC, SO, names("?x11", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t18_a = m_op.indexJoin(t17, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
				Table<String> t18 = m_op.mergeJoin(t18_a, t16, "?x9");
				t18 = m_op.compact(t18, Arrays.asList("?x1", "?x2", "?x5", "?x6", "?x12", "?x10", "?x11", "?x9"));

				Table<String> t19 = m_op.indexJoin(t18, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

				Table<String> t20 = m_op.indexJoin(t19, "?x12", "?x2", IndexDescription.PSOC, SO, names("?x12", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
				t20 = m_op.compact(t20, Arrays.asList("?x4", "?x1", "?x5", "?x6", "?x12", "?x10", "?x11", "?x9"));

				Table<String> t21 = m_op.indexJoin(t10, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t22_a = m_op.indexJoin(t6, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t22 = m_op.mergeJoin(t22_a, t20, "?x4");
				t22 = m_op.compact(t22, Arrays.asList("?x1", "?x5", "?x6", "?x12", "?x13", "?x10", "?x11", "?x9"));

				Table<String> t23_a = m_op.indexJoin(t22, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t23 = m_op.mergeJoin(t23_a, t21, "?x7");
				t23 = m_op.compact(t23, Arrays.asList("?x1", "?x8", "?x5", "?x6", "?x12", "?x13", "?x10", "?x11", "?x9"));

				}
				public void query_q2_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x7");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Publication13"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication13");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
				t3 = m_op.refineWithPrunedPart(p1, "?x6", t3);

				Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent131"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent131");
				Table<String> t4 = m_op.mergeJoin(t4_a, t1, "?x1");
				t4 = m_op.compact(t4, Arrays.asList("?x1"));

				Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

				Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Department10"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department10");

				Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x3", "Research25"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research25");

				Table<String> t8_a = m_op.indexJoin(t7, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				Table<String> t8 = m_op.mergeJoin(t8_a, t6, "?x4");

				Table<String> t9_a = m_op.indexJoin(t4, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x4");

				Table<String> t10_a = m_op.indexJoin(t3, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x4");

				Table<String> t11_a = m_op.indexJoin(t10, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
				Table<String> t11 = m_op.mergeJoin(t11_a, t5, "?x5");

				Table<String> t12_a = m_op.indexJoin(t2, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t12 = m_op.mergeJoin(t12_a, t11, "?x1");

				Table<String> t13 = m_op.indexJoin(t12, "?x6", "?x5", IndexDescription.PSOC, SO, names("?x6", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");

				}
				public void query_q3_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x8");
				p1.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "AssociateProfessor4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor4");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x13", "UndergraduateStudent125@Department9.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "UndergraduateStudent125@Department9.University0.edu");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Lecturer"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Lecturer");

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x7", "GraduateStudent99@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent99@Department2.University0.edu");

				Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

				Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x2", "UndergraduateStudent240@Department9.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "UndergraduateStudent240@Department9.University0.edu");

				Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x12", "GraduateStudent41"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent41");

				Table<String> t8_a = m_op.load(IndexDescription.POCS, SO, names("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent");
				Table<String> t8 = m_op.mergeJoin(t8_a, t6, "?x2");
				t8 = m_op.compact(t8, Arrays.asList("?x2"));

				Table<String> t9 = m_op.load(IndexDescription.POCS, SO, names("?x11", "Publication5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication5");

				Table<String> t10_a = m_op.load(IndexDescription.POCS, SO, names("?x11", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");
				Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x11");
				t10 = m_op.compact(t10, Arrays.asList("?x11"));

				Table<String> t11_a = m_op.indexJoin(t2, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				Table<String> t11 = m_op.mergeJoin(t11_a, t1, "?x1");

				Table<String> t12_a = m_op.indexJoin(t5, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t12 = m_op.mergeJoin(t12_a, t11, "?x1");

				Table<String> t13_a = m_op.indexJoin(t8, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				Table<String> t13 = m_op.mergeJoin(t13_a, t12, "?x1");

				Table<String> t14_a = m_op.indexJoin(t10, "?x11", IndexDescription.PSOC, SO, names("?x11", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t14 = m_op.mergeJoin(t14_a, t4, "?x7");

				Table<String> t15 = m_op.indexJoin(t14, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

				Table<String> t16_a = m_op.indexJoin(t13, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
				Table<String> t16 = m_op.mergeJoin(t16_a, t15, "?x4");
				t16 = m_op.compact(t16, Arrays.asList("?x1", "?x2", "?x7", "?x5", "?x13", "?x11"));

				Table<String> t17 = m_op.indexJoin(t16, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");

				Table<String> t18 = m_op.indexJoin(t17, "?x13", "?x3", IndexDescription.PSOC, SO, names("?x13", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

				Table<String> t19_a = m_op.indexJoin(t3, "?x9", IndexDescription.PSOC, SO, names("?x9", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				Table<String> t19 = m_op.mergeJoin(t19_a, t18, "?x3");
				t19 = m_op.compact(t19, Arrays.asList("?x1", "?x2", "?x7", "?x5", "?x13", "?x11", "?x9"));

				Table<String> t20 = m_op.indexJoin(t7, "?x12", IndexDescription.POCS, SO, names("?x6", "?x12"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t21_a = m_op.indexJoin(t20, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t21 = m_op.mergeJoin(t21_a, t19, "?x1");
				t21 = m_op.compact(t21, Arrays.asList("?x1", "?x2", "?x7", "?x5", "?x12", "?x13", "?x11", "?x9"));

				}
				public void query_q4_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x4");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x2");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication12"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication12");

				Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent107"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent107");
				Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
				t3 = m_op.compact(t3, Arrays.asList("?x1"));

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Research16"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research16");
				t4 = m_op.refineWithPrunedPart(p2, "?x2", t4);

				Table<String> t5_a = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x2");

				Table<String> t6_a = m_op.indexJoin(t2, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t6 = m_op.mergeJoin(t6_a, t5, "?x1");

				Table<String> t7 = m_op.indexJoin(t6, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");

				Table<String> t8 = m_op.indexJoin(t7, "?x1", "?x3", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				t8 = m_op.compact(t8, Arrays.asList("?x1", "?x2", "?x6"));

				}
				public void query_q5_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x4");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf", "?x3");
				PrunedQueryPart p3 = new PrunedQueryPart("p", null);
				p3.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p3.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
				t1 = m_op.refineWithPrunedPart(p3, "?x1", t1);

				Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent44@Department3.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent44@Department3.University0.edu");
				Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
				t2 = m_op.compact(t2, Arrays.asList("?x1"));

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x7", "Publication15"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication15");

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication0");

				Table<String> t5_a = m_op.indexJoin(t3, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t5 = m_op.mergeJoin(t5_a, t2, "?x1");

				Table<String> t6 = m_op.indexJoin(t5, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t7 = m_op.indexJoin(t4, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t7 = m_op.refineWithPrunedPart(p1, "?x4", t7);
				t7 = m_op.compact(t7, Arrays.asList("?x6"));

				Table<String> t8_a = m_op.indexJoin(t7, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t8 = m_op.mergeJoin(t8_a, t6, "?x2");

				Table<String> t9 = m_op.indexJoin(t8, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				t9 = m_op.refineWithPrunedPart(p2, "?x3", t9);

				Table<String> t10 = m_op.indexJoin(t9, "?x2", "?x3", IndexDescription.PSOC, SO, names("?x2", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				t10 = m_op.compact(t10, Arrays.asList("?x1", "?x7", "?x6"));

				}
				public void query_q6_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x2");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p2.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x6");
				p2.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p2.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x4");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
				t1 = m_op.refineWithPrunedPart(p2, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Publication12"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication12");

				Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent47"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent47");
				Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
				t3 = m_op.compact(t3, Arrays.asList("?x1"));

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");

				Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

				Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x2", "GraduateCourse13"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse13");
				t6 = m_op.refineWithPrunedPart(p1, "?x2", t6);

				Table<String> t7_a = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				Table<String> t7 = m_op.mergeJoin(t7_a, t6, "?x2");

				Table<String> t8_a = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t8 = m_op.mergeJoin(t8_a, t5, "?x9");

				Table<String> t9_a = m_op.indexJoin(t4, "?x10", IndexDescription.PSOC, SO, names("?x10", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x9");

				Table<String> t10_a = m_op.indexJoin(t2, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x1");

				Table<String> t11 = m_op.indexJoin(t10, "?x1", "?x10", IndexDescription.PSOC, SO, names("?x1", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

				}
				public void query_q7_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x12", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x4");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p2.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x11");
				PrunedQueryPart p3 = new PrunedQueryPart("p", null);
				p3.addEdge("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor", "?x3");
				PrunedQueryPart p4 = new PrunedQueryPart("p", null);
				p4.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x10");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
				t1 = m_op.refineWithPrunedPart(p2, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
				t3 = m_op.refineWithPrunedPart(p4, "?x6", t3);

				Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x6", "GraduateStudent34@Department1.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent34@Department1.University0.edu");
				Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x6");
				t4 = m_op.compact(t4, Arrays.asList("?x6"));

				Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x4", "GraduateStudent107"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent107");
				t5 = m_op.refineWithPrunedPart(p1, "?x4", t5);

				Table<String> t6_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent92@Department1.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent92@Department1.University0.edu");
				Table<String> t6 = m_op.mergeJoin(t6_a, t1, "?x1");
				t6 = m_op.compact(t6, Arrays.asList("?x1"));

				Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

				Table<String> t8 = m_op.load(IndexDescription.POCS, SO, names("?x7", "GraduateCourse6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse6");

				Table<String> t9_a = m_op.load(IndexDescription.POCS, SO, names("?x5", "FullProfessor1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor1");
				Table<String> t9 = m_op.mergeJoin(t9_a, t2, "?x5");
				t9 = m_op.compact(t9, Arrays.asList("?x5"));

				Table<String> t10_a = m_op.indexJoin(t6, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				Table<String> t10 = m_op.mergeJoin(t10_a, t8, "?x7");

				Table<String> t11_a = m_op.indexJoin(t7, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t11 = m_op.mergeJoin(t11_a, t4, "?x6");

				Table<String> t12_a = m_op.indexJoin(t10, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				Table<String> t12 = m_op.mergeJoin(t12_a, t9, "?x5");

				Table<String> t13_a = m_op.indexJoin(t5, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				Table<String> t13 = m_op.mergeJoin(t13_a, t12, "?x7");

				Table<String> t14_a = m_op.indexJoin(t11, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t14 = m_op.mergeJoin(t14_a, t13, "?x5");

				Table<String> t15 = m_op.indexJoin(t14, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				t15 = m_op.refineWithPrunedPart(p3, "?x3", t15);

				Table<String> t16 = m_op.indexJoin(t15, "?x4", "?x3", IndexDescription.PSOC, SO, names("?x4", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				t16 = m_op.compact(t16, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x5", "?x6"));

				}
				public void query_q8_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x14");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x13", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				PrunedQueryPart p3 = new PrunedQueryPart("p", null);
				p3.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x4");
				PrunedQueryPart p4 = new PrunedQueryPart("p", null);
				p4.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x11");
				PrunedQueryPart p5 = new PrunedQueryPart("p", null);
				p5.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x12");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent13"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent13");
				t1 = m_op.refineWithPrunedPart(p2, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x5", "AssistantProfessor6@Department5.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssistantProfessor6@Department5.University0.edu");
				t2 = m_op.refineWithPrunedPart(p1, "?x5", t2);

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x3", "University0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "University0");

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Course55"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Course55");

				Table<String> t5_a = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");
				Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x6");
				t5 = m_op.compact(t5, Arrays.asList("?x6"));

				Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x7", "Publication11"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication11");
				t6 = m_op.refineWithPrunedPart(p5, "?x7", t6);

				Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x9", "GraduateCourse39"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse39");

				Table<String> t8_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				Table<String> t8 = m_op.mergeJoin(t8_a, t7, "?x9");

				Table<String> t9_a = m_op.indexJoin(t8, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				Table<String> t9 = m_op.mergeJoin(t9_a, t2, "?x5");

				Table<String> t10_a = m_op.indexJoin(t6, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x1");

				Table<String> t11_a = m_op.indexJoin(t10, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
				Table<String> t11 = m_op.mergeJoin(t11_a, t5, "?x6");

				Table<String> t12 = m_op.indexJoin(t11, "?x6", IndexDescription.POCS, SO, names("?x8", "?x6"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
				t12 = m_op.refineWithPrunedPart(p4, "?x8", t12);
				t12 = m_op.compact(t12, Arrays.asList("?x1", "?x7", "?x5", "?x6", "?x9"));

				Table<String> t13 = m_op.indexJoin(t3, "?x3", IndexDescription.POCS, SO, names("?x2", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");

				Table<String> t14 = m_op.indexJoin(t13, "?x3", IndexDescription.POCS, SO, names("?x4", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
				t14 = m_op.refineWithPrunedPart(p3, "?x4", t14);
				t14 = m_op.compact(t14, Arrays.asList("?x3", "?x2"));

				Table<String> t15_a = m_op.indexJoin(t12, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t15 = m_op.mergeJoin(t15_a, t14, "?x2");

				Table<String> t16 = m_op.indexJoin(t15, "?x5", "?x2", IndexDescription.PSOC, SO, names("?x5", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				t16 = m_op.compact(t16, Arrays.asList("?x3", "?x1", "?x7", "?x5", "?x6", "?x9"));

				}
				public void query_q9_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x14", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x8");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x9");
				PrunedQueryPart p3 = new PrunedQueryPart("p", null);
				p3.addEdge("?x12", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x3");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent39"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent39");
				t1 = m_op.refineWithPrunedPart(p2, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Publication0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication0");

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

				Table<String> t5_a = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication1");
				Table<String> t5 = m_op.mergeJoin(t5_a, t2, "?x6");
				t5 = m_op.compact(t5, Arrays.asList("?x6"));

				Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x7", "AssociateProfessor3@Department7.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssociateProfessor3@Department7.University0.edu");

				Table<String> t7_a = m_op.indexJoin(t3, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t7 = m_op.mergeJoin(t7_a, t1, "?x1");

				Table<String> t8_a = m_op.indexJoin(t4, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				Table<String> t8 = m_op.mergeJoin(t8_a, t6, "?x7");

				Table<String> t9_a = m_op.indexJoin(t5, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t9 = m_op.mergeJoin(t9_a, t7, "?x1");

				Table<String> t10_a = m_op.indexJoin(t9, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t10 = m_op.mergeJoin(t10_a, t8, "?x7");

				Table<String> t11 = m_op.indexJoin(t10, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x13"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

				Table<String> t12 = m_op.indexJoin(t11, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

				Table<String> t13 = m_op.indexJoin(t12, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t14 = m_op.indexJoin(t13, "?x13", IndexDescription.POCS, SO, names("?x10", "?x13"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				t14 = m_op.compact(t14, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x5", "?x6", "?x10", "?x11"));

				Table<String> t15 = m_op.indexJoin(t14, "?x11", IndexDescription.PSOC, SO, names("?x11", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
				t15 = m_op.refineWithPrunedPart(p1, "?x8", t15);
				t15 = m_op.compact(t15, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x5", "?x6", "?x10"));

				Table<String> t16 = m_op.indexJoin(t15, "?x2", "?x10", IndexDescription.PSOC, SO, names("?x2", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t16 = m_op.compact(t16, Arrays.asList("?x4", "?x1", "?x7", "?x5", "?x6", "?x10"));

				Table<String> t17 = m_op.indexJoin(t16, "?x10", IndexDescription.PSOC, SO, names("?x10", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
				t17 = m_op.refineWithPrunedPart(p3, "?x3", t17);
				t17 = m_op.compact(t17, Arrays.asList("?x4", "?x1", "?x7", "?x5", "?x6"));

				}
				public void query_q10_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x18", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x2");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x11", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x16");
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
				t21 = m_op.compact(t21, Arrays.asList("?x12", "?x10", "?x15", "?x3", "?x1", "?x2", "?x7", "?x8", "?x5", "?x6"));

				Table<String> t22 = m_op.indexJoin(t21, "?x2", IndexDescription.POCS, SO, names("?x17", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				t22 = m_op.compact(t22, Arrays.asList("?x3", "?x1", "?x7", "?x8", "?x5", "?x6", "?x12", "?x10", "?x17", "?x15"));

				Table<String> t23 = m_op.indexJoin(t22, "?x17", IndexDescription.POCS, SO, names("?x4", "?x17"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t23 = m_op.compact(t23, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x8", "?x5", "?x6", "?x12", "?x10", "?x15"));

				Table<String> t24 = m_op.indexJoin(t23, "?x15", IndexDescription.PSOC, SO, names("?x15", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				t24 = m_op.refineWithPrunedPart(p2, "?x11", t24);
				t24 = m_op.compact(t24, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x8", "?x5", "?x6", "?x12", "?x10"));

				Table<String> t25 = m_op.indexJoin(t24, "?x4", "?x6", IndexDescription.PSOC, SO, names("?x4", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t25 = m_op.compact(t25, Arrays.asList("?x3", "?x1", "?x7", "?x8", "?x5", "?x12", "?x10"));

				}
				}
