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

public class EntityQuery {
	private IndexReader m_idxReader;
	private DataIndex m_dataIndex;
	private StructureIndex m_structureIndex;
	private Map<IndexDescription,IndexStorage> m_indexes;
	private QueryOperators m_op;

	public EntityQuery(IndexReader reader) throws IOException, StorageException {
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

	public void query_q1_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Publication2"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication2");

		}
		public void query_q2_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent34@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent34@Department2.University0.edu");

		Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent34"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent34");
		Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
		t2 = m_op.compact(t2, Arrays.asList("?x1"));

		Table<String> t3 = m_op.indexJoin(t2, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		}
		public void query_q3_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent73@Department3.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent73@Department3.University0.edu");

		Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent73"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent73");
		Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
		t2 = m_op.compact(t2, Arrays.asList("?x1"));

		Table<String> t3 = m_op.indexJoin(t2, "?x1", IndexDescription.POCS, SO, names("?x3", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		Table<String> t4 = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		t4 = m_op.compact(t4, Arrays.asList("?x1"));

		}
		public void query_q4_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent139@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent139@Department8.University0.edu");
		Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
		t2 = m_op.compact(t2, Arrays.asList("?x1"));

		Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent139"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent139");
		Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x1");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		Table<String> t4 = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t4 = m_op.compact(t4, Arrays.asList("?x1"));

		Table<String> t5 = m_op.indexJoin(t4, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		t5 = m_op.compact(t5, Arrays.asList("?x1"));

		Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.POCS, SO, names("?x4", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t6 = m_op.compact(t6, Arrays.asList("?x1"));

		}
		public void query_q5_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent69"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent69");

		Table<String> t2 = m_op.indexJoin(t1, "?x1", IndexDescription.POCS, SO, names("?x3", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t2 = m_op.compact(t2, Arrays.asList("?x1"));

		Table<String> t3 = m_op.indexJoin(t2, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		Table<String> t4 = m_op.indexJoin(t3, "?x1", IndexDescription.POCS, SO, names("?x5", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t4 = m_op.compact(t4, Arrays.asList("?x1"));

		Table<String> t5 = m_op.indexJoin(t4, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t5 = m_op.compact(t5, Arrays.asList("?x1"));

		}
		public void query_q6_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent33@Department9.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent33@Department9.University0.edu");
		Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
		t2 = m_op.compact(t2, Arrays.asList("?x1"));

		Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent33"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent33");
		Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x1");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		Table<String> t4 = m_op.indexJoin(t3, "?x1", IndexDescription.POCS, SO, names("?x5", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t4 = m_op.compact(t4, Arrays.asList("?x1"));

		Table<String> t5 = m_op.indexJoin(t4, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
		t5 = m_op.compact(t5, Arrays.asList("?x1"));

		Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.POCS, SO, names("?x3", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t6 = m_op.compact(t6, Arrays.asList("?x1"));

		Table<String> t7 = m_op.indexJoin(t6, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t7 = m_op.compact(t7, Arrays.asList("?x1"));

		Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t8 = m_op.compact(t8, Arrays.asList("?x1"));

		}
		public void query_q7_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "AssociateProfessor8@Department10.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssociateProfessor8@Department10.University0.edu");

		Table<String> t2 = m_op.indexJoin(t1, "?x1", IndexDescription.POCS, SO, names("?x3", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t2 = m_op.compact(t2, Arrays.asList("?x1"));

		Table<String> t3 = m_op.indexJoin(t2, "?x1", IndexDescription.POCS, SO, names("?x6", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		Table<String> t4 = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		t4 = m_op.compact(t4, Arrays.asList("?x1"));

		Table<String> t5 = m_op.indexJoin(t4, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
		t5 = m_op.compact(t5, Arrays.asList("?x1"));

		Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.POCS, SO, names("?x5", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t6 = m_op.compact(t6, Arrays.asList("?x1"));

		Table<String> t7 = m_op.indexJoin(t6, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
		t7 = m_op.compact(t7, Arrays.asList("?x1"));

		}
		public void query_q8_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent65@Department12.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent65@Department12.University0.edu");
		Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
		t2 = m_op.compact(t2, Arrays.asList("?x1"));

		Table<String> t3 = m_op.indexJoin(t2, "?x1", IndexDescription.POCS, SO, names("?x7", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		Table<String> t4 = m_op.indexJoin(t3, "?x1", IndexDescription.POCS, SO, names("?x5", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t4 = m_op.compact(t4, Arrays.asList("?x1"));

		Table<String> t5 = m_op.indexJoin(t4, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t5 = m_op.compact(t5, Arrays.asList("?x1"));

		Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		t6 = m_op.compact(t6, Arrays.asList("?x1"));

		Table<String> t7 = m_op.indexJoin(t6, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		t7 = m_op.compact(t7, Arrays.asList("?x1"));

		Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.POCS, SO, names("?x3", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t8 = m_op.compact(t8, Arrays.asList("?x1"));

		Table<String> t9 = m_op.indexJoin(t8, "?x1", IndexDescription.POCS, SO, names("?x4", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t9 = m_op.compact(t9, Arrays.asList("?x1"));

		}
		public void query_q9_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "FullProfessor2@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "FullProfessor2@Department2.University0.edu");

		Table<String> t2 = m_op.indexJoin(t1, "?x1", IndexDescription.POCS, SO, names("?x5", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t2 = m_op.compact(t2, Arrays.asList("?x1"));

		Table<String> t3 = m_op.indexJoin(t2, "?x1", IndexDescription.POCS, SO, names("?x8", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		Table<String> t4 = m_op.indexJoin(t3, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t4 = m_op.compact(t4, Arrays.asList("?x1"));

		Table<String> t5 = m_op.indexJoin(t4, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		t5 = m_op.compact(t5, Arrays.asList("?x1"));

		Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.POCS, SO, names("?x7", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t6 = m_op.compact(t6, Arrays.asList("?x1"));

		Table<String> t7 = m_op.indexJoin(t6, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
		t7 = m_op.compact(t7, Arrays.asList("?x1"));

		Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.POCS, SO, names("?x6", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t8 = m_op.compact(t8, Arrays.asList("?x1"));

		Table<String> t9 = m_op.indexJoin(t8, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		t9 = m_op.compact(t9, Arrays.asList("?x1"));

		}
		public void query_q10_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");

		Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "Research7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research7");
		Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
		t2 = m_op.compact(t2, Arrays.asList("?x1"));

		Table<String> t3 = m_op.indexJoin(t2, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		Table<String> t4 = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
		t4 = m_op.compact(t4, Arrays.asList("?x1"));

		Table<String> t5 = m_op.indexJoin(t4, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
		t5 = m_op.compact(t5, Arrays.asList("?x1"));

		Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.POCS, SO, names("?x10", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t6 = m_op.compact(t6, Arrays.asList("?x1"));

		Table<String> t7 = m_op.indexJoin(t6, "?x1", IndexDescription.POCS, SO, names("?x8", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t7 = m_op.compact(t7, Arrays.asList("?x1"));

		Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.POCS, SO, names("?x4", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t8 = m_op.compact(t8, Arrays.asList("?x1"));

		Table<String> t9 = m_op.indexJoin(t8, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
		t9 = m_op.compact(t9, Arrays.asList("?x1"));

		Table<String> t10 = m_op.indexJoin(t9, "?x1", IndexDescription.POCS, SO, names("?x5", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t10 = m_op.compact(t10, Arrays.asList("?x1"));

		Table<String> t11 = m_op.indexJoin(t10, "?x1", IndexDescription.POCS, SO, names("?x6", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t11 = m_op.compact(t11, Arrays.asList("?x1"));

		}
		public void query_q1_spc_2() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Publication2"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication2");

			}
			public void query_q2_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent34@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent34@Department2.University0.edu");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent34"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent34");
			Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
			t2 = m_op.compact(t2, Arrays.asList("?x1"));

			}
			public void query_q3_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf", "?x2");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent73@Department3.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent73@Department3.University0.edu");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent73"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent73");
			Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
			t2 = m_op.compact(t2, Arrays.asList("?x1"));

			}
			public void query_q4_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x3");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x2");
			p1.addEdge("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent139"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent139");
			Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
			t2 = m_op.compact(t2, Arrays.asList("?x1"));

			Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent139@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent139@Department8.University0.edu");
			Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x1");
			t3 = m_op.compact(t3, Arrays.asList("?x1"));

			}
			public void query_q5_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x4");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent69"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent69");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			}
			public void query_q6_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x6");
			p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x4");
			p1.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent33@Department9.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent33@Department9.University0.edu");
			Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
			t2 = m_op.compact(t2, Arrays.asList("?x1"));

			Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent33"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent33");
			Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x1");
			t3 = m_op.compact(t3, Arrays.asList("?x1"));

			}
			public void query_q7_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x1");
			p1.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x2");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom", "?x7");
			p1.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor", "?x4");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "AssociateProfessor8@Department10.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssociateProfessor8@Department10.University0.edu");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			}
			public void query_q8_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x6");
			p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf", "?x2");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x8");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

			Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent65@Department12.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent65@Department12.University0.edu");
			Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
			t2 = m_op.compact(t2, Arrays.asList("?x1"));
			t2 = m_op.refineWithPrunedPart(p1, "?x1", t2);

			}
			public void query_q9_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor", "?x4");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x9");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x3");
			p1.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "FullProfessor2@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "FullProfessor2@Department2.University0.edu");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			}
			public void query_q10_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x2");
			p1.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom", "?x9");
			p1.addEdge("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x3");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x7");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");

			Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "Research7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research7");
			Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
			t2 = m_op.compact(t2, Arrays.asList("?x1"));
			t2 = m_op.refineWithPrunedPart(p1, "?x1", t2);

			}

			public void query_q1_spc_1() throws StorageException {
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Publication2"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication2");

				}
				public void query_q2_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent34@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent34@Department2.University0.edu");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent34"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent34");
				Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
				t2 = m_op.compact(t2, Arrays.asList("?x1"));

				}
				public void query_q3_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf", "?x2");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent73@Department3.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent73@Department3.University0.edu");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent73"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent73");
				Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
				t2 = m_op.compact(t2, Arrays.asList("?x1"));

				}
				public void query_q4_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x3");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x2");
				p1.addEdge("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent139"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent139");
				Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
				t2 = m_op.compact(t2, Arrays.asList("?x1"));

				Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent139@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent139@Department8.University0.edu");
				Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x1");
				t3 = m_op.compact(t3, Arrays.asList("?x1"));

				}
				public void query_q5_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x4");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent69"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent69");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				}
				public void query_q6_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x6");
				p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x4");
				p1.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent33@Department9.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent33@Department9.University0.edu");
				Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
				t2 = m_op.compact(t2, Arrays.asList("?x1"));

				Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent33"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent33");
				Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x1");
				t3 = m_op.compact(t3, Arrays.asList("?x1"));

				}
				public void query_q7_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x1");
				p1.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x2");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom", "?x7");
				p1.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor", "?x4");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "AssociateProfessor8@Department10.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssociateProfessor8@Department10.University0.edu");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				}
				public void query_q8_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x6");
				p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf", "?x2");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x8");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent65@Department12.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent65@Department12.University0.edu");
				Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
				t2 = m_op.compact(t2, Arrays.asList("?x1"));

				}
				public void query_q9_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor", "?x4");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x9");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x3");
				p1.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "FullProfessor2@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "FullProfessor2@Department2.University0.edu");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				}
				public void query_q10_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x2");
				p1.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom", "?x9");
				p1.addEdge("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x3");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x7");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "Research7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research7");
				Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
				t2 = m_op.compact(t2, Arrays.asList("?x1"));

				}
	}
