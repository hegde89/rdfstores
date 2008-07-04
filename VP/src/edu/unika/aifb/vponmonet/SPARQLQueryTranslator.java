package edu.unika.aifb.vponmonet;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.semanticweb.kaon2.api.Namespaces;
import org.semanticweb.kaon2.api.logic.Constant;
import org.semanticweb.kaon2.api.logic.Literal;
import org.semanticweb.kaon2.api.logic.Term;
import org.semanticweb.kaon2.api.logic.Variable;
import org.semanticweb.kaon2.api.owl.elements.DataProperty;
import org.semanticweb.kaon2.api.owl.elements.Description;
import org.semanticweb.kaon2.api.owl.elements.Individual;
import org.semanticweb.kaon2.api.owl.elements.OWLClass;
import org.semanticweb.kaon2.api.owl.elements.ObjectProperty;

import parser.ParseException;
import parser.VPSPARQLParser;

public class SPARQLQueryTranslator {
	private abstract class Pos {
		private AliasedTable m_table;
		private String m_col;
		
		protected Pos() {
			
		}
		
		protected Pos(AliasedTable t, String col) {
			m_table = t;
			m_col = col;
		}
		
		public AliasedTable getTable() {
			return m_table;
		}
		
		public String getColumn() {
			return m_col;
		}
		
		public void setTable(AliasedTable t, String col) {
			m_table = t;
			m_col = col;
		}
	}
	
	private class Var extends Pos {
		private String m_name;
		
		public Var(String name) {
			m_name = name;
		}
		
		public String getVar() {
			return m_name;
		}
		
		public String toString() {
			return "?" + m_name;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((m_name == null) ? 0 : m_name.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final Var other = (Var) obj;
			if (m_name == null) {
				if (other.m_name != null)
					return false;
			} else if (!m_name.equals(other.m_name))
				return false;
			return true;
		}
	}
	
	private class Value extends Pos {
		private Object m_value;
		private String m_type;
		
		public Value(Object m_value, String m_type) {
			this.m_value = m_value;
			this.m_type = m_type;
		}
		
		public Value(Object m_value, String m_type, String table, String col) {
			this(m_value, m_type);
		}
		
		public Object getValue() {
			return m_value;
		}
		
		public String getType() {
			return m_type;
		}
		
		public String toString() {
			return m_value + ":" + m_type;
		}
		
		public String toSQL() {
			return getTable().getAlias() + "." + getColumn() + " = ?";
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((m_type == null) ? 0 : m_type.hashCode());
			result = prime * result
					+ ((m_value == null) ? 0 : m_value.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final Value other = (Value) obj;
			if (m_type == null) {
				if (other.m_type != null)
					return false;
			} else if (!m_type.equals(other.m_type))
				return false;
			if (m_value == null) {
				if (other.m_value != null)
					return false;
			} else if (!m_value.equals(other.m_value))
				return false;
			return true;
		}
	}
	
	private class AliasedTable {
		private String m_alias;
		private String m_origName;
		private Pos m_subject;
		private Pos m_object;
		private Map<String,Pos> m_cols;
		
		public AliasedTable(String orig, String alias) {
			m_alias = alias;
			m_origName = orig;
			m_cols = new HashMap<String,Pos>();
		}
		
		public AliasedTable(String orig, String alias, Pos subject, Pos object) {
			this(orig, alias);
			subject.setTable(this, "subject");
			object.setTable(this, "object");
			m_cols.put("subject", subject);
			m_cols.put("object", object);
		}
		
		public String getAlias() {
			return m_alias;
		}
		
		public String getOrigName() {
			return m_origName;
		}
		
		public Pos getPos(String column) {
			return m_cols.get(column);
		}
		
		public void setPos(String column, Pos pos) {
			m_cols.put(column, pos);
		}

		public Pos getSubject() {
			return m_cols.get("subject");
		}

		public void setSubject(Pos subject) {
			m_cols.put("subject", subject);
		}

		public Pos getObject() {
			return m_cols.get("object");
		}

		public void setObject(Pos object) {
			m_cols.put("object", object);
		}
		
		public String toString() {
			String s = "[" + m_alias + "(" + m_origName + ")";
			String delim = "";
			for (String col : m_cols.keySet()) {
				s += delim + col + ": " + m_cols.get(col);
				delim = ", ";
			}
			return s + "]";
		}
		
		public String toSQL() {
			return m_origName + " AS " + m_alias;
		}
		
		public boolean hasVarAsSubject(Var v) {
			return m_subject.equals(v);
		}

		public boolean hasVarAsObject(Var v) {
			return m_subject.equals(v);
		}
	}
	
	private class Join {
		private Var m_joinVar;
		private List<String> m_tables;
		private Map<String,String> m_tblCol;
		
		public Join(Var var) {
			m_joinVar = var;
			m_tables = new ArrayList<String>();
			m_tblCol = new HashMap<String, String>();
		}
		
		public void addTable(String table, String column) {
			m_tables.add(table);
			m_tblCol.put(table, column);
		}
		
		public String toString() {
			String s = "[" + m_joinVar + " ";
			String delim = "";
			for (String t : m_tables) {
				s += delim + t + "." + m_tblCol.get(t);
				delim = ",";
			}
			return s + "]";
		}
		
		public String toSQL() {
			String s = "";
			String main = m_tables.get(0) + "." + m_tblCol.get(m_tables.get(0));
			String delim = "";
			for (int i = 1; i < m_tables.size(); i++) {
				s += delim + main + " = " + m_tables.get(i) + "." + m_tblCol.get(m_tables.get(i));
				delim = " AND ";
			}
			return s;
		}
		
		public String getMain() {
			return m_tables.get(0) + "." + m_tblCol.get(m_tables.get(0));
		}
		
		public boolean isJoin() {
			return m_tables.size() > 1;
		}
	}
	
	private class VariableAliases {
		private Map<Var,String> m_var2Alias;
		private Map<String,Var> m_alias2Var;
		private int m_varIdx = 0;
		
		public VariableAliases() {
			m_var2Alias = new HashMap<Var, String>();
			m_alias2Var = new HashMap<String, Var>();
		}
		
		public String getAlias(Var var) {
			return m_var2Alias.get(var);
		}
		
		public String addAlias(Var var) {
			String alias = m_var2Alias.get(var);
			if (alias == null) {
				alias = "__v" + m_varIdx;
				m_varIdx++;
				m_var2Alias.put(var, alias);
				m_alias2Var.put(alias, var);
			}
			return alias;
		}
		
		public Var getVariable(String alias) {
			return m_alias2Var.get(alias);
		}
		
		public Set<Var> getVariables() {
			return m_var2Alias.keySet();
		}
		
		public Set<String> getAliases() {
			return m_alias2Var.keySet();
		}
	}
	
	private OntologyMapping m_ontoMap;
	private Connection m_conn;
	private Set<AliasedTable> m_aliasedTables;
	private VariableAliases m_varAliases;
	private int m_aliasIdx = 0;
	
	private static Logger log = Logger.getLogger(SPARQLQueryTranslator.class);
	
	public SPARQLQueryTranslator(OntologyMapping ontoMap) {
		m_ontoMap = ontoMap;
		m_aliasedTables = new HashSet<AliasedTable>();
		m_varAliases = new VariableAliases();
	}
	
	private String nextAlias() {
		m_aliasIdx ++;
		return "t" + m_aliasIdx;
	}
	
	private void addAliasedTable(String orig, Pos subject, Pos object) {
		m_aliasedTables.add(new AliasedTable(orig, nextAlias(), subject, object));
	}
	
	public void reset() {
		m_aliasedTables.clear();
		m_varAliases = new VariableAliases();
	}

	public PreparedStatement translateQuery(String sparql) throws ImportException {
		reset();
		
        VPSPARQLParser parser = new VPSPARQLParser(new StringReader(sparql));
        
        VPSPARQLParser.Query query;
		try {
			query = parser.parseQuery(m_ontoMap, new Namespaces(Namespaces.INSTANCE));
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		
		for (Literal l : query.m_wherePattern.m_literals) {
			log.debug(l);
			if (l.getPredicate() instanceof Description) {
				if (!(l.getPredicate() instanceof OWLClass))
					throw new ImportException("description not an OWLClass");
				OWLClass c = (OWLClass)l.getPredicate();
				if (l.getArity() != 1)
					throw new ImportException("Literal with a description predicate should only have one argument");
				if (!(l.getArgument(0) instanceof Variable))
					throw new ImportException("Argument not a variable");
				Variable v = (Variable)l.getArgument(0);
				
				Pos subject = new Var(v.getVariableName());
				Pos object = new Value(URIHash.hash(c.getURI()), DatatypeMappings.XSD_URI);
				
				if (subject instanceof Var) {
					Var s = new Var(((Var)subject).getVar());
					Var o = new Var(m_varAliases.addAlias((Var)subject));
					addAliasedTable(OntologyMapping.URI_HASHES_TABLE, s, o);
				}
				
				addAliasedTable(OntologyMapping.TYPE_TABLE, subject, object);
			}
			else if (l.getPredicate() instanceof DataProperty) {
				DataProperty p = (DataProperty)l.getPredicate();
				
				if (l.getArity() != 1 && l.getArity() != 2)
					throw new ImportException("Literal with a property predicate should only have one or two arguments");

				Term arg1 = l.getArgument(0);
				Term arg2 = l.getArgument(1);

				Pos subject, object;
				
				if (arg1 instanceof Variable)
					subject = new Var(((Variable)arg1).getVariableName());
				else if (arg1 instanceof Individual)
					subject = new Value(URIHash.hash(((Individual)arg1).getURI()), DatatypeMappings.XSD_URI); 
				else
					throw new ImportException("Object position of a data property should be a variable or an uri.");

				if (arg2 instanceof Variable)
					object = new Var(((Variable)arg2).getVariableName());
				else if (arg2 instanceof Constant)
					object = new Value(((Constant)arg2).getValue(), m_ontoMap.getXSDTypeForProperty(p.getURI()));
				else
					throw new ImportException("Subject position of a data propety should be a variable or a constant.");
				
				if (subject instanceof Var) {
					Var s = new Var(((Var)subject).getVar());
					Var o = new Var(m_varAliases.addAlias((Var)subject));
					addAliasedTable(OntologyMapping.URI_HASHES_TABLE, s, o);
				}
				
				addAliasedTable(m_ontoMap.getPropertyTableName(p.getURI()), subject, object);
			}
			else if (l.getPredicate() instanceof ObjectProperty) {
				ObjectProperty p = (ObjectProperty)l.getPredicate();
				
				if (l.getArity() != 1 && l.getArity() != 2)
					throw new ImportException("Literal with a property predicate should only have one or two arguments");
				
				Term arg1 = l.getArgument(0);
				Term arg2 = l.getArgument(1);
				
				Pos subject, object;

				if (arg1 instanceof Variable)
					subject = new Var(((Variable)arg1).getVariableName());
				else if (arg1 instanceof Individual)
					subject = new Value(URIHash.hash(((Individual)arg1).getURI()), DatatypeMappings.XSD_URI);
				else 
					throw new ImportException("bla");

				if (arg2 instanceof Variable)
					object = new Var(((Variable)arg2).getVariableName());
				else if (arg2 instanceof Individual)
					object = new Value(URIHash.hash(((Individual)arg2).getURI()), DatatypeMappings.XSD_URI);
				else
					throw new ImportException("bla");

				if (subject instanceof Var) {
					Var s = new Var(((Var)subject).getVar());
					Var o = new Var(m_varAliases.addAlias((Var)subject));
					addAliasedTable(OntologyMapping.URI_HASHES_TABLE, s, o);
				}
				
				if (object instanceof Var) {
					Var s = new Var(((Var)object).getVar());
					Var o = new Var(m_varAliases.addAlias((Var)object));
					addAliasedTable(OntologyMapping.URI_HASHES_TABLE, s, o);
				}

				addAliasedTable(m_ontoMap.getPropertyTableName(p.getURI()), subject, object);
			}
		}
		
		Map<Var,Join> var2join = new HashMap<Var,Join>();
		List<Value> values = new ArrayList<Value>();
		for (AliasedTable at : m_aliasedTables) {
			log.debug(at);
			if (at.getSubject() instanceof Var) {
				Var v = (Var)at.getSubject();
				if (!var2join.containsKey(v))
					var2join.put(v, new Join(v));
				var2join.get(v).addTable(at.getAlias(), "subject");
			}
			else
				values.add((Value)at.getSubject());
			
			if (at.getObject() instanceof Var) {
				Var v = (Var)at.getObject();
				if (!var2join.containsKey(v))
					var2join.put(v, new Join(v));
				var2join.get(v).addTable(at.getAlias(), "object");
			}
			else
				values.add((Value)at.getObject());
		}
		
		for (Join j : var2join.values())
			log.debug(j);
		
		log.debug(query);
        
        String delim = "";
        String selectClause = "SELECT ";
        for (Variable v : query.m_distinguishedVariables) {
        	Var outputVar = new Var(v.getVariableName());
        	String joinVar = m_varAliases.getAlias(outputVar) != null ? m_varAliases.getAlias(outputVar) : outputVar.getVar();
        	Var var = new Var(joinVar);
        	if (var2join.containsKey(var)) {
	        	selectClause += delim + var2join.get(var).getMain() + " AS " + outputVar.getVar();
	        	delim = ", ";
        	}
        }
        
        delim = "";
        String fromClause = " FROM ";
        for (AliasedTable at : m_aliasedTables) {
        	fromClause += delim + at.toSQL();
        	delim = ", ";
        }
        
        delim = "";
        String joins = "";
        for (Join j : var2join.values()) {
        	if (j.isJoin()) {
	        	joins += delim + j.toSQL();
	        	delim = " AND ";
        	}
        }
        
        delim = "";
        String valueAssignments = "";
        for (Value v : values) {
        	valueAssignments += delim + v.toSQL();
        	delim = " AND ";
        }

        if (joins.equals(" AND ") || joins.length() == 0)
        	joins = "1 = 1";
        if (valueAssignments.length() == 0)
        	valueAssignments = "1 = 1";
        
        String whereClause = " WHERE " + joins + " AND " + valueAssignments;

		String sql = selectClause + fromClause + whereClause;
		log.debug(sql);
		
		try {
			PreparedStatement pst = m_conn.prepareStatement(sql);
			int parIdx = 1;
			int typeIdx = 0;
			for (Value v : values) {
				pst.setObject(parIdx, v.getValue(), m_ontoMap.getJDBCTypeForXSDType(v.getType()));
				typeIdx++;
				parIdx++;
			}
			return pst;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	public void setDbConnection(Connection conn) {
		m_conn = conn;
	}
}
