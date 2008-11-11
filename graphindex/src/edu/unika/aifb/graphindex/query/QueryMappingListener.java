package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorCompletionService;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.data.Triple;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.graph.isomorphism.MappingListener;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.StatisticsCollector;

public class QueryMappingListener implements MappingListener {
	private StructureIndex m_index;
	private ExtensionManager m_em;
	private Graph<QueryNode> m_origQueryGraph;
	private Graph<QueryNode> m_condensedQueryGraph;
	private Graph<String> m_indexGraph;
	private StatisticsCollector m_collector;
	private ExecutorCompletionService<List<String[]>> m_completionService;
	private List<List<VertexMapping>> m_relatedMappings;
	private int m_numberOfMappings;
	private VCompatibilityCache m_vcc;
	private ExtensionStorage m_es;
	private List<Set<String>> m_nodeGroups;
	private List<Integer> m_compoundNodes;
	private List<Integer> m_nonCompoundNodes;
	private Set<String> m_noIncoming;
	private List<VertexMapping> m_submittedMappings;
	private List<Map<String,String>> m_mappings;
	private StructureIndexReader m_indexReader;
	private Set<String> m_signatures;
	
	private static final Logger log = Logger.getLogger(QueryMappingListener.class);

	public QueryMappingListener(Graph<QueryNode> orig, Graph<QueryNode> queryGraph, Graph<String> indexGraph, StructureIndexReader indexReader, VCompatibilityCache vcc, ExecutorCompletionService<List<String[]>> completionService) {
		m_indexReader = indexReader;
		m_index = m_indexReader.getIndex();
		
		m_em = m_index.getExtensionManager();
		m_es = m_em.getExtensionStorage();
		m_origQueryGraph = orig;
		m_condensedQueryGraph = queryGraph;
		m_indexGraph = indexGraph;
		m_vcc = vcc;
		m_completionService = completionService;
		m_collector = m_index.getCollector();
		m_relatedMappings = new ArrayList<List<VertexMapping>>();
		m_submittedMappings = new ArrayList<VertexMapping>();
		m_mappings = new ArrayList<Map<String,String>>();
		m_signatures = new HashSet<String>();
		
		// TODO do this in rcp algorithm
		for (int i = 0; i < m_condensedQueryGraph.nodeCount(); i++) {
			QueryNode qn = m_condensedQueryGraph.getNode(i);
			for (String v : qn.getMembers()) {
				m_origQueryGraph.getNode(m_origQueryGraph.getNodeId(new QueryNode(v))).setCondensed(qn);
			}
		}

		m_nodeGroups = new ArrayList<Set<String>>();
		m_noIncoming = new HashSet<String>();
		for (int i = 0; i < m_origQueryGraph.nodeCount(); i++) {
			QueryNode qn = m_origQueryGraph.getNode(i);
		
			if (m_origQueryGraph.inDegreeOf(i) == 0)
				for (String member : qn.getMembers())
					m_noIncoming.add(member);
			
			QueryNode condensed = qn.getCondensed();
			if (!condensed.isCompound())
				continue;
			
			Set<String> affectedNodes = new HashSet<String>();
			affectedNodes.add(m_origQueryGraph.getNode(i).getName());
			for (int c : m_origQueryGraph.predecessors(i)) {
				if (m_origQueryGraph.getNode(c).getCondensed().isCompound()) {
//					log.debug(m_origQueryGraph.getNode(i) + " affects " + m_origQueryGraph.getNode(c));
					affectedNodes.add(m_origQueryGraph.getNode(c).getName());
				}
			}
			
			for (int c : m_origQueryGraph.successors(i)) {
				if (m_origQueryGraph.getNode(c).getCondensed().isCompound()) {
//					log.debug(m_origQueryGraph.getNode(i) + " affects " + m_origQueryGraph.getNode(c));
					affectedNodes.add(m_origQueryGraph.getNode(c).getName());
				}
			}
			
			for (Set<String> nodeClass : m_nodeGroups) {
				boolean contains = false;
				for (String node : affectedNodes) {
					if (nodeClass.contains(node)) {
						contains = true;
						break;
					}
				}
				
				if (contains) {
					affectedNodes.addAll(nodeClass);
					nodeClass.clear();
				}
			}
			m_nodeGroups.add(affectedNodes);
		}
		
		for (Iterator<Set<String>> i = m_nodeGroups.iterator(); i.hasNext(); )
			if (i.next().size() == 0)
				i.remove();
		
		log.debug(m_nodeGroups);

		m_nonCompoundNodes = new ArrayList<Integer>();
		m_compoundNodes = new ArrayList<Integer>();
		for (int i = 0; i < m_condensedQueryGraph.nodeCount(); i++) {
			if (!m_condensedQueryGraph.getNode(i).isCompound())
				m_nonCompoundNodes.add(i);
			else
				m_compoundNodes.add(i);
		}
	}
	
	public boolean isRelated(VertexMapping vm1, VertexMapping vm2) {
		boolean related = true;
		for (int node : m_nonCompoundNodes) {
			QueryNode qn = m_condensedQueryGraph.getNode(node);
			if (!vm1.getVertexCorrespondence(qn.getName(), true).equals(vm2.getVertexCorrespondence(qn.getName(), true))) {
				related = false;
				break;
			}
		}
		
		return related;
	}
	
	private void generateAllMappings(List<VertexMapping> relatedMappings) throws StorageException {
		Map<String,List<String>> qv2mapped = new HashMap<String,List<String>>();
		
//		if (relatedMappings.size() == 1) {
//			m_numberOfMappings++;
//			Map<String,String> map = new HashMap<String,String>();
//			for (int node : m_nonCompoundNodes) {
//				String v1 = m_condensedQueryGraph.getNode(node).getSingleMember();
//				String v2 = relatedMappings.get(0).getVertexCorrespondence(m_condensedQueryGraph.getNode(node).getName(), true);
//				map.put(v1, v2);
//			}
//			m_completionService.submit(new MappingValidator(m_index, m_origQueryGraph, new VertexMapping(map), m_collector));
//			return;
//		}
		
//		Map<String,Set<String>> permittedExtensions = new HashMap<String,Set<String>>();
		boolean valid = true;
		Map<String,String> nonCompoundMap = new HashMap<String,String>();
		for (int node : m_nonCompoundNodes) {
			String v1 = m_condensedQueryGraph.getNode(node).getSingleMember();
			String v2 = relatedMappings.get(0).getVertexCorrespondence(m_condensedQueryGraph.getNode(node).getName(), true);
			if (!m_vcc.get(v1, v2)) {
				valid = false;
				break;
			}
			nonCompoundMap.put(v1, v2);
			
//			if (!v1.startsWith("?")) {
//				// subjects of triples contain a reference to the extension they occur in (at most one)
//				// this is used to constrain the mappings of parent nodes
//				for (GraphEdge<QueryNode> e : m_condensedQueryGraph.incomingEdges(node)) {
//					Set<String> possibleSourceExtensions = new HashSet<String>();
//					List<Triple> triples = m_es.getTriples(v2, e.getLabel(), v1);
//					for (Triple t : triples)
//						possibleSourceExtensions.add(t.getSubjectExtension());
//					permittedExtensions.put(m_condensedQueryGraph.getNode(e.getSrc()).getSingleMember(), possibleSourceExtensions);
//				}
//			}
		}
		
		if (!valid)
			return;
		
//		int verticesToMap = 0;
//		for (int node : m_compoundNodes) {
//			verticesToMap += m_condensedQueryGraph.getNode(node).getMembers().size();
//			
//			String qvl = m_condensedQueryGraph.getNode(node).getName();
//			
//			for (VertexMapping vm : relatedMappings) {
//				for (String member : m_condensedQueryGraph.getNode(node).getMembers()) {
//					int originalNode = m_origQueryGraph.getNodeId(new QueryNode(member));
//					if (!member.startsWith("?")) {
//						// same idea as above but now for ground term members of a compound node, which is more complicated
//						for (GraphEdge<QueryNode> e : m_origQueryGraph.incomingEdges(originalNode)) {
//							Set<String> possibleSourceExtensions = new HashSet<String>();
//							List<Triple> triples = m_es.getTriples(vm.getVertexCorrespondence(qvl, true), e.getLabel(), member);
//							for (Triple t : triples)
//								possibleSourceExtensions.add(t.getSubjectExtension());
//							
//							String sourceNode = m_origQueryGraph.getNode(e.getSrc()).getSingleMember();
//							if (!permittedExtensions.containsKey(sourceNode))
//								permittedExtensions.put(sourceNode, possibleSourceExtensions);
//							else
//								permittedExtensions.get(sourceNode).addAll(possibleSourceExtensions);
//						}
//					}
//				}
//			}
//		}
		
		List<List<Map<String,String>>> groupMappings = new ArrayList<List<Map<String,String>>>();
		for (Set<String> group : m_nodeGroups) {
			Set<Map<String,String>> mappings = new HashSet<Map<String,String>>();
			for (VertexMapping vm : relatedMappings) {
				Map<String,String> map = new HashMap<String,String>();
				boolean add = true;
				for (String node : group) {
					String ext = vm.getVertexCorrespondence(m_origQueryGraph.getNode(m_origQueryGraph.getNodeId(new QueryNode(node))).getCondensed().getName(), true);
					
//					if (permittedExtensions.containsKey(node) && !permittedExtensions.get(node).contains(ext)) {
//						add = false;
//						break;
//					}
					
					if (!node.startsWith("?") && !m_vcc.get(node, ext)) {
						add = false;
						break;
					}
					
					map.put(node, ext);
				}
				
				if (add)
					mappings.add(map);
			}
			
			if (mappings.size() == 0)
				return;
			groupMappings.add(new ArrayList<Map<String,String>>(mappings));
//			log.debug(group + ": " + mappings.size());
		}
		
		// TODO choosing a particular extension for a query node in a concrete mapping constrains
		// possible extensions for neighbouring query nodes
		// maybe build classes of query nodes on the equivalence relation xcy: "fixating x constrains possible mapping of y"
		// not all nodes constrain all other compound query nodes (ie. if all paths between these nodes contain at least one
		// non-compound node)
		
//		String[] nodes = new String [verticesToMap];
//		int[] state = new int [verticesToMap];
//		int[] limits = new int [verticesToMap];
//		
//		int i = 0;
//		for (int node : compoundNodes) {
//			String qvl = m_condensedQueryGraph.getNode(node).getName();
//			Set<String> mappedExtensions = new HashSet<String>();
//
//			for (VertexMapping vm : relatedMappings) {
//				mappedExtensions.add(vm.getVertexCorrespondence(qvl, true));
//			}
//			
//			for (String mext : m_condensedQueryGraph.getNode(node).getMembers()) {
//				List<String> exts = new ArrayList<String>(mappedExtensions);
//				for (Iterator<String> it = exts.iterator(); it.hasNext(); ) {
//					String ext = it.next();
//					
//					// check the constraints calculated above
//					if (permittedExtensions.containsKey(mext)) {
//						if (!permittedExtensions.get(mext).contains(ext)) {
//							it.remove();
//							continue;
//						}
//					}
//					
//					if (!mext.startsWith("?")) {
//						if (!m_vcc.get(mext, ext)) {
//							it.remove();
//							continue;
//						}
//					}
//				}
//				qv2mapped.put(mext, exts);
//				nodes[i] = mext;
//				state[i] = 0;
//				limits[i] = exts.size();
//				i++;
//			}
//		}
		
		int[] state = new int [groupMappings.size()];
		int[] limits = new int [groupMappings.size()];
		
		for (int i = 0; i < groupMappings.size(); i++)
			limits[i] = groupMappings.get(i).size();
		
//		int x = 1;
//		for (i = 0; i < limits.length; i++) {
//			log.debug(nodes[i] + " " + limits[i]);
//			x *= limits[i];
//		}
//		log.debug("mappings to generate: " + x);
//		
//		if (x == 0)
//			return;
		
		List<Map<String,String>> mappings = new ArrayList<Map<String,String>>();
		int x = 0;
		boolean carry = false;
		while (!carry) {
			valid = true;
			Map<String,String> map = new HashMap<String,String>(nonCompoundMap);
//			for (int n = 0; n < verticesToMap; n++) {
//				String v1 = nodes[n];
//				String v2 = qv2mapped.get(nodes[n]).get(state[n]);
//				if (!m_vcc.get(v1, v2)) {
//					valid = false;
//					break;
//				}
//				map.put(v1, v2);
//			}
			
			for (int n = 0; n < groupMappings.size(); n++) {
				map.putAll(groupMappings.get(n).get(state[n]));
			}
//			log.debug(map);

//			if (valid) {
//				for (int node : nonCompoundNodes) {
//					String v1 = m_queryGraph.getNode(node).getSingleMember();
//					String v2 = relatedMappings.get(0).getVertexCorrespondence(m_queryGraph.getNode(node).getName(), true);
//					if (!m_vcc.get(v1, v2)) {
//						valid = false;
//						break;
//					}
//					map.put(v1, v2);
//				}
//			}
			
			if (valid) {
//				m_numberOfMappings++;
//				log.debug("valid: " + map);
				VertexMapping vm = new VertexMapping(map);
				mappings.add(map);
//				m_completionService.submit(new MappingValidator(m_index, m_origQueryGraph, vm, m_collector));
				m_submittedMappings.add(vm);
			}
			
			carry = true;
			for (int i = 0; i < state.length; i++) {
				if (carry)
					state[i]++;
				
				if (state[i] >= limits[i]) {
					state[i] = 0;
					carry = true;
				}
				else
					carry = false;
			}
			x++;
		}
		m_mappings.addAll(mappings);
	}
	
	private Set<String> mappingDifference(Map<String,String> map1, Map<String,String> map2) {
		Set<String> diff = new HashSet<String>();
		for (String k : map1.keySet()) {
			if (!map1.get(k).equals(map2.get(k)))
				diff.add(k);
		}
		return diff;
	}
	
	private String getSignature(Map<String,String> map) {
		String sig = "";
		for (int i = 0; i < m_origQueryGraph.nodeCount(); i++) {
			String k = m_origQueryGraph.getNode(i).getSingleMember();
			if (m_noIncoming.contains(k))
				continue;
			sig += map.get(k) + "__";
		}
		return sig;
	}

	public void mapping(VertexMapping mapping) {
//		log.debug("mapping: " + mapping);
		
		if (m_compoundNodes.size() > 0) {
			// related mappings possible
			boolean foundRelated = false;

			// TODO really check if checking the last mapping is enough
			if (m_relatedMappings.size() > 0 && isRelated(m_relatedMappings.get(m_relatedMappings.size() - 1).get(0), mapping))
				m_relatedMappings.get(m_relatedMappings.size() - 1).add(mapping);
			else
				m_relatedMappings.add(new ArrayList<VertexMapping>(Arrays.asList(mapping)));
			
			// otherwise use this
//			for (int i = m_relatedMappings.size() - 1; i >= 0; i--) {
//				List<VertexMapping> lvm = m_relatedMappings.get(i);
//				if (isRelated(lvm.get(0), mapping)) {
//					lvm.add(mapping);
//					foundRelated = true;
//					log.debug("found related at " + i);
//					break;
//				}
//			}
//			if (!foundRelated)
//				m_relatedMappings.add(new ArrayList<VertexMapping>(Arrays.asList(mapping)));
		}
		else {
			Map<String,String> map = new HashMap<String,String>();
			for (int node = 0; node < m_condensedQueryGraph.nodeCount(); node++) {
				QueryNode qn = m_condensedQueryGraph.getNode(node);
				if (!m_vcc.get(qn.getSingleMember(), mapping.getVertexCorrespondence(qn.getName(), true)))
					return;
				map.put(qn.getSingleMember(), mapping.getVertexCorrespondence(qn.getName(), true));
			}
			
			String sig = getSignature(map);
			
			if (m_signatures.contains(sig))
				return;
			
//			for (Map<String,String> prev : m_mappings) {
			
//				boolean found = true;
//				for (String k : map.keySet()) {
//					if (!map.get(k).equals(prev.get(k)) && !m_noIncoming.contains(k)) {
//						found = false;
//						break;
//					}
//				}
//				
//				if (found)
//					return;
				
//				Set<String> diff = mappingDifference(prev, map);
//				if (diff.size() > 0) {
//					boolean allNoIncoming = true;
//					for (String v : diff) {
//						if (!m_noIncoming.contains(v)) {
//							allNoIncoming = false;
//							break;
//						}
//					}
//					
//					if (allNoIncoming) {
//						return;
//					}
//				}
			
//			}
			
			m_mappings.add(map);
			m_signatures.add(sig);
		}
	}

	public int generateMappings() throws StorageException {
//		log.debug("related mapping classes: " + m_relatedMappings.size());

		for (List<VertexMapping> relatedMappings : m_relatedMappings)
			generateAllMappings(relatedMappings);
		
		log.debug("mappings: " + m_mappings.size());
		
		if (m_mappings.size() == 0)
			return 0;
		
		m_completionService.submit(new MappingSetValidator(m_indexReader, m_origQueryGraph, m_mappings, m_collector));
		
		return 1;
	}
}
