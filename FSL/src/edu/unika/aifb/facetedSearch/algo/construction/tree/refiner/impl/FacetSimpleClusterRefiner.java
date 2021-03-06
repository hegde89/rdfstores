/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer Project. 
 * 
 * Faceted Search Layer Project is free software: you can redistribute
 * it and/or modify it under the terms of the GNU General Public License, 
 * version 2 as published by the Free Software Foundation. 
 *  
 * Faceted Search Layer Project is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 * GNU General Public License for more details. 
 *  
 * You should have received a copy of the GNU General Public License 
 * along with Faceted Search Layer Project.  If not, see <http://www.gnu.org/licenses/>. 
 */
package edu.unika.aifb.facetedSearch.algo.construction.tree.refiner.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.jcs.access.exception.CacheException;
import org.apache.log4j.Logger;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.EdgeType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeType;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.ComparatorPool;
import edu.unika.aifb.facetedSearch.algo.construction.tree.impl.BuilderHelper;
import edu.unika.aifb.facetedSearch.algo.construction.tree.refiner.IRefiner;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.DynamicClusterNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.SingleValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticClusterNode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.util.FacetUtils;

/**
 * @author andi
 * 
 */
public class FacetSimpleClusterRefiner implements IRefiner {

	private static Logger s_log = Logger
			.getLogger(FacetSingleLinkageClusterRefiner.class);

	/*
	 * 
	 */
	private SearchSession m_session;

	/*
	 * 
	 */
	private BuilderHelper m_helper;

	/*
	 * 
	 */
	private ComparatorPool m_compPool;

	/*
	 * 
	 */
	private HashSet<String> m_parsedFacetValues;
	private HashSet<String> m_parsedSources;

	public FacetSimpleClusterRefiner(SearchSession session, BuilderHelper helper) {

		m_session = session;
		m_helper = helper;

		init();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder#clean()
	 */
	public void clean() {
		m_parsedFacetValues.clear();
		m_parsedSources.clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder#close()
	 */
	public void close() {
		clean();
	}

	private void doClustering(FacetTree tree, StaticClusterNode node)
			throws CacheException, DatabaseException, IOException {

		int datatype = node.getCurrentFacet().getDataType() == DataType.NOT_SET
				? FacetEnvironment.DataType.STRING
				: node.getCurrentFacet().getDataType();

		List<AbstractSingleFacetValue> lits;

		if (!(node instanceof DynamicClusterNode)) {

			int countS = 0;
			int countSOverlapping = 0;

			lits = new ArrayList<AbstractSingleFacetValue>();

			String domain = node.getDomain();
			Iterator<String> subjIter = m_session.getCache().getSubjects4Node(
					node).iterator();

			while (subjIter.hasNext()) {

				String subject = subjIter.next();

				Iterator<AbstractSingleFacetValue> objIter = m_session
						.getCache().getObjects4StaticNode(node, subject)
						.iterator();

				while (objIter.hasNext()) {

					AbstractSingleFacetValue fv = objIter.next();

					Iterator<String> sourcesIter = m_session.getCache()
							.getSources4Object(domain, subject).iterator();

					if (sourcesIter.hasNext()) {

						while (sourcesIter.hasNext()) {

							String thisSource = sourcesIter.next();

							if (!m_parsedSources.contains(thisSource)) {

								m_session.getCache().addObject2SourceMapping(
										domain, fv.getValue(), thisSource);

								m_parsedSources.add(thisSource);
								countS++;
							}

							countSOverlapping++;
						}
					} else {

						if (!m_parsedSources.contains(subject)) {

							m_session.getCache().addObject2SourceMapping(
									domain, fv.getValue(), subject);

							m_parsedSources.add(subject);
							countS++;
						}

						countSOverlapping++;
					}

					if (!lits.contains(fv)) {
						lits.add(fv);
					}
				}
			}

			node.setCountFV(lits.size());
			node.setCountS(countS);
			node.setCountSOverlapping(countSOverlapping);

			/*
			 * merge sort: O(nlogn)
			 */
			Collections.sort(lits, m_compPool.getComparator(datatype));

		} else {

			lits = m_session.getCache().getLiterals4DynNode(
					(DynamicClusterNode) node);

			node.setCountFV(lits.size());
		}

		if (datatype == FacetEnvironment.DataType.DATE) {

			boolean hasCalChildren;
			boolean valueChange;

			int currentCalValue;
			int nextCalValue;
			int leftBorderIdx;
			int currentCalClusterDepth;

			if (node instanceof DynamicClusterNode) {
				currentCalClusterDepth = ((DynamicClusterNode) node)
						.getCalClusterDepth() + 1;
			} else {
				currentCalClusterDepth = FacetEnvironment.CalClusterDepth.YEAR;
			}

			if (currentCalClusterDepth != FacetEnvironment.CalClusterDepth.DAY) {

				int i = 0;

				while (i < lits.size()) {

					hasCalChildren = false;
					valueChange = false;

					currentCalValue = getCalValue((Literal) lits.get(i),
							currentCalClusterDepth);
					leftBorderIdx = i;

					if (currentCalValue != FacetEnvironment.CalClusterDepth.NOT_SET) {

						DynamicClusterNode dynNode = new DynamicClusterNode();
						dynNode.setLeftBorder(lits.get(i).getValue());
						dynNode.setContent(node.getContent());
						dynNode.setDomain(node.getDomain());
						dynNode.setTopLevelFacet(node.getTopLevelFacet());
						dynNode.setCurrentFacet(node.getCurrentFacet());
						dynNode.setCalClusterDepth(currentCalClusterDepth);
						dynNode.setHasCalChildren(false);

						ArrayList<AbstractSingleFacetValue> lits4Node = new ArrayList<AbstractSingleFacetValue>();
						lits4Node.add(lits.get(i));

						if (!hasCalChildren
								&& hasCalValue((Literal) lits.get(i),
										currentCalClusterDepth + 1)) {

							dynNode.setHasCalChildren(true);
							hasCalChildren = true;
						}

						while (!valueChange && (i < lits.size() - 1)) {

							i++;
							nextCalValue = getCalValue((Literal) lits.get(i),
									currentCalClusterDepth);

							if (nextCalValue != FacetEnvironment.CalClusterDepth.NOT_SET) {

								if ((currentCalValue != nextCalValue)) {

									valueChange = true;

									dynNode.setRightBorder(lits.get(i - 1)
											.getValue());
									dynNode
											.setValue(getLabel4CurrentCalClusterDepth(
													currentCalClusterDepth,
													currentCalValue));

									dynNode.setCountFV(lits4Node.size());

									m_session.getCache().storeLiterals(dynNode,
											lits4Node);

								} else {

									lits4Node.add(lits.get(i));

									if (!hasCalChildren
											&& hasCalValue((Literal) lits
													.get(i),
													currentCalClusterDepth + 1)) {

										dynNode.setHasCalChildren(true);
										hasCalChildren = true;
									}
								}
							}
						}

						if (!valueChange) {

							dynNode.setRightBorder(lits.get(Math.max(0, i - 1))
									.getValue());
							dynNode.setValue(getLabel4CurrentCalClusterDepth(
									currentCalClusterDepth, currentCalValue));
							dynNode.setCountFV(lits.subList(leftBorderIdx,
									lits.size()).size());

							/*
							 * store literals for this dynamic node
							 */

							m_session.getCache().storeLiterals(dynNode,
									lits.subList(leftBorderIdx, lits.size()));

						}

						tree.addVertex(dynNode);

						Edge edge = tree.addEdge(node, dynNode);
						edge.setType(EdgeType.CONTAINS);

						if (i == lits.size() - 1) {
							i++;
						}

					} else {
						i++;
					}
				}
			} else {

				int i = 0;

				while (i < lits.size()) {

					currentCalValue = getCalValue((Literal) lits.get(i),
							currentCalClusterDepth);

					if (currentCalValue != FacetEnvironment.CalClusterDepth.NOT_SET) {

						if (!m_parsedFacetValues.contains(lits.get(i)
								.getValue())) {

							SingleValueNode fvNode = new SingleValueNode(
									((Literal) lits.get(i)).getLiteralValue());
							fvNode.setContent(node.getContent());
							fvNode.setTopLevelFacet(node.getTopLevelFacet());
							fvNode.setCurrentFacet(node.getCurrentFacet());
							fvNode.setType(NodeType.LEAVE);
							fvNode.setDomain(node.getDomain());

							tree.addVertex(fvNode);

							Edge edge = tree.addEdge(node, fvNode);
							edge.setType(EdgeType.CONTAINS);

							m_parsedFacetValues.add(lits.get(i).getValue());
						}
					}

					i++;
				}

				m_parsedFacetValues.clear();
			}
		} else {

			if (lits.size() > FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE) {

				int delta = lits.size()
						/ FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE;

				int i = 0;

				while (i < lits.size()) {

					DynamicClusterNode dynNode = new DynamicClusterNode();
					dynNode.setLeftBorder(lits.get(i).getValue());
					dynNode.setContent(node.getContent());
					dynNode.setDomain(node.getDomain());
					dynNode.setTopLevelFacet(node.getTopLevelFacet());
					dynNode.setCurrentFacet(node.getCurrentFacet());

					if ((i + delta) >= lits.size()) {

						dynNode.setRightBorder(lits.get(lits.size() - 1)
								.getValue());
						dynNode.setValue("["
								+ FacetUtils.getNiceName(FacetUtils
										.getLiteralValue(dynNode
												.getLeftBorder()))
								+ "]"
								+ " - "
								+ "["
								+ FacetUtils.getNiceName(FacetUtils
										.getLiteralValue(dynNode
												.getRightBorder())) + "]");

						dynNode.setCountFV(lits.subList(i, lits.size()).size());

						m_session.getCache().storeLiterals(dynNode,
								lits.subList(i, lits.size()));

					} else {

						dynNode.setRightBorder(lits.get(i + delta).getValue());
						dynNode.setValue("["
								+ FacetUtils.getNiceName(FacetUtils
										.getLiteralValue(dynNode
												.getLeftBorder()))
								+ "]"
								+ " - "
								+ "["
								+ FacetUtils.getNiceName(FacetUtils
										.getLiteralValue(dynNode
												.getRightBorder())) + "]");

						dynNode.setCountFV(lits.subList(i, i + delta + 1)
								.size());

						m_session.getCache().storeLiterals(dynNode,
								lits.subList(i, i + delta + 1));
					}

					tree.addVertex(dynNode);

					Edge edge = tree.addEdge(node, dynNode);
					edge.setType(EdgeType.CONTAINS);

					i += delta + 1;
				}
			} else {

				m_helper.clean();
				m_helper.insertFacetValues(tree, node, lits);
			}
		}
	}

	private int getCalValue(Literal lit, int currentDepth) {

		XMLGregorianCalendar cal = (XMLGregorianCalendar) lit
				.getParsedLiteral();

		if (cal == null) {
			cal = XMLDatatypeUtil.parseCalendar(lit.getLiteralValue());
			lit.setParsedLiteral(cal);
		}

		switch (currentDepth) {
			case FacetEnvironment.CalClusterDepth.YEAR : {

				int year = cal.getYear();

				if (year != DatatypeConstants.FIELD_UNDEFINED) {
					return year;
				} else {
					return FacetEnvironment.CalClusterDepth.NOT_SET;
				}
			}
			case FacetEnvironment.CalClusterDepth.MONTH : {

				int month = cal.getMonth();

				if (month != DatatypeConstants.FIELD_UNDEFINED) {
					return month;
				} else {
					return FacetEnvironment.CalClusterDepth.NOT_SET;
				}
			}
			case FacetEnvironment.CalClusterDepth.DAY : {

				int day = cal.getDay();

				if (day != DatatypeConstants.FIELD_UNDEFINED) {
					return day;
				} else {
					return FacetEnvironment.CalClusterDepth.NOT_SET;
				}
			}
		}

		return FacetEnvironment.CalClusterDepth.NOT_SET;
	}

	private String getLabel4CurrentCalClusterDepth(int currentDepth, int value) {

		switch (currentDepth) {
			case FacetEnvironment.CalClusterDepth.YEAR : {
				return "Year: " + value;
			}
			case FacetEnvironment.CalClusterDepth.MONTH : {
				return "Month: " + getMonth4Value(value);
			}
			case FacetEnvironment.CalClusterDepth.DAY : {
				return "Day: " + String.valueOf(value);
			}
		}

		return "";
	}

	private String getMonth4Value(int value) {

		switch (value) {

			case 1 : {
				return "January";
			}
			case 2 : {
				return "February";
			}
			case 3 : {
				return "March";
			}
			case 4 : {
				return "April";
			}
			case 5 : {
				return "May";
			}
			case 6 : {
				return "June";
			}
			case 7 : {
				return "July";
			}
			case 8 : {
				return "August";
			}
			case 9 : {
				return "September";
			}
			case 10 : {
				return "October";
			}
			case 11 : {
				return "November";
			}
			case 12 : {
				return "December";
			}
		}

		return String.valueOf(value);
	}

	private boolean hasCalValue(Literal lit, int currentDepth) {
		return getCalValue(lit, currentDepth) != FacetEnvironment.CalClusterDepth.NOT_SET;
	}

	private void init() {

		m_compPool = ComparatorPool.getInstance();
		m_parsedFacetValues = new HashSet<String>();
		m_parsedSources = new HashSet<String>();
	}

	public boolean refine(FacetTree tree, StaticClusterNode node) {

		if (node.getCurrentFacet().isDataPropertyBased()) {

			if (node instanceof SingleValueNode) {
				return false;
			} else {

				long time1 = System.currentTimeMillis();

				try {

					doClustering(tree, node);

				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (CacheException e) {
					e.printStackTrace();
				}

				long time2 = System.currentTimeMillis();

				s_log.debug("did clustering for node '" + node + "' in "
						+ (time2 - time1) + " ms!");
				return true;
			}

		} else {
			s_log.error("facet " + node.getCurrentFacet()
					+ " has invalid facetType!");
			return false;
		}
	}
}