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
package edu.unika.aifb.facetedSearch.index.db.binding;

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;

/**
 * @author andi
 * 
 */
public class LiteralListBinding
		extends
			TupleBinding<List<AbstractSingleFacetValue>> {

	private AbstractSingleFacetValueBinding m_fvBinding;

	public LiteralListBinding() {
		m_fvBinding = new AbstractSingleFacetValueBinding();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sleepycat.bind.tuple.TupleBinding#entryToObject(com.sleepycat.bind
	 * .tuple.TupleInput)
	 */
	@Override
	public List<AbstractSingleFacetValue> entryToObject(TupleInput input) {

		List<AbstractSingleFacetValue> fvList = new ArrayList<AbstractSingleFacetValue>();
		int listSize = input.readInt();
		int count = 0;

		while (++count < listSize) {

			AbstractSingleFacetValue fv = m_fvBinding.entryToObject(input);
			fvList.add(fv);
		}

		return fvList;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sleepycat.bind.tuple.TupleBinding#objectToEntry(java.lang.Object,
	 * com.sleepycat.bind.tuple.TupleOutput)
	 */
	@Override
	public void objectToEntry(List<AbstractSingleFacetValue> object,
			TupleOutput output) {

		output.writeInt(object.size());

		for (AbstractSingleFacetValue fv : object) {
			m_fvBinding.objectToEntry(fv, output);
		}
	}
}