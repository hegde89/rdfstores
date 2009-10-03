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

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.model.impl.Resource;

/**
 * @author andi
 * 
 */
public class AbstractSingleFacetValueBinding extends TupleBinding<AbstractSingleFacetValue> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sleepycat.bind.tuple.TupleBinding#entryToObject(com.sleepycat.bind
	 * .tuple.TupleInput)
	 */
	@Override
	public AbstractSingleFacetValue entryToObject(TupleInput input) {

		AbstractSingleFacetValue fv;

		Boolean isResource = input.readBoolean();
		String value = input.readString();
		String sourceExt = input.readString();
		String rangeExt = input.readString();

		if (isResource) {
			fv = new Resource();
		} else {
			fv = new Literal();
		}

		fv.setIsResource(isResource);
		fv.setValue(value);
		fv.setSourceExt(sourceExt);
		fv.setRangeExt(rangeExt);

		return fv;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sleepycat.bind.tuple.TupleBinding#objectToEntry(java.lang.Object,
	 * com.sleepycat.bind.tuple.TupleOutput)
	 */
	@Override
	public void objectToEntry(AbstractSingleFacetValue object,
			TupleOutput output) {

		output.writeBoolean(object.isResource());
		output.writeString(object.getValue());
		output.writeString(object.getSourceExt());
		output.writeString(object.getRangeExt());
	}
}
