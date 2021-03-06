/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.unika.aifb.graphindex.storage.keyword;

import java.util.BitSet;

public class BloomFilter extends Filter {
	private static final long serialVersionUID = 5208974595676250223L;
	
	private BitSet filter_;

    public BloomFilter(int numElements, int bucketsPerElement)
    {
        this(BloomCalculations.computeBestK(bucketsPerElement), new BitSet(numElements * bucketsPerElement + 20));
    }

    public BloomFilter(int numElements, double maxFalsePosProbability)
    {
        BloomCalculations.BloomSpecification spec = BloomCalculations
                .computeBucketsAndK(maxFalsePosProbability);
        filter_ = new BitSet(numElements * spec.bucketsPerElement + 20);
        hashCount = spec.K;
    }

    /*
     * This version is only used by the deserializer.
     */
    BloomFilter(int hashes, BitSet filter)
    {
        hashCount = hashes;
        filter_ = filter;
    }

    public void clear()
    {
        filter_.clear();
    }

    int buckets()
    {
        return filter_.size();
    }

    BitSet filter()
    {
        return filter_;
    }

    public boolean contains(String key)
    {
        for (int bucketIndex : getHashBuckets(key))
        {
            if (!filter_.get(bucketIndex))
            {
                return false;
            }
        }
        return true;
    }

    public boolean contains(byte[] key)
    {
        for (int bucketIndex : getHashBuckets(key))
        {
            if (!filter_.get(bucketIndex))
            {
                return false;
            }
        }
        return true;
    }

    /*
     param@ key -- value whose hash is used to fill
     the filter_.
     This is a general purpose API.
     */
    public void add(String key)
    {
        for (int bucketIndex : getHashBuckets(key))
        {
            filter_.set(bucketIndex);
        }
    }

    public void add(byte[] key)
    {
        for (int bucketIndex : getHashBuckets(key))
        {
            filter_.set(bucketIndex);
        }
    }

    public String toString()
    {
        return filter_.toString();
    }

    int emptyBuckets()
    {
        int n = 0;
        for (int i = 0; i < buckets(); i++)
        {
            if (!filter_.get(i))
            {
                n++;
            }
        }
        return n;
    }
}

