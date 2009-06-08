package edu.unika.aifb.keywordsearch.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import cern.jet.random.engine.MersenneTwister;

public class BloomFilter implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7008691576228185395L;

	// The random integers used as seed for MurmurHash
	private int[] init;
	private MurmurHash hashFunc_ = new MurmurHash();
	
//	private static List<ISimpleHash> hashLibrary_ = new ArrayList<ISimpleHash>();

//	static {
//		hashLibrary_.add(new RSHash());
//		hashLibrary_.add(new JSHash());
//		hashLibrary_.add(new PJWHash());
//		hashLibrary_.add(new ELFHash());
//		hashLibrary_.add(new BKDRHash());
//		hashLibrary_.add(new SDBMHash());
//		hashLibrary_.add(new DJBHash());
//		hashLibrary_.add(new DEKHash());
//		hashLibrary_.add(new BPHash());
//		hashLibrary_.add(new FNVHash());
//		hashLibrary_.add(new APHash());
//	}

	private BitSet filter_;

	private int count_;

	private int size_;

	private int hashes_;
	
	private Random random_ = new Random(System.currentTimeMillis());

	public BloomFilter(int bitsPerElement) {
		if (bitsPerElement < 1)
			throw new IllegalArgumentException("Number of bitsPerElement "
					+ "must be non-negative.");
		// Adding a small random number of bits so that even if the set
		// of elements hasn't changed, we'll get different false positives.
		size_ = 20 + random_.nextInt(64);
		filter_ = new BitSet(size_);
		hashes_ = BloomCalculations.computeBestK(bitsPerElement);
		init = new int[hashes_];
		final MersenneTwister mersenneTwister = new MersenneTwister( new Random().nextInt() );
		for( int i = 0; i < hashes_; i++ ) {
			init[ i ] = mersenneTwister.nextInt();
		}
	}

	public BloomFilter(int numElements, int bitsPerElement) {
		// TODO -- think about the trivial cases more.
		// Note that it should indeed be possible to send a bloom filter
		// that
		// encodes the empty set.
		if (numElements < 0 || bitsPerElement < 1)
			throw new IllegalArgumentException("Number of elements and bits "
					+ "must be non-negative.");
		// Adding a small random number of bits so that even if the set
		// of elements hasn't changed, we'll get different false positives.
		count_ = numElements;
		size_ = numElements * bitsPerElement + 20 + random_.nextInt(64);
		filter_ = new BitSet(size_);
		hashes_ = BloomCalculations.computeBestK(bitsPerElement);
		init = new int[hashes_];
		final MersenneTwister mersenneTwister = new MersenneTwister( new Random().nextInt() );
		for( int i = 0; i < hashes_; i++ ) {
			init[ i ] = mersenneTwister.nextInt();
		}
	}

	public BloomFilter(int numElements, double maxFalsePosProbability) {
		if (numElements < 0)
			throw new IllegalArgumentException("Number of elements must be "
					+ "non-negative.");
		BloomCalculations.BloomSpecification spec = BloomCalculations.computeBitsAndK(maxFalsePosProbability);
		// Add a small random number of bits so that even if the set
		// of elements hasn't changed, we'll get different false positives.
		count_ = numElements;
		size_ = numElements * spec.bitsPerElement + 20 + random_.nextInt(64);
		filter_ = new BitSet(size_);
		hashes_ = spec.K;
		init = new int[hashes_];
		final MersenneTwister mersenneTwister = new MersenneTwister( new Random().nextInt() );
		for( int i = 0; i < hashes_; i++ ) {
			init[ i ] = mersenneTwister.nextInt();
		}
	}

	int count() {
		return count_;
	}

	int size() {
		return size_;
	}

	int hashes() {
		return hashes_;
	}

	BitSet filter() {
		return filter_;
	}
	
	public BloomFilter merge(BloomFilter bf) {
		BloomFilter mergedBf = null;
		if (filter_.size() >= bf.filter_.size()) {
			filter_.or(bf.filter_);
			mergedBf = this;
		} else {
			bf.filter_.or(filter_);
			mergedBf = bf;
		}
		return mergedBf;
	}
	
	public String toString() {
		return filter_.toString();
	}

	public void add(String key) {
		for (int i = 0; i < hashes_; ++i) {
			if(key.startsWith("http://www."))
				key = key.substring(11);
			else if(key.startsWith("http://"))
				key = key.substring(7);
			int hashValue = hashFunc_.hash(key, init[i]);
			int index = Math.abs(hashValue % size_);
			filter_.set(index);
		}
	}
	
	public boolean contains(String key) {
		boolean bVal = true;
		for (int i = 0; i < hashes_; ++i) {
			if(key.startsWith("http://www."))
				key = key.substring(11);
			else if(key.startsWith("http://"))
				key = key.substring(7);
			int hashValue = hashFunc_.hash(key, init[i]);
			int index = Math.abs(hashValue % size_);
			if (!filter_.get(index)) {
				bVal = false;
				break;
			}
		}
		return bVal;
	}
	
//	public boolean isPresent(String key) {
//		boolean bVal = true;
//		for (int i = 0; i < hashes_; ++i) {
//			ISimpleHash hash = hashLibrary_.get(i);
//			int hashValue = hash.hash(key);
//			int index = Math.abs(hashValue % size_);
//			if (!filter_.get(index)) {
//				bVal = false;
//				break;
//			}
//		}
//		return bVal;
//	}

	/*
	 * param@ key -- value whose hash is used to fill the filter_. This is a
	 * general purpose API.
	 */
//	public void fill(String key) {
//		for (int i = 0; i < hashes_; ++i) {
//			ISimpleHash hash = hashLibrary_.get(i);
//			int hashValue = hash.hash(key);
//			int index = Math.abs(hashValue % size_);
//			filter_.set(index);
//		}
//	}
}

class MurmurHash implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7349905957826487735L;

	public int hash(byte[] data, int seed) {
		int m = 0x5bd1e995;
		int r = 24;

		int h = seed ^ data.length;

		int len = data.length;
		int len_4 = len >> 2;

		for (int i = 0; i < len_4; i++) {
			int i_4 = i << 2;
			int k = data[i_4 + 3];
			k = k << 8;
			k = k | (data[i_4 + 2] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 1] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 0] & 0xff);
			k *= m;
			k ^= k >>> r;
			k *= m;
			h *= m;
			h ^= k;
		}

		int len_m = len_4 << 2;
		int left = len - len_m;

		if (left != 0) {
			if (left >= 3) {
				h ^= (int) data[len - 3] << 16;
			}
			if (left >= 2) {
				h ^= (int) data[len - 2] << 8;
			}
			if (left >= 1) {
				h ^= (int) data[len - 1];
			}

			h *= m;
		}

		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;

		return h;
	}

	public int hash(String str, int seed) {
		return hash(str.getBytes(), seed);
	}
}

//interface ISimpleHash {
//	public int hash(String str);
//}
//
//class RSHash implements ISimpleHash {
//	public int hash(String str) {
//		int b = 378551;
//		int a = 63689;
//		int hash = 0;
//
//		for (int i = 0; i < str.length(); i++) {
//			hash = hash * a + str.charAt(i);
//			a = a * b;
//		}
//		return hash;
//	}
//}
//
//class JSHash implements ISimpleHash {
//	public int hash(String str) {
//		int hash = 1315423911;
//		for (int i = 0; i < str.length(); i++) {
//			hash ^= ((hash << 5) + str.charAt(i) + (hash >> 2));
//		}
//		return hash;
//	}
//}
//
//class PJWHash implements ISimpleHash {
//	public int hash(String str) {
//		int bitsInUnsignedInt = (4 * 8);
//		int threeQuarters = (bitsInUnsignedInt * 3) / 4;
//		int oneEighth = bitsInUnsignedInt / 8;
//		int highBits = (0xFFFFFFFF) << (bitsInUnsignedInt - oneEighth);
//		int hash = 0;
//		int test = 0;
//
//		for (int i = 0; i < str.length(); i++) {
//			hash = (hash << oneEighth) + str.charAt(i);
//
//			if ((test = hash & highBits) != 0) {
//				hash = ((hash ^ (test >> threeQuarters)) & (~highBits));
//			}
//		}
//		return hash;
//	}
//}
//
//class ELFHash implements ISimpleHash {
//	public int hash(String str) {
//		int hash = 0;
//		int x = 0;
//		for (int i = 0; i < str.length(); i++) {
//			hash = (hash << 4) + str.charAt(i);
//
//			if ((x = hash & 0xF0000000) != 0) {
//				hash ^= (x >> 24);
//			}
//			hash &= ~x;
//		}
//		return hash;
//	}
//}
//
//class BKDRHash implements ISimpleHash {
//	public int hash(String str) {
//		int seed = 131; // 31 131 1313 13131 131313 etc..
//		int hash = 0;
//		for (int i = 0; i < str.length(); i++) {
//			hash = (hash * seed) + str.charAt(i);
//		}
//		return hash;
//	}
//}
//
//class SDBMHash implements ISimpleHash {
//	public int hash(String str) {
//		int hash = 0;
//		for (int i = 0; i < str.length(); i++) {
//			hash = str.charAt(i) + (hash << 6) + (hash << 16) - hash;
//		}
//		return hash;
//	}
//}
//
//class DJBHash implements ISimpleHash {
//	public int hash(String str) {
//		int hash = 5381;
//		for (int i = 0; i < str.length(); i++) {
//			hash = ((hash << 5) + hash) + str.charAt(i);
//		}
//		return hash;
//	}
//}
//
//class DEKHash implements ISimpleHash {
//	public int hash(String str) {
//		int hash = str.length();
//		for (int i = 0; i < str.length(); i++) {
//			hash = ((hash << 5) ^ (hash >> 27)) ^ str.charAt(i);
//		}
//		return hash;
//	}
//}
//
//class BPHash implements ISimpleHash {
//	public int hash(String str) {
//		int hash = 0;
//		for (int i = 0; i < str.length(); i++) {
//			hash = hash << 7 ^ str.charAt(i);
//		}
//		return hash;
//	}
//}
//
//class FNVHash implements ISimpleHash {
//	public int hash(String str) {
//		int fnv_prime = 0x811C9DC5;
//		int hash = 0;
//		for (int i = 0; i < str.length(); i++) {
//			hash *= fnv_prime;
//			hash ^= str.charAt(i);
//		}
//		return hash;
//	}
//}
//
//class APHash implements ISimpleHash {
//	public int hash(String str) {
//		int hash = 0xAAAAAAAA;
//		for (int i = 0; i < str.length(); i++) {
//			if ((i & 1) == 0) {
//				hash ^= ((hash << 7) ^ str.charAt(i) ^ (hash >> 3));
//			} else {
//				hash ^= (~((hash << 11) ^ str.charAt(i) ^ (hash >> 5)));
//			}
//		}
//		return hash;
//	}
//}

