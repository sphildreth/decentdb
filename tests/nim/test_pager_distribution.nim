import unittest
import pager/pager

suite "Pager Distribution":
  test "splitmix64 produces reasonable distribution":
    # Basic sanity check that we don't map everything to the same bucket
    var buckets: array[16, int]
    for i in 0 ..< 1000:
      let hash = splitmix64(uint64(i))
      let bucket = int(hash mod 16)
      buckets[bucket].inc
    
    # Check that no bucket is empty (highly unlikely with uniform dist)
    for i in 0 ..< 16:
      check buckets[i] > 0
      
    # Check that no bucket has > 2x expected load (expected ~62)
    # This is a loose check to avoid flakiness but catch terrible hashing
    for i in 0 ..< 16:
      check buckets[i] < 125
