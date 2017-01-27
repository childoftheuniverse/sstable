Sorted String Table support for Go Filesystem objects
=====================================================

This package implements sorted string tables as defined by the Cassandra
project: <https://wiki.apache.org/cassandra/MemtableSSTable>

A sorted string table is essentially just a collection of records which are
structured as key-value pairs. The "sorted" part of the name refers to the
fact that this framework guarantees that sstable files written using it will
always have their keys in ascending order.

This means that if you have a record (k1, v1) followed by a record (k2, v2)
in the file, there is a guarantee that k2 >= k1.

Indices
-------

The sstable library can keep various types of indices for you when writing the
sstable. They can be configured in 2 different ways:

IndexType_EVERY_N means that regardless of the type of key, every n-th key
will cause a new index record to be written to the index file. This can
produce a large amount of index records so it's not always desirable.

IndexType_PREFIXLEN defines that there is a significant prefix of n bytes.
Whenever the first n bytes of the next key are different from those of the
previous key, a new index record will be written. This can of course have a
hugely varying density which can cause large gaps in coverage, but under
certain circumstances it can be more reasonable than IndexType_EVERY_N.
