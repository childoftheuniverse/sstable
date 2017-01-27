package sstable

import (
	"errors"
	"io"
	"strings"

	"github.com/childoftheuniverse/filesystem"
	"github.com/childoftheuniverse/recordio"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

/*
Err_NotSeeker indicates that seeking was attempted on an I/O object which does
not support seeks.
*/
var Err_NotSeeker error = errors.New(
	"Seeks not supported")

/*
Reader implements various ways of reading data from an sstable file:
indexed reads, or simple linear lookups.
*/
type Reader struct {
	in          *recordio.RecordReader
	orig_in     filesystem.ReadCloser
	offset      int64
	in_idx      *recordio.RecordReader
	orig_in_idx filesystem.ReadCloser
	idx_offset  int64

	cache_entry_index bool
	entry_index_cache map[string]int64
}

/*
NewReader creates a new, linear-lookup sstable reader around the specified
ReadCloser.
*/
func NewReader(in filesystem.ReadCloser) *Reader {
	return &Reader{
		orig_in: in,
		in:      recordio.NewRecordReader(in),
	}
}

/*
NewReaderWithIdx creates a new, index-lookup sstable reader around the given
ReadClosers for the data and index input streams. If requested using the
create_cache flag, the index will be scanned entirely upon initialization and
kept in memory in tree form in order to speed up future lookups.

A working Reader is always going to be returned. The error will indicate only
whether the index could be loaded into memory successfully.

The context will only be used for reading the index.
*/
func NewReaderWithIdx(
	ctx context.Context, sst filesystem.ReadCloser, idx filesystem.ReadCloser,
	create_cache bool) (*Reader, error) {
	var err error

	var rd *Reader = &Reader{
		orig_in:           sst,
		in:                recordio.NewRecordReader(sst),
		orig_in_idx:       idx,
		in_idx:            recordio.NewRecordReader(idx),
		cache_entry_index: create_cache,
		entry_index_cache: make(map[string]int64),
	}

	if create_cache {
		err = rd.cacheEntryIndex(ctx)
	}

	return rd, err
}

/*
cacheEntryIndex is a helper which reads an sstable index file into memory for
future lookups.
*/
func (r *Reader) cacheEntryIndex(ctx context.Context) error {
	if r.cache_entry_index && r.orig_in_idx != nil {
		var sk filesystem.Seeker
		var ir IndexRecord
		var err error
		var ok bool

		sk, ok = r.orig_in_idx.(filesystem.Seeker)

		r.entry_index_cache = make(map[string]int64)

		if r.idx_offset > 0 {
			if !ok {
				return Err_NotSeeker
			}

			// Go back to the beginning of the index.
			if err = sk.Seek(ctx, 0); err != nil {
				return err
			}
			r.idx_offset = 0
		}

		for {
			if err = ctx.Err(); err != nil {
				return err
			}

			err = r.in_idx.ReadMessage(ctx, &ir)
			if err != nil {
				break
			}
			if ok {
				var new_offset int64
				new_offset, err = sk.Tell(ctx)
				if err == nil {
					r.idx_offset = new_offset
				} else {
					r.idx_offset += int64(proto.Size(&ir))
				}
			} else {
				r.idx_offset += int64(proto.Size(&ir))
			}
			r.entry_index_cache[ir.Key] = ir.Offset
		}

		if err != io.EOF {
			return err
		}
	}

	return nil
}

/*
Tell tries its best to determine the readers current position in the input
stream.
*/
func (r *Reader) Tell(ctx context.Context) int64 {
	var sk filesystem.Seeker
	var ok bool

	sk, ok = r.orig_in.(filesystem.Seeker)
	if ok {
		var offset int64
		var err error
		// Ask seeker for the current position.
		offset, err = sk.Tell(ctx)
		if err != nil {
			r.offset = offset
		}
	}

	return r.offset
}

/*
SeekTo seeks the reader to the specified offset in the input stream.
If the input stream supports seeking, the underlying seek functionality is
used. Otherwise, seeking forward is emulated by reading and discarding
the right amount of data, and any attempt to seek backwards will lead to
an error being returned.
*/
func (r *Reader) SeekTo(ctx context.Context, offset int64) error {
	var err error
	var sk filesystem.Seeker
	var ok bool

	sk, ok = r.orig_in.(filesystem.Seeker)
	if ok {
		// Just tell the seeker to go to that position.
		err = sk.Seek(ctx, offset)
		if err != nil {
			r.offset = offset
		}
	} else {
		if r.offset > offset {
			return Err_NotSeeker
		}

		for r.offset < offset {
			var p []byte
			var l int = 1024

			if offset-r.offset < 1024 {
				l = int(offset - r.offset)
			}

			p = make([]byte, l)
			l, err = r.orig_in.Read(ctx, p)
			r.offset += int64(l)
			if err != nil {
				return err
			}
		}
	}

	return err
}

/*
ReadAllStrings reads all records from the specified sstable file into a byte
map and return that. Please note that this may use up a lot of resources,
since this will essentially read the entire file into memory.
*/
func (r *Reader) ReadAllStrings(ctx context.Context, rv map[string]string) (
	err error) {
	var rdata KeyValue

	for {
		err = r.in.ReadMessage(ctx, &rdata)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return
		}

		rv[rdata.Key] = rdata.Value
	}
}

/*
ReadAllProto reads all records from the sstable into a map of keys to protocol
buffers and return that. Please note that this may use up a lot of resources,
since this will essentially read the entire file into memory.
*/
func (r *Reader) ReadAllProto(ctx context.Context, pb proto.Message,
	rv map[string]proto.Message) (err error) {
	var rdata KeyValue

	for {
		var msg proto.Message

		err = r.in.ReadMessage(ctx, &rdata)
		if err == io.EOF {
			err = nil
			return
		}
		if err != nil {
			return
		}

		msg = proto.Clone(pb)
		err = proto.Unmarshal([]byte(rdata.Value), msg)

		rv[rdata.Key] = msg
	}
}

/*
indexLookup looks up the offset of the data record closest to the specified
key. The result is an offset which can be seeked to; it may however point to
the next key which is greater than the requested one.
*/
func (r *Reader) indexLookup(ctx context.Context, key string) (int64, error) {
	if r.cache_entry_index {
		var k, closest_k string
		var v, closest_v int64

		// Look up the key in the index.
		for k, v = range r.entry_index_cache {
			// Is the key we're looking for after the current key?
			if strings.Compare(key, k) > 0 {
				// Is it closer than the previous match?
				if strings.Compare(k, closest_k) > 0 {
					closest_k = k
					closest_v = v
				}
			} else if strings.Compare(key, k) == 0 {
				return v, nil
			}
		}

		return closest_v, nil
	} else if r.in_idx != nil {
		var closest_k string
		var closest_v int64

		// Read the on-disk index instead and locate the key in it.
		if r.idx_offset > 0 {
			var sk filesystem.Seeker
			var ok bool

			sk, ok = r.orig_in_idx.(filesystem.Seeker)
			if !ok {
				return 0, Err_NotSeeker
			}

			// Go back to the beginning of the index.
			sk.Seek(ctx, 0)
			r.idx_offset = 0
		}

		for {
			var ir IndexRecord
			var err error

			err = r.in_idx.ReadMessage(ctx, &ir)
			if err == io.EOF {
				return closest_v, nil
			}
			if err != nil {
				return closest_v, err
			}

			r.idx_offset += int64(proto.Size(&ir))

			// Is the key we're looking for after the current key?
			if strings.Compare(key, ir.Key) > 0 {
				// Is it closer than the previous match?
				if strings.Compare(ir.Key, closest_k) > 0 {
					closest_k = ir.Key
					closest_v = ir.Offset
				}
			} else if strings.Compare(key, ir.Key) == 0 {
				return ir.Offset, nil
			}
		}

		return closest_v, nil
	} else {
		return r.idx_offset, nil
	}
}

/*
ReadNextString finds the next record from the current position in the sstable
file and returns it, along with the corresponding key, as a string.
*/
func (r *Reader) ReadNextString(ctx context.Context) (string, string, error) {
	var rdata KeyValue
	var err error

	err = r.in.ReadMessage(ctx, &rdata)
	if err != nil {
		return "", "", err
	}

	return rdata.Key, rdata.Value, nil
}

/*
ReadNextProto finds the next record from the current position in the sstable
file and returns it, along with the corresponding key. The key will be
returned as a string while the record data will be emplaced into the specified
protocol buffer.
*/
func (r *Reader) ReadNextProto(ctx context.Context, pb proto.Message) (
	string, error) {
	var key string
	var val string
	var err error

	key, val, err = r.ReadNextString(ctx)
	if err != nil {
		return "", err
	}

	pb.Reset()

	// Fill the result into the specified protocol buffer.
	err = proto.Unmarshal([]byte(val), pb)
	return key, err
}

/*
ReadSubsequentString looks up and reads the first record following the
specified key. Return results as a string. The result can be a record with the
exact key specified, a larger key or an error in case no larger keys exist in
the sstable.
*/
func (r *Reader) ReadSubsequentString(ctx context.Context, key string) (
	string, string, error) {
	var rdata KeyValue
	var offset int64
	var err error

	// Determine the latest index record which suggests that searching
	// from it might be useful.
	offset, err = r.indexLookup(ctx, key)
	if err != nil {
		return "", "", err
	}

	// Now go to that point.
	err = r.SeekTo(ctx, offset)
	if err != nil {
		return "", "", err
	}

	for {
		var cv int

		err = r.in.ReadMessage(ctx, &rdata)
		if err == io.EOF {
			// End of file; record not found.
			return "", "", nil
		}
		if err != nil {
			return "", "", err
		}

		cv = strings.Compare(rdata.Key, key)
		if cv == 0 || cv > 0 {
			return rdata.Key, rdata.Value, nil
		}
	}
}

/*
ReadSubsequentProto looks up and reads the record specified by the given key.
It emplaces the result into the specified protocol buffer. Returns the key of
the row actually found.
*/
func (r *Reader) ReadSubsequentProto(
	ctx context.Context, key string, pb proto.Message) (string, error) {
	var val string
	var rkey string
	var err error

	rkey, val, err = r.ReadSubsequentString(ctx, key)
	if err != nil {
		return "", err
	}

	pb.Reset()

	// Fill the result into the specified protocol buffer.
	err = proto.Unmarshal([]byte(val), pb)
	return rkey, err
}

/*
ReadString looks up and reads the record specified by the given key. It then
returns the result as a string.
*/
func (r *Reader) ReadString(ctx context.Context, key string) (string, error) {
	var rdata KeyValue
	var offset int64
	var err error

	// Determine the latest index record which suggests that searching
	// from it might be useful.
	offset, err = r.indexLookup(ctx, key)
	if err != nil {
		return "", err
	}

	// Now go to that point.
	err = r.SeekTo(ctx, offset)
	if err != nil {
		return "", err
	}

	for {
		var cv int

		err = r.in.ReadMessage(ctx, &rdata)
		if err == io.EOF {
			// End of file; record not found.
			return "", nil
		}
		if err != nil {
			return "", err
		}

		cv = strings.Compare(rdata.Key, key)
		if cv == 0 {
			return rdata.Value, nil
		}

		if cv > 0 {
			// We're well past the record now and it wasn't found.
			return "", nil
		}
	}
}

/*
ReadProto looks up and reads the record specified by the given key. It then
emplaces the result into the specified protocol buffer.
*/
func (r *Reader) ReadProto(
	ctx context.Context, key string, pb proto.Message) error {
	var val string
	var err error

	val, err = r.ReadString(ctx, key)
	if err != nil {
		return err
	}

	pb.Reset()

	// Fill the result into the specified protocol buffer.
	err = proto.Unmarshal([]byte(val), pb)
	return err
}
