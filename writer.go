package sstable

import (
	"errors"
	"github.com/childoftheuniverse/filesystem"
	"github.com/childoftheuniverse/recordio"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"sort"
	"strings"
)

const (
	IndexType_NONE = iota
	IndexType_EVERY_N
	IndexType_PREFIXLEN
)

/*
Err_UnsupportedIndexType is thrown when an unknown or unsupported IndexType
has been specified.
*/
var Err_UnsupportedIndexType = errors.New(
	"Unknown/unsupported IndexType")

/*
Err_KeyOrderViolation is thrown to indicate that the keys written to an
sstable file are not strictly ascending, which would mean that we're writing
an unsorted string table instead.
*/
var Err_KeyOrderViolation = errors.New(
	"Key order violation")

/*
Writer is a helper for writing structured data to a sorted string table file.

The code itself doesn't care a lot about whether the destination
is a file or something else.
*/
type Writer struct {
	out      *recordio.RecordWriter
	out_seek filesystem.Seeker
	out_idx  *recordio.RecordWriter

	index_type int
	index_n    int

	last_key string

	// index_offset points to the offset of the following record in the data file.
	index_offset      int64
	prev_index_ctr    int
	prev_index_prefix string
}

/*
NewWriter creates a new sstable writer around the supplied filesystem writer.
This does not assign an index writer, so no index will be written.
*/
func NewWriter(ctx context.Context, out filesystem.WriteCloser) *Writer {
	var writer = recordio.NewRecordWriter(out)
	var seeker filesystem.Seeker
	var offset int64
	var err error
	var ok bool

	seeker, ok = out.(filesystem.Seeker)
	if ok {
		// Make sure we're aware of our current position in the output stream.
		offset, err = seeker.Tell(ctx)
		if err != nil {
			offset = 0 // No clue; make sure this is zero'd out.
		}
	}

	return &Writer{
		out:        writer,
		out_seek:   seeker,
		index_type: IndexType_NONE,

		index_offset: offset,
	}
}

/*
NewIndexedWriter creates a new sstable writer around the supplied filesystem
writers; one of the writers is used for writing data, the other one will hold
an index.
*/
func NewIndexedWriter(ctx context.Context, out filesystem.WriteCloser,
	out_idx filesystem.WriteCloser, index_type int, n int) *Writer {
	var writer = recordio.NewRecordWriter(out)
	var seeker filesystem.Seeker
	var offset int64
	var err error
	var ok bool

	seeker, ok = out.(filesystem.Seeker)
	if ok {
		// Make sure we're aware of our current position in the output stream.
		offset, err = seeker.Tell(ctx)
		if err != nil {
			offset = 0 // No clue; make sure this is zero'd out.
		}
	}

	return &Writer{
		out:          writer,
		out_seek:     seeker,
		out_idx:      recordio.NewRecordWriter(out_idx),
		index_type:   index_type,
		index_n:      n,
		index_offset: offset,
	}
}

/*
WriteString creates a new sstable record with the specified key and value and
appends it to the end of the sstable file. If an index has been configured, it
will be updated with the record.

Write errors may indicate that the data has been written successfully to the
data file but not the index; it might be a complete failure too though.
*/
func (w *Writer) WriteString(ctx context.Context, key, value string) error {
	var rdata KeyValue
	var new_offset int64
	var record []byte
	var length int
	var err error

	if strings.Compare(w.last_key, key) > 0 {
		return Err_KeyOrderViolation
	}

	rdata.Key = key
	rdata.Value = value

	record, err = proto.Marshal(&rdata)
	if err != nil {
		return err
	}

	// Then, write out the actual data.
	length, err = w.out.Write(ctx, record)
	if err != nil {
		return err
	}
	w.last_key = key

	// Now, generate the index entry.
	if w.out_idx != nil && w.index_type != IndexType_NONE {
		switch w.index_type {
		case IndexType_PREFIXLEN:
			var prefix string
			if len(key) <= w.index_n {
				prefix = key
			} else {
				prefix = key[:w.index_n]
			}

			if prefix != w.prev_index_prefix {
				var ir IndexRecord
				var idxdata []byte

				ir.Key = prefix
				ir.Offset = w.index_offset

				idxdata, err = proto.Marshal(&ir)
				if err != nil {
					return err
				}
				_, err = w.out_idx.Write(ctx, idxdata)
				if err != nil {
					return err
				}

				w.prev_index_prefix = prefix
			}
			break
		case IndexType_EVERY_N:
			w.prev_index_ctr = (w.prev_index_ctr + 1) % w.index_n

			if w.prev_index_ctr == 0 {
				var ir IndexRecord
				var idxdata []byte

				ir.Key = key
				ir.Offset = w.index_offset

				idxdata, err = proto.Marshal(&ir)
				if err != nil {
					return err
				}
				_, err = w.out_idx.Write(ctx, idxdata)
				if err != nil {
					return err
				}

				w.prev_index_prefix = key
			}
			break
		default:
			return Err_UnsupportedIndexType
		}
	}

	if w.out_seek != nil {
		// Finally, update counters.
		new_offset, err = w.out_seek.Tell(ctx)
		if err == nil {
			// Underlying object supports seeks, just use the known current position in
			// the underlying file.
			w.index_offset = new_offset
		} else {
			// No seek support; we will just assume the position in the underlying data
			// store has advanced by the length of the data written. Please note this
			// can be wrong (e.g. when using compression).
			w.index_offset += int64(length)
		}
	}

	return nil
}

/*
WriteProto encodes the specified protocol buffer and appends it to the
sstable together with the specified key.
*/
func (w *Writer) WriteProto(
	ctx context.Context, key string, value proto.Message) error {
	var pbdata []byte
	var err error

	pbdata, err = proto.Marshal(value)
	if err != nil {
		return err
	}

	return w.WriteString(ctx, key, string(pbdata))
}

/*
WriteStringMap iterates over a map of strings, sorts them and adds them to an
sstable file.
*/
func (w *Writer) WriteStringMap(
	ctx context.Context, data map[string]string) error {
	var keys []string
	var key string

	for key, _ = range data {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key = range keys {
		var err error = w.WriteString(ctx, key, data[key])
		if err != nil {
			return err
		}
	}

	return nil
}

/*
WriteProtoMap iterates over a map associating strings as keys with protocol
buffers as values, sorts them and adds every record to the sstable file.
*/
func (w *Writer) WriteProtoMap(
	ctx context.Context, data map[string]proto.Message) error {
	var key string
	var value proto.Message

	for key, value = range data {
		var err error = w.WriteProto(ctx, key, value)
		if err != nil {
			return err
		}
	}

	return nil
}
