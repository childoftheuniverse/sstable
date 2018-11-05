package sstable

import (
	"github.com/childoftheuniverse/filesystem-internal"
	"golang.org/x/net/context"
	"math/rand"
	"sort"
	"testing"
)

var testdata map[string]string = map[string]string{
	"aaa":       "foo",
	"aab":       "bar",
	"abc":       "bla",
	"bipolar":   "why",
	"bac":       "fire",
	"bat":       "boo",
	"bit":       "yes",
	"boat":      "float",
	"cat":       "maw",
	"cut":       "ow",
	"cute":      "oh",
	"dude":      "bro",
	"ear":       "tear",
	"europa":    "do not go there",
	"excalibur": "wat",
	"flub":      "fump",
	"inferno":   "every single time",
	"jupiter":   "sulfuric sunsets are rubbish",
	"mars":      "syria planum",
	"mercury":   "home",
	"mmm":       "baz",
	"nasa":      "spacex",
	"neptune":   "underwater",
	"saturn":    "white star",
	"uranus":    "freezer",
	"venus":     "sunev",
	"pluto":     "too cute",
	"zqc":       "quux",
}

// Write single strings and attempt to read an individual one back.
func TestWriteAndReadSingleStringsNotIndexed(t *testing.T) {
	var ctx = context.Background()
	var buf = internal.NewAnonymousFile()
	var writer *Writer = NewWriter(ctx, buf)
	var reader *Reader
	var keys []string
	var k, v string
	var err error

	for k, _ = range testdata {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	// Fill the sstable with some test data.
	for _, k = range keys {
		v = testdata[k]
		err = writer.WriteString(ctx, k, v)
		if err != nil {
			t.Error("Error writing record ", k, ": ", err)
		}
	}

	// Reset position.
	buf.Close(ctx)

	// Now try reading it back and comparing.
	reader = NewReader(buf)
	v, err = reader.ReadString(ctx, "mmm")
	if err != nil {
		t.Error("Error reading record mmm: ", err)
	}
	if v != testdata["mmm"] {
		t.Error("Mismatched data: expected ", testdata["mmm"], ", got ", v)
	}

	// Reset position.
	buf.Close(ctx)

	// Try to read a nonexistent key.
	reader = NewReader(buf)
	v, err = reader.ReadString(ctx, "nonexistent")
	if err != nil {
		t.Error("Error reading nonexistent record: ", v)
	}
	if len(v) > 0 {
		t.Error("Reading nonexistent record returned ", v,
			", should be nothing")
	}

	// Reset position.
	buf.Close(ctx)

	// Try to read a nonexistent key.
	reader = NewReader(buf)
	k, v, err = reader.ReadSubsequentString(ctx, "maa")
	if err != nil {
		t.Error("Error reading first record after maa: ", v)
	}
	if k != "mars" {
		t.Error("Mismatched key: expected mars, got ", k)
	}
	if v != testdata["mars"] {
		t.Error("Mismatched data: expected ", testdata["mars"], ", got ", v)
	}
}

func TestWriteAndReadStringMapIndexedNotCached(t *testing.T) {
	var ctx = context.Background()
	var buf = internal.NewAnonymousFile()
	var idx = internal.NewAnonymousFile()
	var writer *Writer = NewIndexedWriter(ctx, buf, idx, IndexType_EVERY_N, 4)
	var reader *Reader
	var keys []string
	var k, v string
	var err error

	for k, _ = range testdata {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	// Fill the sstable with some test data.
	for _, k = range keys {
		v = testdata[k]
		err = writer.WriteString(ctx, k, v)
		if err != nil {
			t.Error("Error writing record ", k, ": ", err)
		}
	}

	// Reset position.
	buf.Close(ctx)

	// Now try reading it back and comparing.
	reader = NewReader(buf)
	v, err = reader.ReadString(ctx, "mmm")
	if err != nil {
		t.Error("Error reading record mmm: ", err)
	}
	if v != testdata["mmm"] {
		t.Error("Mismatched data: expected ", testdata["mmm"], ", got ", v)
	}

	// Reset position.
	buf.Close(ctx)

	// Try to read a nonexistent key.
	reader = NewReader(buf)
	k, v, err = reader.ReadSubsequentString(ctx, "maa")
	if err != nil {
		t.Error("Error reading first record after maa: ", v)
	}
	if k != "mars" {
		t.Error("Mismatched key: expected mars, got ", k)
	}
	if v != testdata["mars"] {
		t.Error("Mismatched data: expected ", testdata["mars"], ", got ", v)
	}
}

// Write collection strings without index and attempt to read all of them back.
func TestWriteAndReadStringMapNotIndexed(t *testing.T) {
	var ctx = context.Background()
	var result_map = make(map[string]string)
	var buf = internal.NewAnonymousFile()
	var writer = NewWriter(ctx, buf)
	var reader *Reader
	var k, v string
	var err error
	var ok bool

	// Fill the sstable with some test data.
	err = writer.WriteStringMap(ctx, testdata)
	if err != nil {
		t.Error("Error writing records: ", err)
	}

	// Reset position.
	buf.Close(ctx)

	// Now try reading it back and comparing.
	reader = NewReader(buf)
	err = reader.ReadAllStrings(ctx, result_map)
	if err != nil {
		t.Error("Error reading record ", k, ": ", err)
	}
	for k, _ = range testdata {
		v, ok = result_map[k]
		if !ok {
			t.Error("Value for ", k, " missing from result_map")
		} else if v != testdata[k] {
			t.Error("Value mismatch in result_map: ", k, " should be ", testdata[k],
				", is ", v)
		}
	}
	for k, _ = range result_map {
		_, ok = testdata[k]
		if !ok {
			t.Error("Extra map entry for ", k, " in result_map")
		}
	}
}

// Write test data in the wrong order and see if the library catches it.
func TestWriteKeyOrderViolation(t *testing.T) {
	var ctx = context.Background()
	var buf = internal.NewAnonymousFile()
	var writer = NewWriter(ctx, buf)
	var err error

	err = writer.WriteString(ctx, "test2", "data2")
	if err != nil {
		t.Errorf("Unexpected error writing test data to sstable: %s", err)
	}

	err = writer.WriteString(ctx, "test1", "data1")
	if err == nil {
		t.Error("WriteString in reverse order didn't fire, expected it to")
	}
	if err != Err_KeyOrderViolation {
		t.Errorf("Unexpected error type after key order violation: %s", err)
	}

	err = writer.WriteString(ctx, "test2", "data3")
	if err != nil {
		t.Errorf("Unexpected error writing second record to same key: %s", err)
	}
}

// Write collection strings without index and access them at random.
func BenchmarkIndexlessLookup(b *testing.B) {
	var ctx = context.Background()
	var buf = internal.NewAnonymousFile()
	var writer = NewWriter(ctx, buf)
	var reader *Reader
	var keys []string
	var k, v string
	var err error
	var i int

	for k, _ = range testdata {
		keys = append(keys, k)
	}

	// Fill the sstable with some test data.
	err = writer.WriteStringMap(ctx, testdata)
	if err != nil {
		b.Error("Error writing records: ", err)
	}

	b.StartTimer()

	for i = 0; i < b.N; i++ {
		// Reset position.
		buf.Close(ctx)

		reader = NewReader(buf)
		k = keys[rand.Intn(len(keys))]
		v, err = reader.ReadString(ctx, k)
		if err != nil {
			b.Error("Error reading record ", k, ": ", err)
		} else if v != testdata[k] {
			b.Error("Mismatched record data for ", k, ": expected ", testdata[k],
				", got ", v)
		}
	}

	b.StopTimer()
	b.ReportAllocs()
}

// Write collection strings with index and access them at random.
func BenchmarkIndexedNonCachedLookup(b *testing.B) {
	var ctx = context.Background()
	var buf = internal.NewAnonymousFile()
	var buf_idx = internal.NewAnonymousFile()
	var writer *Writer = NewIndexedWriter(ctx, buf, buf_idx, IndexType_EVERY_N, 4)
	var reader *Reader
	var keys []string
	var k, v string
	var err error
	var i int

	for k, _ = range testdata {
		keys = append(keys, k)
	}

	// Fill the sstable with some test data.
	err = writer.WriteStringMap(ctx, testdata)
	if err != nil {
		b.Error("Error writing records: ", err)
	}

	b.StartTimer()

	for i = 0; i < b.N; i++ {
		// Reset position.
		buf.Close(ctx)
		buf_idx.Close(ctx)

		reader, err = NewReaderWithIdx(ctx, buf, buf_idx, false)
		if err != nil {
			b.Error("Error creating indexed reader: ", err)
			continue
		}
		k = keys[rand.Intn(len(keys))]
		v, err = reader.ReadString(ctx, k)
		if err != nil {
			b.Error("Error reading record ", k, ": ", err)
		} else if v != testdata[k] {
			b.Error("Mismatched record data for ", k, ": expected ", testdata[k],
				", got ", v)
		}
	}

	b.StopTimer()
	b.ReportAllocs()
}
