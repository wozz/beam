package fileio

// similar to built in textio, however each file is output without
// modification instead of split into individual lines

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*writeFileFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*writeFileFmtFn)(nil)).Elem())
	beam.RegisterFunction(readFn)
	beam.RegisterFunction(expandFn)
}

// Read reads a set of files and returns each file as a PCollection<string>.
func Read(s beam.Scope, glob string) beam.PCollection {
	s = s.Scope("fileio.Read")

	filesystem.ValidateScheme(glob)
	return read(s, beam.Create(s, glob))
}

func read(s beam.Scope, col beam.PCollection) beam.PCollection {
	files := beam.ParDo(s, expandFn, col)
	return beam.ParDo(s, readFn, files)
}

func expandFn(ctx context.Context, glob string, emit func(string)) error {
	if strings.TrimSpace(glob) == "" {
		return nil // ignore empty string elements here
	}

	fs, err := filesystem.New(ctx, glob)
	if err != nil {
		return err
	}
	defer fs.Close()

	files, err := fs.List(ctx, glob)
	if err != nil {
		return err
	}
	for _, filename := range files {
		emit(filename)
	}
	return nil
}

func readFn(ctx context.Context, filename string, emit func(string)) error {
	log.Infof(ctx, "Reading file %v", filename)

	fs, err := filesystem.New(ctx, filename)
	if err != nil {
		return err
	}
	defer fs.Close()

	fd, err := fs.OpenRead(ctx, filename)
	if err != nil {
		return err
	}
	defer fd.Close()

	fileBytes, err := ioutil.ReadAll(fd)
	if err != nil {
		log.Errorf(ctx, "could not read file: %v, %v", filename, err)
		return err
	}
	emit(string(fileBytes))
	return nil
}

// Write writes a PCollection<[]byte> to a file.
func Write(s beam.Scope, filename string, col beam.PCollection) {
	s = s.Scope("fileio.Write")

	filesystem.ValidateScheme(filename)

	pre := beam.AddFixedKey(s, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeFileFn{Filename: filename}, post)
}

type writeFileFn struct {
	Filename string `json:"filename"`
}

func (w *writeFileFn) ProcessElement(ctx context.Context, _ int, bytes func(*[]byte) bool) error {
	fs, err := filesystem.New(ctx, w.Filename)
	if err != nil {
		return err
	}
	defer fs.Close()

	fd, err := fs.OpenWrite(ctx, w.Filename)
	if err != nil {
		return err
	}
	buf := bufio.NewWriterSize(fd, 1<<20) // use 1MB buffer

	log.Infof(ctx, "Writing to %v", w.Filename)

	var b []byte
	for bytes(&b) {
		n, err := buf.Write(b)
		if err != nil {
			log.Errorf(ctx, "Could not write to file: %v", err)
			return err
		}
		if n != len(b) {
			log.Errorf(ctx, "did not write all bytes to buffer: wrote %d, expected %d", n, len(b))
		}
	}
	if err := buf.Flush(); err != nil {
		log.Errorf(ctx, "error flushing buffer: %v", err)
		return err
	}
	return fd.Close()
}

// WritePerKey writes a PCollection<KV<int, []byte>> to a set of files.
// Provide the filename format, which will be used with the int key to form each
// filename.
func WritePerKey(s beam.Scope, filenameFormat string, col beam.PCollection) {
	s = s.Scope("fileio.WritePerKey")

	filesystem.ValidateScheme(fmt.Sprintf(filenameFormat, 0))
	beam.ParDo0(s, &writeFileFmtFn{FilenameFormat: filenameFormat}, col)
}

type writeFileFmtFn struct {
	FilenameFormat string `json:"filenameFmt"`
}

func (w writeFileFmtFn) ProcessElement(ctx context.Context, i int, bytes []byte) error {
	filename := fmt.Sprintf(w.FilenameFormat, i)
	fs, err := filesystem.New(ctx, filename)
	if err != nil {
		return err
	}
	defer fs.Close()
	fd, err := fs.OpenWrite(ctx, filename)
	if err != nil {
		return err
	}
	buf := bufio.NewWriterSize(fd, 1<<20) // use 1MB buffer
	n, err := buf.Write(bytes)
	if err != nil {
		log.Errorf(ctx, "Cloud not write to file: %s %v", filename, err)
		return err
	}
	if n != len(bytes) {
		log.Errorf(ctx, "did not write all bytes to buffer: wrote %d, expected %d", n, len(bytes))
	}
	if err := buf.Flush(); err != nil {
		log.Errorf(ctx, "error flushing buffer: %v", err)
		return err
	}
	return fd.Close()
}
