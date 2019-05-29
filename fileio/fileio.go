package fileio

// similar to built in textio, however each file is output without
// modification instead of split into individual lines

import (
	"context"
	"io/ioutil"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

func init() {
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
