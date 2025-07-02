package main

import (
	_ "embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/alexrjones/pgmodelparse/pgmodelparse"
	"github.com/davecgh/go-spew/spew"
	"github.com/rs/zerolog/log"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Usage: pgmodelparse <dir>")
		os.Exit(1)
	}

	var migrations []string
	err := filepath.WalkDir(os.Args[1], func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".sql") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		migrations = append(migrations, string(data))
		return nil
	})
	if err != nil {
		log.Fatal().Err(err).Send()
		return
	}

	compiler := pgmodelparse.NewCompiler()
	for _, mig := range migrations {
		err = compiler.ParseRaw(mig)
		if err != nil {
			log.Fatal().Err(err).Send()
			return
		}
	}
	spew.Dump(compiler.Catalog)
}
