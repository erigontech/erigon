func combineImports(pkgDir string) map[string]bool {

	combined := make(map[string]bool)

	for _, g := range gens {
		if g.pkgDir == pkgDir {
			for k := range g.imports {
				combined[k] = true
			}
		}
	}
	return combined
}

func combineContents(pkgDir string) []byte {
	var b []byte

	for _, g := range gens {
		if g.pkgDir == pkgDir {
			b = append(b, g.content...)
		}
	}
	return b
}

func writeToFile() {
	// outfile := fmt.Sprintf("%s/gen_%s_rlp.go", g.pkgDir, strings.ToLower(g.typename))
	// fmt.Println("outfile: ", outfile)
	// if err := os.WriteFile(outfile, content, 0600); err != nil {
	// 	_exit(err.Error())
	// }

	// TODO: create a single file for multiple types if they are in the same dir

	sameFiles := make(map[string]int) // count how many types located in the same dir

	for _, g := range gens {
		sameFiles[g.pkgDir] += 1
	}

	var b []string

	for _, g := range gens {
		if sameFiles[g.pkgDir] > 1 { // if couple of more types are in the same dir
			// save them for later usage (will give it a single file name)
			b = append(b, g.pkgDir)
		}
	}

	for len(b) > 0 {
		dir := b[0]
		for _, g := range gens {
			if g.pkgDir == dir {
				g.filename = "common" // TODO: should be different? e.g depends on the types?
			}
		}
		b = b[1:]
	}

	fmt.Println("---- Removing previously generated files ----")

	// remove all the files first
	for _, g := range gens {
		if err := os.Remove(g.filename); err != nil {
			_exit(err.Error())
		}
	}

	for _, g := range gens {
		f, err := os.OpenFile(g.filename, os.O_APPEND, 0600)
		if err != nil {
			_exit(err.Error())
		}

		if sameFiles[g.pkgDir] == -1 {
			continue
		}

		if sameFiles[g.pkgDir] > 1 {
			f.Write([]byte(headerMsg))
			f.Write([]byte("package " + g.named.Obj().Pkg().Name() + "\n\n"))
			f.Write(importsToBytes(combineImports(g.pkgDir)))
			f.Write(combineContents(g.pkgDir))
			f.Close()
			sameFiles[g.pkgDir] = -1
		} else if sameFiles[g.pkgDir] == 1 {
			f.Write([]byte(headerMsg))
			f.Write([]byte("package " + g.named.Obj().Pkg().Name() + "\n\n"))
			f.Write(importsToBytes(g.imports))
			f.Write(g.content)
			f.Close()
		} else {
			panic("writeToFile else case")
		}
	}
}

func importsToBytes(imports map[string]bool) []byte {
	imports["fmt"] = true
	imports["io"] = true
	imports[rlpPackagePath] = true

	var result []byte
	// result = append(result, []byte(headerMsg)...)
	// result = append(result, []byte("package "+pkgSrc.Name()+"\n\n")...)
	result = append(result, []byte("import (\n")...)
	for k := range imports {
		result = append(result, []byte("    ")...)
		result = append(result, '"')
		result = append(result, []byte(k)...)
		result = append(result, '"', '\n')
	}
	result = append(result, []byte(")\n\n")...)
	return result
}