package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/erigontech/erigon/db/state"
	"github.com/spf13/cobra"
	"reflect"
	"strings"
)

var inspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "List all SchemaGen fields and their types",
	RunE: func(cmd *cobra.Command, args []string) error {
		fields := InspectSchemaFields(&state.Schema)
		data, err := json.MarshalIndent(fields, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	},
}

// FieldInfo holds name and kind of a schema field
type FieldInfo struct {
	Name string `json:"name"`
	Kind string `json:"kind"` // "domainCfg" or "iiCfg"
}

// InspectSchemaFields uses reflection to list SchemaGen fields and classify their types
func InspectSchemaFields(s *state.SchemaGen) []FieldInfo {
	return inspectSchemaFields(s)
}

func inspectSchemaFields(s *state.SchemaGen) []FieldInfo {
	var result []FieldInfo
	v := reflect.ValueOf(*s)
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		kind := field.Type.Name() // domainCfg, iiCfg, etc.
		result = append(result, FieldInfo{
			Name: field.Name,
			Kind: kind,
		})
	}
	return result
}

var (
	domainType = "domain"
	idxType    = "idx"
)

func parseName(name string) (string, string) {
	name = strings.ToLower(name)
	if strings.HasSuffix(name, domainType) {
		name, _ = strings.CutSuffix(name, domainType)
		return name, domainType
	}
	if strings.HasSuffix(name, idxType) {
		name, _ = strings.CutSuffix(name, idxType)
		return name, idxType
	}
	return name, ""
}

func getNames(s *state.SchemaGen) (res map[string]string, domains []string) {
	fields := inspectSchemaFields(s)
	res = make(map[string]string)
	for _, f := range fields {
		name, ftype := parseName(f.Name)
		res[name] = ftype
		domains = append(domains, name)
	}
	return res, domains
}

var extCfgMap = map[string][]string{
	domainType: {".kv", ".bt", ".kvi", ".kvei", ".vi", ".v"},
	idxType:    {".efi", ".ef"},
}
