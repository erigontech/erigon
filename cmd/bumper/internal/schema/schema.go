package schema

import (
	"github.com/erigontech/erigon/db/snaptype"
	"gopkg.in/yaml.v3"
	"os"
	"sort"
)

type TwoVers struct {
	Current snaptype.Version `yaml:"current"`
	Min     snaptype.Version `yaml:"min"`
}

func (v TwoVers) MarshalYAML() (any, error) {
	n := &yaml.Node{Kind: yaml.MappingNode}
	n.Content = []*yaml.Node{
		{Kind: yaml.ScalarNode, Value: "current"},
		{Kind: yaml.ScalarNode, Tag: "!!str", Value: v.Current.String()},
		{Kind: yaml.ScalarNode, Value: "min"},
		{Kind: yaml.ScalarNode, Tag: "!!str", Value: v.Min.String()},
	}
	return n, nil
}

type Group map[string]TwoVers

type Category struct {
	Domain Group `yaml:"domain,omitempty"`
	Hist   Group `yaml:"hist,omitempty"`
	Ii     Group `yaml:"ii,omitempty"`
}

type Schema map[string]Category

func Load(path string) (Schema, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Schema
	if err := yaml.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return s, nil
}

func Save(path string, s Schema) error {
	b, err := yaml.Marshal(s)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func Cats(s Schema) []string {
	cs := make([]string, 0, len(s))
	for k := range s {
		cs = append(cs, k)
	}
	sort.Strings(cs)
	return cs
}
