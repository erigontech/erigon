package utils

import (
	"flag"
	"fmt"

	"github.com/c2h5oh/datasize"
	"github.com/urfave/cli/v2"
)

// Custom cli.Flag type which expand the received string to an absolute path.
// e.g. ~/.ethereum -> /home/username/.ethereum
type DatasizeFlag struct {
	Name string

	Category    string
	DefaultText string
	Usage       string

	Required   bool
	Hidden     bool
	HasBeenSet bool

	Value datasizeFlagValue

	Aliases []string
}

func (f *DatasizeFlag) Names() []string { return append([]string{f.Name}, f.Aliases...) }
func (f *DatasizeFlag) IsSet() bool     { return f.HasBeenSet }
func (f *DatasizeFlag) String() string  { return cli.FlagStringer(f) }

// called by cli library, grabs variable from environment (if in env)
// and adds variable to flag set for parsing.
func (f *DatasizeFlag) Apply(set *flag.FlagSet) error {
	eachName(f, func(name string) {
		set.Var(&f.Value, f.Name, f.Usage)
	})
	return nil
}

func (f *DatasizeFlag) IsRequired() bool { return f.Required }

func (f *DatasizeFlag) IsVisible() bool { return !f.Hidden }

func (f *DatasizeFlag) GetCategory() string { return f.Category }

func (f *DatasizeFlag) TakesValue() bool     { return true }
func (f *DatasizeFlag) GetUsage() string     { return f.Usage }
func (f *DatasizeFlag) GetValue() string     { return f.Value.String() }
func (f *DatasizeFlag) GetEnvVars() []string { return nil } // env not supported

func (f *DatasizeFlag) GetDefaultText() string {
	if f.DefaultText != "" {
		return f.DefaultText
	}
	return f.GetValue()
}

type datasizeFlagValue datasize.ByteSize

func (b *datasizeFlagValue) String() string {
	if b == nil {
		return ""
	}
	a := datasize.ByteSize(*b)
	return a.String()
}

func (b *datasizeFlagValue) Set(s string) error {
	val, err := datasize.ParseString(s)
	if err != nil {
		return fmt.Errorf("parse datasize: %v", err)
	}
	*b = datasizeFlagValue(val)
	return nil
}

// DatasizeFlagValue returns the value of a DatasizeFlag from the flag set.
func DatasizeFlagValue(ctx *cli.Context, name string) *datasize.ByteSize {
	val := ctx.Generic(name)
	if val == nil {
		return nil
	}
	return (*datasize.ByteSize)(val.(*datasizeFlagValue))
}
