package spectest

import (
	"fmt"
	"io/fs"

	"gopkg.in/yaml.v3"
)

func ReadMeta(root fs.FS, name string, obj any) error {
	bts, err := fs.ReadFile(root, name)
	if err != nil {
		return fmt.Errorf("couldnt read meta: %w", err)
	}
	err = yaml.Unmarshal(bts, obj)
	if err != nil {
		return fmt.Errorf("couldnt parse meta: %w", err)
	}
	return nil
}
