//go:build !linux

package disk

func UpdatePrometheusDiskStats() error {
	return nil
}
