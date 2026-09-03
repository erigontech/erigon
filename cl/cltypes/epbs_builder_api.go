// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package cltypes

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"unicode/utf8"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/hexutil"
	commonssz "github.com/erigontech/erigon/common/ssz"
)

const (
	MaxBuilderAuthDataSize       = 4096
	MaxBuilderEntries            = 64
	MaxBuilderURLSize            = 2048
	MaxBuilderPubkeys            = 64
	MaxBuilderPreferencesEntries = 4096
)

type BuilderRequestAuth struct {
	Data hexutil.Bytes `json:"data"`
	Slot uint64        `json:"slot,string"`
}

func (b *BuilderRequestAuth) Static() bool { return false }

func (b *BuilderRequestAuth) EncodingSizeSSZ() int { return 4 + 8 + len(b.Data) }

func (b *BuilderRequestAuth) EncodeSSZ(dst []byte) ([]byte, error) {
	if err := validateBuilderAuthData(b.Data); err != nil {
		return nil, err
	}
	return ssz2.MarshalSSZ(dst, &rawByteList{value: &b.Data, limit: MaxBuilderAuthDataSize}, b.Slot)
}

func (b *BuilderRequestAuth) DecodeSSZ(buf []byte, version int) error {
	return b.DecodeSSZStrict(buf, version)
}

func (b *BuilderRequestAuth) DecodeSSZStrict(buf []byte, version int) error {
	if err := ssz2.UnmarshalSSZStrict(buf, version, &rawByteList{value: &b.Data, limit: MaxBuilderAuthDataSize}, &b.Slot); err != nil {
		return err
	}
	return validateBuilderAuthData(b.Data)
}

func (b *BuilderRequestAuth) Clone() clonable.Clonable {
	if b == nil {
		return &BuilderRequestAuth{}
	}
	return &BuilderRequestAuth{Data: bytes.Clone(b.Data), Slot: b.Slot}
}

func (b *BuilderRequestAuth) HashSSZ() ([32]byte, error) {
	if err := validateBuilderAuthData(b.Data); err != nil {
		return [32]byte{}, err
	}
	data := solid.NewByteListSSZ(MaxBuilderAuthDataSize)
	if err := data.SetBytes(b.Data); err != nil {
		return [32]byte{}, err
	}
	return merkle_tree.HashTreeRoot(data, b.Slot)
}

func (b *BuilderRequestAuth) UnmarshalJSON(data []byte) error {
	var value struct {
		Data *hexutil.Bytes `json:"data"`
		Slot *uint64        `json:"slot,string"`
	}
	if err := decodeStrictJSON(data, &value); err != nil {
		return err
	}
	if value.Data == nil || value.Slot == nil {
		return errors.New("builder request auth requires data and slot")
	}
	if err := validateBuilderAuthData(*value.Data); err != nil {
		return err
	}
	b.Data = bytes.Clone(*value.Data)
	b.Slot = *value.Slot
	return nil
}

func (b BuilderRequestAuth) MarshalJSON() ([]byte, error) {
	if err := validateBuilderAuthData(b.Data); err != nil {
		return nil, err
	}
	type builderRequestAuth BuilderRequestAuth
	return json.Marshal(builderRequestAuth(b))
}

type SignedBuilderRequestAuth struct {
	Message   *BuilderRequestAuth `json:"message"`
	Signature common.Bytes96      `json:"signature"`
}

func (s *SignedBuilderRequestAuth) Static() bool { return false }

func (s *SignedBuilderRequestAuth) EncodingSizeSSZ() int {
	if s.Message == nil {
		return 4 + len(s.Signature)
	}
	return 4 + len(s.Signature) + s.Message.EncodingSizeSSZ()
}

func (s *SignedBuilderRequestAuth) EncodeSSZ(dst []byte) ([]byte, error) {
	if s.Message == nil {
		return nil, errors.New("signed builder request auth has nil message")
	}
	return ssz2.MarshalSSZ(dst, s.Message, s.Signature[:])
}

func (s *SignedBuilderRequestAuth) DecodeSSZ(buf []byte, version int) error {
	return s.DecodeSSZStrict(buf, version)
}

func (s *SignedBuilderRequestAuth) DecodeSSZStrict(buf []byte, version int) error {
	s.Message = new(BuilderRequestAuth)
	return ssz2.UnmarshalSSZStrict(buf, version, s.Message, s.Signature[:])
}

func (s *SignedBuilderRequestAuth) Clone() clonable.Clonable {
	if s == nil {
		return &SignedBuilderRequestAuth{}
	}
	var message *BuilderRequestAuth
	if s.Message != nil {
		message = s.Message.Clone().(*BuilderRequestAuth)
	}
	return &SignedBuilderRequestAuth{Message: message, Signature: s.Signature}
}

func (s *SignedBuilderRequestAuth) HashSSZ() ([32]byte, error) {
	if s.Message == nil {
		return [32]byte{}, errors.New("signed builder request auth has nil message")
	}
	return merkle_tree.HashTreeRoot(s.Message, s.Signature[:])
}

func (s *SignedBuilderRequestAuth) UnmarshalJSON(data []byte) error {
	var value struct {
		Message   *BuilderRequestAuth `json:"message"`
		Signature *common.Bytes96     `json:"signature"`
	}
	if err := decodeStrictJSON(data, &value); err != nil {
		return err
	}
	if value.Message == nil || value.Signature == nil {
		return errors.New("signed builder request auth requires message and signature")
	}
	s.Message = value.Message
	s.Signature = *value.Signature
	return nil
}

func (s SignedBuilderRequestAuth) MarshalJSON() ([]byte, error) {
	if s.Message == nil {
		return nil, errors.New("signed builder request auth has nil message")
	}
	if err := validateBuilderAuthData(s.Message.Data); err != nil {
		return nil, err
	}
	type signedBuilderRequestAuth SignedBuilderRequestAuth
	return json.Marshal(signedBuilderRequestAuth(s))
}

type BuilderEntry struct {
	URL                 string                    `json:"url"`
	Auth                *SignedBuilderRequestAuth `json:"auth"`
	BuilderPubkeys      []common.Bytes48          `json:"builder_pubkeys"`
	MaxExecutionPayment uint64                    `json:"max_execution_payment,string"`
	MinBid              uint64                    `json:"min_bid,string"`
	BuilderBoostFactor  uint64                    `json:"builder_boost_factor,string"`
}

func (b *BuilderEntry) Static() bool { return false }

func (b *BuilderEntry) EncodingSizeSSZ() int {
	size := 4 + 4 + 4 + 8 + 8 + 8 + len(b.URL) + len(b.BuilderPubkeys)*len(common.Bytes48{})
	if b.Auth != nil {
		size += b.Auth.EncodingSizeSSZ()
	}
	return size
}

func (b *BuilderEntry) EncodeSSZ(dst []byte) ([]byte, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	urlBytes := hexutil.Bytes(b.URL)
	return ssz2.MarshalSSZ(dst,
		&rawByteList{value: &urlBytes, limit: MaxBuilderURLSize},
		b.Auth,
		&builderPubkeyList{values: &b.BuilderPubkeys},
		b.MaxExecutionPayment,
		b.MinBid,
		b.BuilderBoostFactor,
	)
}

func (b *BuilderEntry) DecodeSSZ(buf []byte, version int) error {
	return b.DecodeSSZStrict(buf, version)
}

func (b *BuilderEntry) DecodeSSZStrict(buf []byte, version int) error {
	var urlBytes hexutil.Bytes
	b.Auth = new(SignedBuilderRequestAuth)
	b.BuilderPubkeys = nil
	if err := ssz2.UnmarshalSSZStrict(buf, version,
		&rawByteList{value: &urlBytes, limit: MaxBuilderURLSize},
		b.Auth,
		&builderPubkeyList{values: &b.BuilderPubkeys},
		&b.MaxExecutionPayment,
		&b.MinBid,
		&b.BuilderBoostFactor,
	); err != nil {
		return err
	}
	b.URL = string(urlBytes)
	return b.validate()
}

func (b *BuilderEntry) Clone() clonable.Clonable {
	if b == nil {
		return &BuilderEntry{}
	}
	var auth *SignedBuilderRequestAuth
	if b.Auth != nil {
		auth = b.Auth.Clone().(*SignedBuilderRequestAuth)
	}
	return &BuilderEntry{
		URL: b.URL, Auth: auth, BuilderPubkeys: append([]common.Bytes48(nil), b.BuilderPubkeys...),
		MaxExecutionPayment: b.MaxExecutionPayment, MinBid: b.MinBid, BuilderBoostFactor: b.BuilderBoostFactor,
	}
}

func (b *BuilderEntry) UnmarshalJSON(data []byte) error {
	var value struct {
		URL                 *string                   `json:"url"`
		Auth                *SignedBuilderRequestAuth `json:"auth"`
		BuilderPubkeys      *[]common.Bytes48         `json:"builder_pubkeys"`
		MaxExecutionPayment *uint64                   `json:"max_execution_payment,string"`
		MinBid              *uint64                   `json:"min_bid,string"`
		BuilderBoostFactor  *uint64                   `json:"builder_boost_factor,string"`
	}
	if err := decodeStrictJSON(data, &value); err != nil {
		return err
	}
	if value.URL == nil || value.Auth == nil || value.BuilderPubkeys == nil || value.MaxExecutionPayment == nil || value.MinBid == nil || value.BuilderBoostFactor == nil {
		return errors.New("builder entry is missing a required field")
	}
	*b = BuilderEntry{URL: *value.URL, Auth: value.Auth, BuilderPubkeys: *value.BuilderPubkeys, MaxExecutionPayment: *value.MaxExecutionPayment, MinBid: *value.MinBid, BuilderBoostFactor: *value.BuilderBoostFactor}
	return b.validate()
}

func (b BuilderEntry) MarshalJSON() ([]byte, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	type builderEntry BuilderEntry
	return json.Marshal(builderEntry(b))
}

func (b *BuilderEntry) validate() error {
	if err := validateBuilderURL(b.URL); err != nil {
		return err
	}
	if b.Auth == nil || b.Auth.Message == nil {
		return errors.New("builder entry has nil auth")
	}
	if len(b.BuilderPubkeys) > MaxBuilderPubkeys {
		return fmt.Errorf("builder pubkey count %d exceeds %d", len(b.BuilderPubkeys), MaxBuilderPubkeys)
	}
	return validateBuilderAuthData(b.Auth.Message.Data)
}

func (b *BuilderEntry) Validate() error {
	return b.validate()
}

type BuilderConfig struct {
	MinBid             uint64          `json:"min_bid,string"`
	BuilderBoostFactor uint64          `json:"builder_boost_factor,string"`
	Builders           []*BuilderEntry `json:"builders"`
}

func (b *BuilderConfig) Static() bool { return false }

func (b *BuilderConfig) EncodingSizeSSZ() int {
	return 8 + 8 + 4 + (&builderEntryList{values: &b.Builders}).EncodingSizeSSZ()
}

func (b *BuilderConfig) EncodeSSZ(dst []byte) ([]byte, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	return ssz2.MarshalSSZ(dst, b.MinBid, b.BuilderBoostFactor, &builderEntryList{values: &b.Builders})
}

func (b *BuilderConfig) DecodeSSZ(buf []byte, version int) error {
	return b.DecodeSSZStrict(buf, version)
}

func (b *BuilderConfig) DecodeSSZStrict(buf []byte, version int) error {
	b.Builders = nil
	if err := ssz2.UnmarshalSSZStrict(buf, version, &b.MinBid, &b.BuilderBoostFactor, &builderEntryList{values: &b.Builders}); err != nil {
		return err
	}
	return b.validate()
}

func (b *BuilderConfig) DecodeSSZStrictStructural(buf []byte, version int) error {
	b.Builders = nil
	rawEntries := make([]*rawBuilderEntry, 0)
	if err := ssz2.UnmarshalSSZStrict(buf, version, &b.MinBid, &b.BuilderBoostFactor, &rawBuilderEntryList{values: &rawEntries}); err != nil {
		return err
	}
	b.Builders = make([]*BuilderEntry, len(rawEntries))
	for i, raw := range rawEntries {
		b.Builders[i] = raw.entry
	}
	return nil
}

func (b *BuilderConfig) Clone() clonable.Clonable {
	clone := &BuilderConfig{MinBid: b.MinBid, BuilderBoostFactor: b.BuilderBoostFactor, Builders: make([]*BuilderEntry, len(b.Builders))}
	for i, entry := range b.Builders {
		if entry != nil {
			clone.Builders[i] = entry.Clone().(*BuilderEntry)
		}
	}
	return clone
}

func (b *BuilderConfig) UnmarshalJSON(data []byte) error {
	var value struct {
		MinBid             *uint64          `json:"min_bid,string"`
		BuilderBoostFactor *uint64          `json:"builder_boost_factor,string"`
		Builders           *[]*BuilderEntry `json:"builders"`
	}
	if err := decodeStrictJSON(data, &value); err != nil {
		return err
	}
	if value.MinBid == nil || value.BuilderBoostFactor == nil || value.Builders == nil {
		return errors.New("builder config is missing a required field")
	}
	if len(*value.Builders) > MaxBuilderEntries {
		return fmt.Errorf("builder count %d exceeds %d", len(*value.Builders), MaxBuilderEntries)
	}
	for i, entry := range *value.Builders {
		if entry == nil {
			return fmt.Errorf("builder %d is nil", i)
		}
	}
	*b = BuilderConfig{MinBid: *value.MinBid, BuilderBoostFactor: *value.BuilderBoostFactor, Builders: *value.Builders}
	return b.validate()
}

func (b BuilderConfig) MarshalJSON() ([]byte, error) {
	if b.Builders == nil {
		return nil, errors.New("builder config has nil builders")
	}
	if err := b.validate(); err != nil {
		return nil, err
	}
	type builderConfig BuilderConfig
	return json.Marshal(builderConfig(b))
}

func (b *BuilderConfig) validate() error {
	if len(b.Builders) > MaxBuilderEntries {
		return fmt.Errorf("builder count %d exceeds %d", len(b.Builders), MaxBuilderEntries)
	}
	seen := make(map[string]struct{}, len(b.Builders))
	for i, entry := range b.Builders {
		if entry == nil {
			return fmt.Errorf("builder %d is nil", i)
		}
		if err := entry.validate(); err != nil {
			return fmt.Errorf("builder %d: %w", i, err)
		}
		encoded, err := entry.EncodeSSZ(nil)
		if err != nil {
			return fmt.Errorf("builder %d: %w", i, err)
		}
		key := string(encoded)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("builder %d duplicates an earlier entry", i)
		}
		seen[key] = struct{}{}
	}
	return nil
}

type BuilderPreferencesEntry struct {
	ProposerPubkey      common.Bytes48            `json:"proposer_pubkey"`
	URL                 string                    `json:"url"`
	Auth                *SignedBuilderRequestAuth `json:"auth"`
	MaxExecutionPayment uint64                    `json:"max_execution_payment,string"`
}

type BuilderPreferencesEntries []*BuilderPreferencesEntry

func (b BuilderPreferencesEntries) Static() bool { return false }

func (b BuilderPreferencesEntries) EncodingSizeSSZ() int {
	size := len(b) * 4
	for _, entry := range b {
		if entry != nil {
			size += entry.EncodingSizeSSZ()
		}
	}
	return size
}

func (b BuilderPreferencesEntries) EncodeSSZ(dst []byte) ([]byte, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	return commonssz.EncodeDynamicList(dst, b)
}

func (b *BuilderPreferencesEntries) DecodeSSZ(buf []byte, version int) error {
	return b.DecodeSSZStrict(buf, version)
}

func (b *BuilderPreferencesEntries) DecodeSSZStrict(buf []byte, version int) error {
	entries, err := commonssz.DecodeDynamicListStrict[*BuilderPreferencesEntry](buf, 0, uint32(len(buf)), MaxBuilderPreferencesEntries, version)
	if err != nil {
		return err
	}
	*b = entries
	return b.validate()
}

func (b BuilderPreferencesEntries) Clone() clonable.Clonable {
	clone := make(BuilderPreferencesEntries, len(b))
	for i, entry := range b {
		if entry != nil {
			clone[i] = entry.Clone().(*BuilderPreferencesEntry)
		}
	}
	return &clone
}

func (b BuilderPreferencesEntries) MarshalJSON() ([]byte, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	if b == nil {
		return []byte("[]"), nil
	}
	type builderPreferencesEntries BuilderPreferencesEntries
	return json.Marshal(builderPreferencesEntries(b))
}

func (b *BuilderPreferencesEntries) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errors.New("builder preferences entries cannot be null")
	}
	type builderPreferencesEntries BuilderPreferencesEntries
	var entries builderPreferencesEntries
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}
	*b = BuilderPreferencesEntries(entries)
	return b.validate()
}

func (b BuilderPreferencesEntries) validate() error {
	if len(b) > MaxBuilderPreferencesEntries {
		return fmt.Errorf("builder preferences entry count %d exceeds %d", len(b), MaxBuilderPreferencesEntries)
	}
	for i, entry := range b {
		if entry == nil {
			return fmt.Errorf("builder preferences entry %d is nil", i)
		}
		if err := entry.validate(); err != nil {
			return fmt.Errorf("builder preferences entry %d: %w", i, err)
		}
	}
	return nil
}

func (b *BuilderPreferencesEntry) Static() bool { return false }

func (b *BuilderPreferencesEntry) EncodingSizeSSZ() int {
	size := len(b.ProposerPubkey) + 4 + 4 + 8 + len(b.URL)
	if b.Auth != nil {
		size += b.Auth.EncodingSizeSSZ()
	}
	return size
}

func (b *BuilderPreferencesEntry) EncodeSSZ(dst []byte) ([]byte, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	urlBytes := hexutil.Bytes(b.URL)
	return ssz2.MarshalSSZ(dst, b.ProposerPubkey[:], &rawByteList{value: &urlBytes, limit: MaxBuilderURLSize}, b.Auth, b.MaxExecutionPayment)
}

func (b *BuilderPreferencesEntry) DecodeSSZ(buf []byte, version int) error {
	return b.DecodeSSZStrict(buf, version)
}

func (b *BuilderPreferencesEntry) DecodeSSZStrict(buf []byte, version int) error {
	var urlBytes hexutil.Bytes
	b.Auth = new(SignedBuilderRequestAuth)
	if err := ssz2.UnmarshalSSZStrict(buf, version, b.ProposerPubkey[:], &rawByteList{value: &urlBytes, limit: MaxBuilderURLSize}, b.Auth, &b.MaxExecutionPayment); err != nil {
		return err
	}
	b.URL = string(urlBytes)
	return b.validate()
}

func (b *BuilderPreferencesEntry) Clone() clonable.Clonable {
	if b == nil {
		return &BuilderPreferencesEntry{}
	}
	var auth *SignedBuilderRequestAuth
	if b.Auth != nil {
		auth = b.Auth.Clone().(*SignedBuilderRequestAuth)
	}
	return &BuilderPreferencesEntry{ProposerPubkey: b.ProposerPubkey, URL: b.URL, Auth: auth, MaxExecutionPayment: b.MaxExecutionPayment}
}

func (b *BuilderPreferencesEntry) UnmarshalJSON(data []byte) error {
	var value struct {
		ProposerPubkey      *common.Bytes48           `json:"proposer_pubkey"`
		URL                 *string                   `json:"url"`
		Auth                *SignedBuilderRequestAuth `json:"auth"`
		MaxExecutionPayment *uint64                   `json:"max_execution_payment,string"`
	}
	if err := decodeStrictJSON(data, &value); err != nil {
		return err
	}
	if value.ProposerPubkey == nil || value.URL == nil || value.Auth == nil || value.MaxExecutionPayment == nil {
		return errors.New("builder preferences entry is missing a required field")
	}
	*b = BuilderPreferencesEntry{ProposerPubkey: *value.ProposerPubkey, URL: *value.URL, Auth: value.Auth, MaxExecutionPayment: *value.MaxExecutionPayment}
	return b.validate()
}

func (b BuilderPreferencesEntry) MarshalJSON() ([]byte, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	type builderPreferencesEntry BuilderPreferencesEntry
	return json.Marshal(builderPreferencesEntry(b))
}

func (b *BuilderPreferencesEntry) validate() error {
	if err := validateBuilderURL(b.URL); err != nil {
		return err
	}
	if b.Auth == nil || b.Auth.Message == nil {
		return errors.New("builder preferences entry has nil auth")
	}
	return validateBuilderAuthData(b.Auth.Message.Data)
}

type BuilderPreferences struct {
	MaxExecutionPayment uint64 `json:"max_execution_payment,string"`
}

func (b *BuilderPreferences) Static() bool         { return true }
func (b *BuilderPreferences) EncodingSizeSSZ() int { return 8 }
func (b *BuilderPreferences) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, b.MaxExecutionPayment)
}
func (b *BuilderPreferences) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZStrict(buf, version, &b.MaxExecutionPayment)
}
func (b *BuilderPreferences) DecodeSSZStrict(buf []byte, version int) error {
	return b.DecodeSSZ(buf, version)
}
func (b *BuilderPreferences) Clone() clonable.Clonable {
	return &BuilderPreferences{MaxExecutionPayment: b.MaxExecutionPayment}
}

func (b *BuilderPreferences) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.MaxExecutionPayment)
}

func (b *BuilderPreferences) UnmarshalJSON(data []byte) error {
	var value struct {
		MaxExecutionPayment *uint64 `json:"max_execution_payment,string"`
	}
	if err := decodeStrictJSON(data, &value); err != nil {
		return err
	}
	if value.MaxExecutionPayment == nil {
		return errors.New("builder preferences requires max_execution_payment")
	}
	b.MaxExecutionPayment = *value.MaxExecutionPayment
	return nil
}

type BuilderPreferencesRequest struct {
	Preferences *BuilderPreferences       `json:"preferences"`
	Auth        *SignedBuilderRequestAuth `json:"auth"`
}

func (b *BuilderPreferencesRequest) Static() bool { return false }
func (b *BuilderPreferencesRequest) EncodingSizeSSZ() int {
	size := 8 + 4
	if b.Auth != nil {
		size += b.Auth.EncodingSizeSSZ()
	}
	return size
}
func (b *BuilderPreferencesRequest) EncodeSSZ(dst []byte) ([]byte, error) {
	if b.Preferences == nil || b.Auth == nil || b.Auth.Message == nil {
		return nil, errors.New("builder preferences request has nil field")
	}
	return ssz2.MarshalSSZ(dst, b.Preferences, b.Auth)
}
func (b *BuilderPreferencesRequest) DecodeSSZ(buf []byte, version int) error {
	return b.DecodeSSZStrict(buf, version)
}
func (b *BuilderPreferencesRequest) DecodeSSZStrict(buf []byte, version int) error {
	b.Preferences = new(BuilderPreferences)
	b.Auth = new(SignedBuilderRequestAuth)
	return ssz2.UnmarshalSSZStrict(buf, version, b.Preferences, b.Auth)
}
func (b *BuilderPreferencesRequest) Clone() clonable.Clonable {
	clone := &BuilderPreferencesRequest{}
	if b.Preferences != nil {
		clone.Preferences = b.Preferences.Clone().(*BuilderPreferences)
	}
	if b.Auth != nil {
		clone.Auth = b.Auth.Clone().(*SignedBuilderRequestAuth)
	}
	return clone
}

func (b *BuilderPreferencesRequest) HashSSZ() ([32]byte, error) {
	if b.Preferences == nil || b.Auth == nil {
		return [32]byte{}, errors.New("builder preferences request has nil field")
	}
	return merkle_tree.HashTreeRoot(b.Preferences, b.Auth)
}
func (b *BuilderPreferencesRequest) UnmarshalJSON(data []byte) error {
	var value struct {
		Preferences *BuilderPreferences       `json:"preferences"`
		Auth        *SignedBuilderRequestAuth `json:"auth"`
	}
	if err := decodeStrictJSON(data, &value); err != nil {
		return err
	}
	if value.Preferences == nil || value.Auth == nil {
		return errors.New("builder preferences request is missing a required field")
	}
	b.Preferences, b.Auth = value.Preferences, value.Auth
	return nil
}

func (b BuilderPreferencesRequest) MarshalJSON() ([]byte, error) {
	if b.Preferences == nil || b.Auth == nil || b.Auth.Message == nil {
		return nil, errors.New("builder preferences request has nil field")
	}
	type builderPreferencesRequest BuilderPreferencesRequest
	return json.Marshal(builderPreferencesRequest(b))
}

type rawByteList struct {
	value *hexutil.Bytes
	limit int
}

func (r *rawByteList) Static() bool         { return false }
func (r *rawByteList) EncodingSizeSSZ() int { return len(*r.value) }
func (r *rawByteList) EncodeSSZ(dst []byte) ([]byte, error) {
	if len(*r.value) > r.limit {
		return nil, fmt.Errorf("byte list length %d exceeds %d", len(*r.value), r.limit)
	}
	return append(dst, (*r.value)...), nil
}
func (r *rawByteList) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) > r.limit {
		return fmt.Errorf("byte list length %d exceeds %d", len(buf), r.limit)
	}
	*r.value = bytes.Clone(buf)
	return nil
}
func (r *rawByteList) DecodeSSZStrict(buf []byte, version int) error {
	return r.DecodeSSZ(buf, version)
}
func (r *rawByteList) Clone() clonable.Clonable {
	value := hexutil.Bytes(nil)
	return &rawByteList{value: &value, limit: r.limit}
}

type builderPubkeyList struct{ values *[]common.Bytes48 }

func (l *builderPubkeyList) Static() bool         { return false }
func (l *builderPubkeyList) EncodingSizeSSZ() int { return len(*l.values) * len(common.Bytes48{}) }
func (l *builderPubkeyList) EncodeSSZ(dst []byte) ([]byte, error) {
	if len(*l.values) > MaxBuilderPubkeys {
		return nil, fmt.Errorf("builder pubkey count %d exceeds %d", len(*l.values), MaxBuilderPubkeys)
	}
	for i := range *l.values {
		dst = append(dst, (*l.values)[i][:]...)
	}
	return dst, nil
}
func (l *builderPubkeyList) DecodeSSZ(buf []byte, _ int) error {
	if len(buf)%len(common.Bytes48{}) != 0 {
		return commonssz.ErrLowBufferSize
	}
	count := len(buf) / len(common.Bytes48{})
	if count > MaxBuilderPubkeys {
		return fmt.Errorf("builder pubkey count %d exceeds %d", count, MaxBuilderPubkeys)
	}
	*l.values = make([]common.Bytes48, count)
	for i := range *l.values {
		copy((*l.values)[i][:], buf[i*len(common.Bytes48{}):])
	}
	return nil
}
func (l *builderPubkeyList) DecodeSSZStrict(buf []byte, version int) error {
	return l.DecodeSSZ(buf, version)
}
func (l *builderPubkeyList) Clone() clonable.Clonable {
	values := []common.Bytes48(nil)
	return &builderPubkeyList{values: &values}
}

type builderEntryList struct{ values *[]*BuilderEntry }

func (l *builderEntryList) Static() bool { return false }
func (l *builderEntryList) EncodingSizeSSZ() int {
	size := len(*l.values) * 4
	for _, entry := range *l.values {
		if entry != nil {
			size += entry.EncodingSizeSSZ()
		}
	}
	return size
}

type rawBuilderRequestAuth struct {
	data hexutil.Bytes
	slot uint64
}

func (r *rawBuilderRequestAuth) Static() bool { return false }
func (r *rawBuilderRequestAuth) EncodingSizeSSZ() int {
	return 4 + 8 + len(r.data)
}
func (r *rawBuilderRequestAuth) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, &rawByteList{value: &r.data, limit: MaxBuilderAuthDataSize}, r.slot)
}
func (r *rawBuilderRequestAuth) DecodeSSZ(buf []byte, version int) error {
	return r.DecodeSSZStrict(buf, version)
}
func (r *rawBuilderRequestAuth) DecodeSSZStrict(buf []byte, version int) error {
	return ssz2.UnmarshalSSZStrict(buf, version, &rawByteList{value: &r.data, limit: MaxBuilderAuthDataSize}, &r.slot)
}
func (r *rawBuilderRequestAuth) Clone() clonable.Clonable { return new(rawBuilderRequestAuth) }

type rawSignedBuilderRequestAuth struct {
	message   *rawBuilderRequestAuth
	signature common.Bytes96
}

func (r *rawSignedBuilderRequestAuth) Static() bool { return false }
func (r *rawSignedBuilderRequestAuth) EncodingSizeSSZ() int {
	if r.message == nil {
		return 4 + len(r.signature)
	}
	return 4 + len(r.signature) + r.message.EncodingSizeSSZ()
}
func (r *rawSignedBuilderRequestAuth) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, r.message, r.signature[:])
}
func (r *rawSignedBuilderRequestAuth) DecodeSSZ(buf []byte, version int) error {
	return r.DecodeSSZStrict(buf, version)
}
func (r *rawSignedBuilderRequestAuth) DecodeSSZStrict(buf []byte, version int) error {
	r.message = new(rawBuilderRequestAuth)
	return ssz2.UnmarshalSSZStrict(buf, version, r.message, r.signature[:])
}
func (r *rawSignedBuilderRequestAuth) Clone() clonable.Clonable {
	return new(rawSignedBuilderRequestAuth)
}

type rawBuilderEntry struct{ entry *BuilderEntry }

func (r *rawBuilderEntry) Static() bool { return false }
func (r *rawBuilderEntry) EncodingSizeSSZ() int {
	if r.entry == nil {
		return 0
	}
	return r.entry.EncodingSizeSSZ()
}
func (r *rawBuilderEntry) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.entry == nil || r.entry.Auth == nil || r.entry.Auth.Message == nil {
		return nil, errors.New("raw builder entry has nil auth")
	}
	urlBytes := hexutil.Bytes(r.entry.URL)
	auth := &rawSignedBuilderRequestAuth{
		message:   &rawBuilderRequestAuth{data: r.entry.Auth.Message.Data, slot: r.entry.Auth.Message.Slot},
		signature: r.entry.Auth.Signature,
	}
	return ssz2.MarshalSSZ(dst,
		&rawByteList{value: &urlBytes, limit: MaxBuilderURLSize},
		auth,
		&builderPubkeyList{values: &r.entry.BuilderPubkeys},
		r.entry.MaxExecutionPayment,
		r.entry.MinBid,
		r.entry.BuilderBoostFactor,
	)
}
func (r *rawBuilderEntry) DecodeSSZ(buf []byte, version int) error {
	return r.DecodeSSZStrict(buf, version)
}
func (r *rawBuilderEntry) DecodeSSZStrict(buf []byte, version int) error {
	var urlBytes hexutil.Bytes
	var auth rawSignedBuilderRequestAuth
	var pubkeys []common.Bytes48
	entry := new(BuilderEntry)
	if err := ssz2.UnmarshalSSZStrict(buf, version,
		&rawByteList{value: &urlBytes, limit: MaxBuilderURLSize},
		&auth,
		&builderPubkeyList{values: &pubkeys},
		&entry.MaxExecutionPayment,
		&entry.MinBid,
		&entry.BuilderBoostFactor,
	); err != nil {
		return err
	}
	entry.URL = string(urlBytes)
	entry.BuilderPubkeys = pubkeys
	entry.Auth = &SignedBuilderRequestAuth{
		Message:   &BuilderRequestAuth{Data: auth.message.data, Slot: auth.message.slot},
		Signature: auth.signature,
	}
	r.entry = entry
	return nil
}
func (r *rawBuilderEntry) Clone() clonable.Clonable { return new(rawBuilderEntry) }

type rawBuilderEntryList struct{ values *[]*rawBuilderEntry }

func (l *rawBuilderEntryList) Static() bool { return false }
func (l *rawBuilderEntryList) EncodingSizeSSZ() int {
	size := len(*l.values) * 4
	for _, entry := range *l.values {
		size += entry.EncodingSizeSSZ()
	}
	return size
}
func (l *rawBuilderEntryList) EncodeSSZ(dst []byte) ([]byte, error) {
	return commonssz.EncodeDynamicList(dst, *l.values)
}
func (l *rawBuilderEntryList) DecodeSSZ(buf []byte, version int) error {
	return l.DecodeSSZStrict(buf, version)
}
func (l *rawBuilderEntryList) DecodeSSZStrict(buf []byte, version int) error {
	values, err := commonssz.DecodeDynamicListStrict[*rawBuilderEntry](buf, 0, uint32(len(buf)), MaxBuilderEntries, version)
	if err != nil {
		return err
	}
	*l.values = values
	return nil
}
func (l *rawBuilderEntryList) Clone() clonable.Clonable {
	values := []*rawBuilderEntry(nil)
	return &rawBuilderEntryList{values: &values}
}
func (l *builderEntryList) EncodeSSZ(dst []byte) ([]byte, error) {
	if len(*l.values) > MaxBuilderEntries {
		return nil, fmt.Errorf("builder count %d exceeds %d", len(*l.values), MaxBuilderEntries)
	}
	for i, entry := range *l.values {
		if entry == nil {
			return nil, fmt.Errorf("builder %d is nil", i)
		}
	}
	return commonssz.EncodeDynamicList(dst, *l.values)
}
func (l *builderEntryList) DecodeSSZ(buf []byte, version int) error {
	values, err := commonssz.DecodeDynamicListStrict[*BuilderEntry](buf, 0, uint32(len(buf)), MaxBuilderEntries, version)
	if err != nil {
		return err
	}
	*l.values = values
	return nil
}
func (l *builderEntryList) DecodeSSZStrict(buf []byte, version int) error {
	return l.DecodeSSZ(buf, version)
}
func (l *builderEntryList) Clone() clonable.Clonable {
	values := []*BuilderEntry(nil)
	return &builderEntryList{values: &values}
}

func validateBuilderAuthData(data []byte) error {
	if len(data) == 0 || len(data) > MaxBuilderAuthDataSize {
		return fmt.Errorf("builder auth data length %d is outside [1,%d]", len(data), MaxBuilderAuthDataSize)
	}
	return nil
}

func validateBuilderURL(builderURL string) error {
	if len(builderURL) == 0 || len(builderURL) > MaxBuilderURLSize {
		return fmt.Errorf("builder URL length %d is outside [1,%d]", len(builderURL), MaxBuilderURLSize)
	}
	if !utf8.ValidString(builderURL) {
		return errors.New("builder URL is not valid UTF-8")
	}
	parsed, err := url.Parse(builderURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil {
		return errors.New("builder URL must be an HTTP(S) URL without user information")
	}
	return nil
}

func decodeStrictJSON(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("JSON contains trailing data")
	}
	return nil
}
