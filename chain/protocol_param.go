/*
   Copyright 2023 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package chain

const (
	// EIP-4844: Shard Blob Transactions
	FieldElementsPerBlob         = 4096 // each field element is 32 bytes
	BlobSize                     = FieldElementsPerBlob * 32
	DataGasPerBlob        uint64 = 0x20000
	TargetDataGasPerBlock uint64 = 0x60000
	MaxDataGasPerBlock    uint64 = 0xC0000
	MaxBlobsPerBlock      uint64 = MaxDataGasPerBlock / DataGasPerBlob
)
