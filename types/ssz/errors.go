/*
   Copyright 2021 Erigon contributors

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

package ssz

import "errors"

var (
	ErrLowBufferSize    = errors.New("ssz(DecodeSSZ): bad encoding size")
	ErrBadDynamicLength = errors.New("ssz(DecodeSSZ): bad dynamic length")
	ErrBadOffset        = errors.New("ssz(DecodeSSZ): invalid offset")
	ErrBufferNotRounded = errors.New("ssz(DecodeSSZ): badly rounded operator")
	ErrTooBigList       = errors.New("ssz(DecodeSSZ): list too big")
)
