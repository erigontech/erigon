// Copyright 2024 The Erigon Authors
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

package graphql

import (
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
)

func TestGraphQLQueryBlock(t *testing.T) {
	t.Skip("Not a unit test")

	for i, tt := range []struct {
		body string
		want string
		code int
		comp string
	}{
		{ // Get chainID
			body: `{"query": "{chainID}","variables": null}`,
			want: `{"data":{"chainID":"0x[0-9A-F]+"}}`,
			code: 200,
			comp: "regexp",
		},
		{ // Should return latest block
			body: `{"query": "{block{number}}","variables": null}`,
			want: `{"data":{"block":{"number":\d{8,}}}}`,
			code: 200,
			comp: "regexp",
		},
		{ // Should return info about latest block
			body: `{"query": "{block{number,gasUsed,gasLimit}}","variables": null}`,
			want: `{"data":{"block":{"number":\d{8,},"gasUsed":\d+,"gasLimit":\d+}}`,
			code: 200,
			comp: "regexp",
		},
		{ // Should return info about genesis block
			body: `{"query": "{block(number:0){number,gasUsed,gasLimit}}","variables": null}`,
			want: `{"data":{"block":{"number":0,"gasUsed":0,"gasLimit":5000}}}`,
			code: 200,
		},
		{
			body: `{"query": "{block(number:-1){number,gasUsed,gasLimit}}","variables": null}`,
			want: `{"data":{"block":null}}`,
			code: 200,
		},
		{
			body: `{"query": "{block(number:-500){number,gasUsed,gasLimit}}","variables": null}`,
			want: `{"data":{"block":null}}`,
			code: 200,
		},
		{
			body: `{"query": "{block(number:\"0\"){number,gasUsed,gasLimit}}","variables": null}`,
			want: `{"data":{"block":{"number":0,"gasUsed":0,"gasLimit":5000}}}`,
			code: 200,
		},
		{
			body: `{"query": "{block(number:\"-33\"){number,gasUsed,gasLimit}}","variables": null}`,
			want: `{"data":{"block":null}}`,
			code: 200,
		},
		{
			body: `{"query": "{block(number:\"1337\"){number,gasUsed,gasLimit}}","variables": null}`,
			want: `{"data":{"block":{"number":1337,"gasUsed":0,"gasLimit":5000}}}`,
			code: 200,
		},
		{
			body: `{"query": "{block(number:\"0xbad\"){number,gasUsed,gasLimit}}","variables": null}`,
			want: `{"data":{"block":{"number":2989,"gasUsed":0,"gasLimit":5000}}}`,
			code: 200,
		},
		{ // hex strings are currently not supported. If that's added to the spec, this test will need to change
			body: `{"query": "{block(number:\"0x0\"){number,gasUsed,gasLimit}}","variables": null}`,
			want: `{"data":{"block":{"number":0,"gasUsed":0,"gasLimit":5000}}}`,
			code: 200,
		},
		{
			body: `{"query": "{block(number:\"a\"){number,gasUsed,gasLimit}}","variables": null}`,
			want: `{"data":{"block":null}}`,
			code: 200,
		},
		{
			body: `{"query": "{bleh{number}}","variables": null}"`,
			want: `{"errors":[{"message":"Cannot query field \"bleh\" on type \"Query\".","locations":[{"line":1,"column":2}],"extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}],"data":null}`,
			code: 422,
		},
		{ // Should return baseFeePerGas
			body: `{"query": "{block{number,baseFeePerGas}}","variables": null}`,
			want: `{"data":{"block":{"number":\d{8,},"baseFeePerGas":"\w+"}}`,
			code: 200,
			comp: "regexp",
		},
		{ // Should return ommerHash, ommerCount and ommers
			body: `{"query": "{block(number:15537381){ommerHash,ommerCount,ommers{hash}}}","variables": null}`,
			want: `{"data":{"block":{"ommerHash":"0x22f29046fa689683c504ad6fd9a7a9d5803f8e6bb66de435438b563f586651fe","ommerCount":1,"ommers":[{"hash":"0xf4af15465ca81e65866c6e64cbc446b735a06fb2118dda69a7c21d4ab0b1e217"}]}}}`,
			code: 200,
		},
		{ // Should return withdrawals
			body: `{"query": "{block{withdrawals{index,validator,address,amount}}}","variables": null}`,
			want: `{"data":{"block":{"withdrawals":\[({"index":\d+,"validator":\d+,"address":"0x[0-9a-fA-F]+","amount":"0x[0-9a-fA-F]+"},?)*\]}}}`,
			code: 200,
			comp: "regexp",
		},
		// should return `estimateGas` as decimal
		/*
			{
				body: `{"query": "{block{ estimateGas(data:{}) }}"}`,
				want: `{"data":{"block":{"estimateGas":53000}}}`,
				code: 200,
			},
		*/
		// should return `status` as decimal
		/*
			{
				body: `{"query": "{block {number call (data : {from : \"0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b\", to: \"0x6295ee1b4f6dd65047762f924ecd367c17eabf8f\", data :\"0x12a7b914\"}){data status}}}"}`,
				want: `{"data":{"block":{"number":10,"call":{"data":"0x","status":1}}}}`,
				code: 200,
			},
		*/
	} {
		resp, err := http.Post("http://localhost:8545/graphql", "application/json", strings.NewReader(tt.body))
		if err != nil {
			t.Fatalf("could not post: %v", err)
		}
		defer resp.Body.Close()

		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("could not read from response body: %v", err)
		}
		if have := string(bodyBytes); tt.comp == "" && have != tt.want {
			t.Errorf("testcase (exact match) %d %s,\nhave:\n%v\nwant:\n%v", i, tt.body, have, tt.want)
		}
		if have := string(bodyBytes); tt.comp == "regexp" {
			match, err := regexp.MatchString(tt.want, have)
			if err != nil || !match {
				t.Errorf("testcase (regexp) %d %s,\nhave:\n%v\nwant:\n%v %t", i, tt.body, have, tt.want, match)
			}
		}
		if tt.code != resp.StatusCode {
			t.Errorf("testcase (status code) %d %s,\nwrong statuscode, have: %v, want: %v", i, tt.body, resp.StatusCode, tt.code)
		}
	}
}
