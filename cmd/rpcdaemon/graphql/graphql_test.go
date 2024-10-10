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
