// Copyright 2015 - 2017 Ka-Hing Cheung
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const (
	signatureVersion = "2"
	signatureMethod  = "HmacSHA1"
	timeFormat       = "Mon, 2 Jan 2006 15:04:05 +0000"
)

var subresources = []string{
	"acl",
	"delete",
	"lifecycle",
	"location",
	"logging",
	"notification",
	"partNumber",
	"policy",
	"requestPayment",
	"torrent",
	"uploadId",
	"uploads",
	"versionId",
	"versioning",
	"versions",
	"website",
}

// escapePath percent-encodes a URL path, preserving forward slashes.
func escapePath(path string, encodeSep bool) string {
	var buf strings.Builder
	for i := 0; i < len(path); i++ {
		c := path[i]
		if c == '/' && !encodeSep {
			buf.WriteByte(c)
		} else if isValidPathByte(c) {
			buf.WriteByte(c)
		} else {
			fmt.Fprintf(&buf, "%%%02X", c)
		}
	}
	return buf.String()
}

func isValidPathByte(c byte) bool {
	if c >= 'A' && c <= 'Z' {
		return true
	}
	if c >= 'a' && c <= 'z' {
		return true
	}
	if c >= '0' && c <= '9' {
		return true
	}
	switch c {
	case '-', '_', '.', '~':
		return true
	}
	return false
}

type v2SigningMiddleware struct {
	credentials aws.CredentialsProvider
	pathStyle   bool
}

func (m *v2SigningMiddleware) ID() string { return "V2Signer" }

func (m *v2SigningMiddleware) HandleFinalize(
	ctx context.Context,
	in middleware.FinalizeInput,
	next middleware.FinalizeHandler,
) (middleware.FinalizeOutput, middleware.Metadata, error) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return next.HandleFinalize(ctx, in)
	}

	// Skip signing for anonymous credentials
	creds, err := m.credentials.Retrieve(ctx)
	if err != nil {
		return middleware.FinalizeOutput{}, middleware.Metadata{}, err
	}
	if creds.AccessKeyID == "" && creds.SecretAccessKey == "" {
		return next.HandleFinalize(ctx, in)
	}

	err = signV2Request(req.Request, creds, m.pathStyle)
	if err != nil {
		return middleware.FinalizeOutput{}, middleware.Metadata{}, err
	}

	return next.HandleFinalize(ctx, in)
}

func signV2Request(httpReq *http.Request, creds aws.Credentials, pathStyle bool) error {
	query := httpReq.URL.Query()

	contentMD5 := httpReq.Header.Get("Content-MD5")
	contentType := httpReq.Header.Get("Content-Type")
	date := time.Now().UTC().Format(timeFormat)
	httpReq.Header.Set("x-amz-date", date)

	if creds.SessionToken != "" {
		httpReq.Header.Set("x-amz-security-token", creds.SessionToken)
	}

	// in case this is a retry, ensure no signature present
	httpReq.Header.Del("Authorization")

	method := httpReq.Method

	uri := httpReq.URL.Opaque
	if uri != "" {
		if questionMark := strings.Index(uri, "?"); questionMark != -1 {
			uri = uri[0:questionMark]
		}
		uri = "/" + strings.Join(strings.Split(uri, "/")[3:], "/")
	} else {
		uri = httpReq.URL.Path
	}
	path := escapePath(uri, false)
	if !pathStyle {
		host := strings.SplitN(httpReq.URL.Host, ".", 2)[0]
		path = "/" + host + uri
	}
	if path == "" {
		path = "/"
	}

	// build URL-encoded query keys and values
	queryKeysAndValues := []string{}
	for _, key := range subresources {
		if _, ok := query[key]; ok {
			k := strings.Replace(url.QueryEscape(key), "+", "%20", -1)
			v := strings.Replace(url.QueryEscape(query.Get(key)), "+", "%20", -1)
			if v != "" {
				v = "=" + v
			}
			queryKeysAndValues = append(queryKeysAndValues, k+v)
		}
	}

	// join into one query string
	queryStr := strings.Join(queryKeysAndValues, "&")

	if queryStr != "" {
		path += "?" + queryStr
	}

	tmp := []string{
		method,
		contentMD5,
		contentType,
		"",
	}

	var headers []string
	for k := range httpReq.Header {
		k = strings.ToLower(k)
		if strings.HasPrefix(k, "x-amz-") {
			headers = append(headers, k)
		}
	}
	sort.Strings(headers)

	for _, k := range headers {
		v := strings.Join(httpReq.Header[http.CanonicalHeaderKey(k)], ",")
		tmp = append(tmp, k+":"+v)
	}

	tmp = append(tmp, path)

	// build the canonical string for the V2 signature
	stringToSign := strings.Join(tmp, "\n")

	hash := hmac.New(sha1.New, []byte(creds.SecretAccessKey))
	hash.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(hash.Sum(nil))
	httpReq.Header.Set("Authorization",
		"AWS "+creds.AccessKeyID+":"+signature)

	return nil
}

// V2SignerMiddleware returns a function that adds v2 signing middleware
// to the smithy middleware stack, replacing the default v4 signer.
func V2SignerMiddleware(creds aws.CredentialsProvider, pathStyle bool) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		// Remove default v4 signer
		_, err := stack.Finalize.Remove("Signing")
		if err != nil {
			// Signer might not exist yet, that's ok
		}
		// Also try to remove the v4a signer
		_, _ = stack.Finalize.Remove("SigV4SignHTTPRequestMiddleware")
		// Add v2 signer
		return stack.Finalize.Add(&v2SigningMiddleware{
			credentials: creds,
			pathStyle:   pathStyle,
		}, middleware.After)
	}
}
