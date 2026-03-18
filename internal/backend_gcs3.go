// Copyright 2019 Ka-Hing Cheung
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
	. "github.com/kahing/goofys/api/common"

	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/jacobsa/fuse"
)

// GCS variant of S3
type GCS3 struct {
	*S3Backend
}

type GCS3MultipartBlobCommitInput struct {
	Size uint64
	ETag *string
	Prev *MultipartBlobAddInput
}

func NewGCS3(bucket string, flags *FlagStorage, config *S3Config) (*GCS3, error) {
	s3Backend, err := NewS3(bucket, flags, config)
	if err != nil {
		return nil, err
	}
	s3Backend.Capabilities().Name = "gcs3"
	s := &GCS3{S3Backend: s3Backend}
	s.S3Backend.gcs = true
	s.S3Backend.cap.NoParallelMultipart = true
	return s, nil
}

func (s *GCS3) Delegate() interface{} {
	return s
}

func (s *GCS3) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	// GCS does not have multi-delete
	var wg sync.WaitGroup
	var overallErr error

	for _, key := range param.Items {
		wg.Add(1)
		go func(key string) {
			_, err := s.DeleteBlob(&DeleteBlobInput{
				Key: key,
			})
			if err != nil && err != fuse.ENOENT {
				overallErr = err
			}
			wg.Done()
		}(key)
	}
	wg.Wait()
	if overallErr != nil {
		return nil, mapAwsError(overallErr)
	}

	return &DeleteBlobsOutput{}, nil
}

func (s *GCS3) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	// GCS resumable upload: we need to manipulate the request extensively
	// (clear query params, set x-goog-resumable, read Location header),
	// so we use raw HTTP with v2 signing instead of the SDK client.

	// Build the URL for the object
	endpoint := s.flags.Endpoint
	if endpoint == "" {
		endpoint = "https://storage.googleapis.com"
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	u.Path = "/" + s.bucket + "/" + param.Key

	httpReq, err := http.NewRequestWithContext(context.TODO(), "POST", u.String(), nil)
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("x-goog-resumable", "start")
	if param.ContentType != nil {
		httpReq.Header.Set("Content-Type", *param.ContentType)
	}
	if s.config.StorageClass != "" {
		httpReq.Header.Set("x-goog-storage-class", s.config.StorageClass)
	}
	if s.config.UseSSE {
		sseType := string(s.sseType)
		httpReq.Header.Set("x-amz-server-side-encryption", sseType)
		if s.config.UseKMS && s.config.KMSKeyID != "" {
			httpReq.Header.Set("x-amz-server-side-encryption-aws-kms-key-id", s.config.KMSKeyID)
		}
	}
	if s.config.ACL != "" {
		httpReq.Header.Set("x-goog-acl", s.config.ACL)
	}

	// Sign with v2 signer
	creds, err := s.awsCfg.Credentials.Retrieve(context.TODO())
	if err != nil {
		return nil, err
	}
	err = signV2Request(httpReq, creds, !s.config.Subdomain)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		s3Log.Errorf("CreateMultipartUpload %v = %v", param.Key, err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		body, _ := ioutil.ReadAll(resp.Body)
		s3Log.Errorf("CreateMultipartUpload %v = %v %v", param.Key, resp.StatusCode, string(body))
		return nil, fmt.Errorf("GCS resumable upload init failed: %v", resp.Status)
	}

	location := resp.Header.Get("Location")
	_, err = url.Parse(location)
	if err != nil {
		s3Log.Errorf("CreateMultipartUpload %v %v = %v", param.Key, location, err)
		return nil, err
	}

	return &MultipartBlobCommitInput{
		Key:         &param.Key,
		Metadata:    param.Metadata,
		UploadId:    &location,
		Parts:       make([]*string, 10000), // at most 10K parts
		backendData: &GCS3MultipartBlobCommitInput{},
	}, nil
}

func (s *GCS3) uploadPart(param *MultipartBlobAddInput, totalSize uint64, last bool) (etag *string, err error) {
	atomic.AddUint32(&param.Commit.NumParts, 1)

	if closer, ok := param.Body.(io.Closer); ok {
		defer closer.Close()
	}

	// The resumable upload URL (UploadId) serves as the authentication token,
	// so we use raw HTTP - no signing needed.
	uploadURL := *param.Commit.UploadId

	httpReq, err := http.NewRequestWithContext(context.TODO(), "PUT", uploadURL, param.Body)
	if err != nil {
		return nil, err
	}

	start := totalSize - param.Size
	end := totalSize - 1
	var size string
	if last {
		size = strconv.FormatUint(totalSize, 10)
	} else {
		size = "*"
	}

	contentRange := fmt.Sprintf("bytes %v-%v/%v", start, end, size)

	httpReq.Header.Set("Content-Length", strconv.FormatUint(param.Size, 10))
	httpReq.Header.Set("Content-Range", contentRange)
	httpReq.ContentLength = int64(param.Size)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		err = mapAwsError(err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 308 {
		// status indicating that we need more parts to finish this
		return nil, nil
	} else if resp.StatusCode >= 400 {
		body, _ := ioutil.ReadAll(resp.Body)
		err = fmt.Errorf("GCS upload part failed: %v %v", resp.Status, string(body))
		return
	}

	etagVal := resp.Header.Get("ETag")
	if etagVal != "" {
		etag = &etagVal
	}

	return
}

func (s *GCS3) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	var commitData *GCS3MultipartBlobCommitInput
	var ok bool
	if commitData, ok = param.Commit.backendData.(*GCS3MultipartBlobCommitInput); !ok {
		panic("Incorrect commit data type")
	}

	if commitData.Prev != nil {
		if commitData.Prev.Size == 0 || commitData.Prev.Size%(256*1024) != 0 {
			s3Log.Errorf("size of each block must be multiple of 256KB: %v", param.Size)
			return nil, fuse.EINVAL
		}

		_, err := s.uploadPart(commitData.Prev, commitData.Size, false)
		if err != nil {
			return nil, err
		}
	}
	commitData.Size += param.Size

	copy := *param
	commitData.Prev = &copy
	param.Body = nil

	return &MultipartBlobAddOutput{}, nil
}

func (s *GCS3) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	var commitData *GCS3MultipartBlobCommitInput
	var ok bool
	if commitData, ok = param.backendData.(*GCS3MultipartBlobCommitInput); !ok {
		panic("Incorrect commit data type")
	}

	if commitData.Prev == nil {
		panic("commit should include last part")
	}

	etag, err := s.uploadPart(commitData.Prev, commitData.Size, true)
	if err != nil {
		return nil, err
	}

	return &MultipartBlobCommitOutput{
		ETag: etag,
	}, nil
}

