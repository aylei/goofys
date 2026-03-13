// Copyright 2019 Databricks
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

package common

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type S3Config struct {
	Profile         string
	AccessKey       string
	SecretKey       string
	RoleArn         string
	RoleExternalId  string
	RoleSessionName string
	StsEndpoint     string

	RequesterPays bool
	Region        string
	RegionSet     bool

	StorageClass string

	UseSSE     bool
	UseKMS     bool
	KMSKeyID   string
	SseC       string
	SseCDigest string
	ACL        string

	Subdomain bool

	Credentials aws.CredentialsProvider
	AwsCfg      *aws.Config

	BucketOwner string
}

var s3AwsCfg *aws.Config

func (c *S3Config) Init() *S3Config {
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.StorageClass == "" {
		c.StorageClass = "STANDARD"
	}
	return c
}

func (c *S3Config) ToAwsConfig(flags *FlagStorage) (aws.Config, error) {
	var optFns []func(*config.LoadOptions) error

	optFns = append(optFns, config.WithRegion(c.Region))
	optFns = append(optFns, config.WithHTTPClient(&http.Client{
		Transport: &defaultHTTPTransport,
		Timeout:   flags.HTTPTimeout,
	}))

	if c.Profile != "" {
		optFns = append(optFns, config.WithSharedConfigProfile(c.Profile))
	}

	if c.Credentials == nil {
		if c.AccessKey != "" {
			c.Credentials = credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")
		}
	}

	if c.Credentials != nil {
		optFns = append(optFns, config.WithCredentialsProvider(c.Credentials))
	}

	if c.AwsCfg == nil {
		if s3AwsCfg == nil {
			cfg, err := config.LoadDefaultConfig(context.TODO(), optFns...)
			if err != nil {
				return aws.Config{}, err
			}
			s3AwsCfg = &cfg
		}
		c.AwsCfg = s3AwsCfg
	}

	awsCfg := *c.AwsCfg

	if flags.DebugS3 {
		awsCfg.ClientLogMode = aws.LogRequest | aws.LogResponse
	}

	if c.RoleArn != "" {
		stsCfg := awsCfg
		if c.Credentials != nil {
			stsCfg.Credentials = c.Credentials
		}
		stsClient := sts.NewFromConfig(stsCfg, func(o *sts.Options) {
			if c.StsEndpoint != "" {
				o.BaseEndpoint = &c.StsEndpoint
			}
		})
		c.Credentials = stscreds.NewAssumeRoleProvider(stsClient, c.RoleArn,
			func(o *stscreds.AssumeRoleOptions) {
				if c.RoleExternalId != "" {
					o.ExternalID = &c.RoleExternalId
				}
				o.RoleSessionName = c.RoleSessionName
			})
		awsCfg.Credentials = c.Credentials
	}

	if c.SseC != "" {
		key, err := base64.StdEncoding.DecodeString(c.SseC)
		if err != nil {
			return aws.Config{}, fmt.Errorf("sse-c is not base64-encoded: %v", err)
		}

		c.SseC = string(key)
		m := md5.Sum(key)
		c.SseCDigest = base64.StdEncoding.EncodeToString(m[:])
	}

	return awsCfg, nil
}
