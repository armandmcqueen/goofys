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
	. "github.com/armandmcqueen/goofys/api/common"
	"syscall"

	//"github.com/aws/aws-sdk-go/aws/session"

	//"github.com/aws/aws-sdk-go/aws/session"

	"fmt"
	//"net/http"
	//"net/url"
	//"strconv"
	"strings"
	//"sync/atomic"
	//"syscall"
	//"time"

	"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/corehandlers"
	//"github.com/aws//aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/jacobsa/fuse"
)

type ManifestS3Backend struct {
	*s3.S3
	cap Capabilities

	awsConfig *aws.Config
	config    *S3Config
	sseType   string

	aws      bool
	gcs      bool
	v2Signer bool
}


func NewManifestS3() (*ManifestS3Backend, error) {
	debugMode := true

	config := &S3Config {
		Profile:         "",
		AccessKey:   ""    ,
		SecretKey:       "",
		RoleArn:  "",
		RoleExternalId: "",
		RoleSessionName: "",
		StsEndpoint:     "",

		RequesterPays: false,
		Region: "us-east-1",
		RegionSet:     true,

		StorageClass: "STANDARD",

		UseSSE:     false,
		UseKMS:     false,

		Subdomain: false,

	}
	awsConfig, err := config.ToAwsConfigNoFlags(true)
	if err != nil {
		return nil, err
	}
	s := &ManifestS3Backend{
		awsConfig: awsConfig,
		config:    config,
		cap: Capabilities{
			Name:             "s3",
			MaxMultipartSize: 5 * 1024 * 1024 * 1024,
		},
	}

	if debugMode {
		awsConfig.LogLevel = aws.LogLevel(aws.LogDebug | aws.LogDebugWithRequestErrors)
	}

	if config.UseKMS {
		//SSE header string for KMS server-side encryption (SSE-KMS)
		s.sseType = s3.ServerSideEncryptionAwsKms
	} else if config.UseSSE {
		//SSE header string for non-KMS server-side encryption (SSE-S3)
		s.sseType = s3.ServerSideEncryptionAes256
	}

	s.newManifestS3()
	return s, nil
}


func (s *ManifestS3Backend) Bucket() string {
	return ""
}


func (s *ManifestS3Backend) Capabilities() *Capabilities {
	return &s.cap
}



func (s *ManifestS3Backend) setV2Signer(handlers *request.Handlers) {
	handlers.Sign.Clear()
	handlers.Sign.PushBack(SignV2)
	handlers.Sign.PushBackNamed(corehandlers.BuildContentLengthHandler)
}

func (s *ManifestS3Backend) newManifestS3() {
	s.S3 = s3.New(s.config.Session, s.awsConfig)
	if s.config.RequesterPays {
		s.S3.Handlers.Build.PushBack(addRequestPayer)
	}
	if s.v2Signer {
		s.setV2Signer(&s.S3.Handlers)
	}
	s.S3.Handlers.Sign.PushBack(addAcceptEncoding)
}


func (s *ManifestS3Backend) testBucket(bucket, key string) (err error) {
	_, err = s.HeadBlobProper(bucket, &HeadBlobInput{Key: key})
	if err != nil {
		if err == fuse.ENOENT {
			err = nil
		}
	}

	return
}

func (s *ManifestS3Backend) fallbackV2Signer() (err error) {
	if s.v2Signer {
		return fuse.EINVAL
	}

	s3Log.Infoln("Falling back to v2 signer")
	s.v2Signer = true
	s.newManifestS3()
	return
}

func (s *ManifestS3Backend) InitProper(bucket, key string) error {

	// Init just confirms that AWS creds are working
	err := s.testBucket(bucket, key)
	if err != nil {
		panic(fmt.Sprintf("Could not access test AWS item bucket: %s, key: %s.", bucket, key))
	}

	return nil
}

func (s *ManifestS3Backend) Init(key string) error {
	panic("This method should never be called (it only exists for temporary compatibility with StorageBacked")
}

func (s *ManifestS3Backend) ListObjectsV2(params *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, string, error) {
	if s.aws {
		req, resp := s.S3.ListObjectsV2Request(params)
		err := req.Send()
		if err != nil {
			return nil, "", err
		}
		return resp, s.getRequestId(req), nil
	} else {
		v1 := s3.ListObjectsInput{
			Bucket:       params.Bucket,
			Delimiter:    params.Delimiter,
			EncodingType: params.EncodingType,
			MaxKeys:      params.MaxKeys,
			Prefix:       params.Prefix,
			RequestPayer: params.RequestPayer,
		}
		if params.StartAfter != nil {
			v1.Marker = params.StartAfter
		} else {
			v1.Marker = params.ContinuationToken
		}

		objs, err := s.S3.ListObjects(&v1)
		if err != nil {
			return nil, "", err
		}

		count := int64(len(objs.Contents))
		v2Objs := s3.ListObjectsV2Output{
			CommonPrefixes:        objs.CommonPrefixes,
			Contents:              objs.Contents,
			ContinuationToken:     objs.Marker,
			Delimiter:             objs.Delimiter,
			EncodingType:          objs.EncodingType,
			IsTruncated:           objs.IsTruncated,
			KeyCount:              &count,
			MaxKeys:               objs.MaxKeys,
			Name:                  objs.Name,
			NextContinuationToken: objs.NextMarker,
			Prefix:                objs.Prefix,
			StartAfter:            objs.Marker,
		}

		return &v2Objs, "", nil
	}
}


func (s *ManifestS3Backend) getRequestId(r *request.Request) string {
	return r.HTTPResponse.Header.Get("x-amz-request-id") + ": " +
		r.HTTPResponse.Header.Get("x-amz-id-2")
}

func (s *ManifestS3Backend) HeadBlobProper(bucket string, param *HeadBlobInput) (*HeadBlobOutput, error) {
	head := s3.HeadObjectInput{
		Bucket: PString(bucket),
		Key: &param.Key,
	}
	if s.config.SseC != "" {
		head.SSECustomerAlgorithm = PString("AES256")
		head.SSECustomerKey = &s.config.SseC
		head.SSECustomerKeyMD5 = &s.config.SseCDigest
	}

	req, resp := s.S3.HeadObjectRequest(&head)
	err := req.Send()
	if err != nil {
		return nil, mapAwsError(err)
	}
	return &HeadBlobOutput{
		BlobItemOutput: BlobItemOutput{
			Key:          &param.Key,
			ETag:         resp.ETag,
			LastModified: resp.LastModified,
			Size:         uint64(*resp.ContentLength),
			StorageClass: resp.StorageClass,
		},
		ContentType: resp.ContentType,
		Metadata:    metadataToLower(resp.Metadata),
		IsDirBlob:   strings.HasSuffix(param.Key, "/"),
		RequestId:   s.getRequestId(req),
	}, nil
}

func (s *ManifestS3Backend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	panic("This method should never be called. It only exists for temporary compatibility with StorageBackend. " +
			 "Use HeadBlobProper instead")
}

func (s *ManifestS3Backend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {

	// TODO: Implement the manifest version of this
	panic("Not implemented")

	//var maxKeys *int64
	//
	//if param.MaxKeys != nil {
	//	maxKeys = aws.Int64(int64(*param.MaxKeys))
	//}
	//
	//resp, reqId, err := s.ListObjectsV2(&s3.ListObjectsV2Input{
	//	Bucket:            &s.bucket,
	//	Prefix:            param.Prefix,
	//	Delimiter:         param.Delimiter,
	//	MaxKeys:           maxKeys,
	//	StartAfter:        param.StartAfter,
	//	ContinuationToken: param.ContinuationToken,
	//})
	//if err != nil {
	//	return nil, mapAwsError(err)
	//}
	//
	//prefixes := make([]BlobPrefixOutput, 0)
	//items := make([]BlobItemOutput, 0)
	//
	//for _, p := range resp.CommonPrefixes {
	//	prefixes = append(prefixes, BlobPrefixOutput{Prefix: p.Prefix})
	//}
	//for _, i := range resp.Contents {
	//	items = append(items, BlobItemOutput{
	//		Key:          i.Key,
	//		ETag:         i.ETag,
	//		LastModified: i.LastModified,
	//		Size:         uint64(*i.Size),
	//		StorageClass: i.StorageClass,
	//	})
	//}
	//
	//return &ListBlobsOutput{
	//	Prefixes:              prefixes,
	//	Items:                 items,
	//	NextContinuationToken: resp.NextContinuationToken,
	//	IsTruncated:           *resp.IsTruncated,
	//	RequestId:             reqId,
	//}, nil
}





func (s *ManifestS3Backend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	panic("This method should never be called. It only exists for temporary compatibility with StorageBackend. " +
			 "Use GetBlobProper instead")
}


func (s *ManifestS3Backend) GetBlobProper(bucket string, param *GetBlobInput) (*GetBlobOutput, error) {
	get := s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &param.Key,
	}

	if s.config.SseC != "" {
		get.SSECustomerAlgorithm = PString("AES256")
		get.SSECustomerKey = &s.config.SseC
		get.SSECustomerKeyMD5 = &s.config.SseCDigest
	}

	if param.Start != 0 || param.Count != 0 {
		var bytes string
		if param.Count != 0 {
			bytes = fmt.Sprintf("bytes=%v-%v", param.Start, param.Start+param.Count-1)
		} else {
			bytes = fmt.Sprintf("bytes=%v-", param.Start)
		}
		get.Range = &bytes
	}
	// TODO handle IfMatch

	req, resp := s.GetObjectRequest(&get)
	err := req.Send()
	if err != nil {
		return nil, mapAwsError(err)
	}

	return &GetBlobOutput{
		HeadBlobOutput: HeadBlobOutput{
			BlobItemOutput: BlobItemOutput{
				Key:          &param.Key,
				ETag:         resp.ETag,
				LastModified: resp.LastModified,
				Size:         uint64(*resp.ContentLength),
				StorageClass: resp.StorageClass,
			},
			ContentType: resp.ContentType,
			Metadata:    metadataToLower(resp.Metadata),
		},
		Body:      resp.Body,
		RequestId: s.getRequestId(req),
	}, nil
}


func (s *ManifestS3Backend) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (s *ManifestS3Backend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	return nil, syscall.ENOTSUP
}

func (s *ManifestS3Backend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, syscall.ENOTSUP
}



func (s *ManifestS3Backend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (s *ManifestS3Backend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (s *ManifestS3Backend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	return nil, syscall.ENOTSUP
}

func (s *ManifestS3Backend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	return nil, syscall.ENOTSUP
}

func (s *ManifestS3Backend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	return nil, syscall.ENOTSUP
}

func (s *ManifestS3Backend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	return nil, syscall.ENOTSUP
}

func (s *ManifestS3Backend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return nil, syscall.ENOTSUP
}

func (s *ManifestS3Backend) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	return nil, syscall.ENOTSUP
}

func (s *ManifestS3Backend) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	return nil, syscall.ENOTSUP
}




func (s *ManifestS3Backend) Delegate() interface{} {
	return s
}
