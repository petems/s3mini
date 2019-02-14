package s3wrapper

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

// ListOutput represents the pruned and
// normalized result of a list call to S3,
// this is meant to cut down on memory and
// overhead being used in the channels
type ListOutput struct {
	IsPrefix     bool
	Size         int64
	Key          string
	LastModified time.Time
	Bucket       string
	FullKey      string
}

// S3Wrapper is a wrapper for the S3
// library which aims to make some of
// it's functions faster
type S3Wrapper struct {
	concurrencySemaphore chan struct{}
	svc                  *s3.S3
}

// ParseS3Uri parses a s3 uri into its bucket and prefix
func parseS3Uri(s3Uri string) (bucket string, prefix string) {
	s3UriParts := strings.Split(s3Uri, "/")
	prefix = strings.Join(s3UriParts[3:], "/")
	bucket = s3UriParts[2]
	return bucket, prefix
}

// FormatS3Uri takes a bucket and a prefix and turns it into
// a S3 URI
func FormatS3Uri(bucket string, key string) string {
	return fmt.Sprintf("s3://%s", path.Join(bucket, key))
}

// New creates a new S3Wrapper
func New(svc *s3.S3, maxParallel int) *S3Wrapper {
	return &S3Wrapper{
		svc:                  svc,
		concurrencySemaphore: make(chan struct{}, maxParallel),
	}
}

// GetErrorMapFromRegex takes an error message and a regex for key parts of the error
// message and returns a map of the error messages as strings
func GetErrorMapFromRegex(regEx, url string) (errorsMap map[string]string) {

	var compRegEx = regexp.MustCompile(regEx)
	match := compRegEx.FindStringSubmatch(url)

	errorsMap = make(map[string]string)
	for i, name := range compRegEx.SubexpNames() {
		if i > 0 && i <= len(match) {
			errorsMap[name] = match[i]
		}
	}
	return
}

// WithMaxConcurrency sets the maximum concurrency for the S3 operations
func (w *S3Wrapper) WithMaxConcurrency(maxConcurrency int) *S3Wrapper {
	w.concurrencySemaphore = make(chan struct{}, maxConcurrency)
	return w
}

// ListAll is a convienience function for listing and collating all the results for multiple S3 URIs
func (w *S3Wrapper) ListAll(s3Uris []string, recursive bool, delimiter string, keyRegex string) chan *ListOutput {
	ch := make(chan *ListOutput, 10000)
	var wg sync.WaitGroup
	for _, s3Uri := range s3Uris {
		wg.Add(1)
		go func(s3Uri string) {
			defer wg.Done()
			for itm := range w.List(s3Uri, recursive, delimiter, keyRegex) {
				ch <- itm
			}
		}(s3Uri)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	return ch
}

// List is a wrapping function to parallelize listings and normalize the results from the API
func (w *S3Wrapper) List(s3Uri string, recursive bool, delimiter string, keyRegex string) chan *ListOutput {
	bucket, prefix := parseS3Uri(s3Uri)
	if recursive {
		delimiter = ""
	}
	var keyRegexFilter *regexp.Regexp
	if keyRegex != "" {
		keyRegexFilter = regexp.MustCompile(keyRegex)
	}

	params := &s3.ListObjectsV2Input{
		Bucket:       aws.String(bucket), // Required
		Delimiter:    aws.String(delimiter),
		EncodingType: aws.String(s3.EncodingTypeUrl),
		FetchOwner:   aws.Bool(false),
		MaxKeys:      aws.Int64(1000),
		Prefix:       aws.String(prefix),
	}

	ch := make(chan *ListOutput, 10000)
	go func() {
		defer close(ch)
		w.concurrencySemaphore <- struct{}{}
		defer func() { <-w.concurrencySemaphore }()

		err := w.svc.ListObjectsV2Pages(params, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, prefix := range page.CommonPrefixes {
				if *prefix.Prefix != delimiter {
					escapedPrefix, err := url.QueryUnescape(*prefix.Prefix)
					if err != nil {
						escapedPrefix = *prefix.Prefix
					}
					formattedKey := FormatS3Uri(bucket, escapedPrefix)
					ch <- &ListOutput{
						IsPrefix:     true,
						Key:          escapedPrefix,
						FullKey:      formattedKey,
						LastModified: time.Time{},
						Size:         0,
						Bucket:       bucket,
					}
				}
			}

			for _, key := range page.Contents {
				escapedKey, err := url.QueryUnescape(*key.Key)
				if err != nil {
					escapedKey = *key.Key
				}
				formattedKey := FormatS3Uri(bucket, escapedKey)
				if keyRegexFilter != nil && !keyRegexFilter.MatchString(formattedKey) {
					continue
				}
				ch <- &ListOutput{
					IsPrefix:     false,
					Key:          escapedKey,
					FullKey:      formattedKey,
					LastModified: *key.LastModified,
					Size:         *key.Size,
					Bucket:       bucket,
				}
			}
			return true
		})
		if err != nil {
			aerr := err.(awserr.Error)
			if aerr.Code() == "MissingRegion" {
				fmt.Println("Could not find a region set, please set with AWS_REGION or within your AWS configuration")
				os.Exit(1)
			}
			if aerr.Code() == "BucketRegionError" {
				wrongRegionRegex := `incorrect region, the bucket is not in '(?P<wrongRegion>[[:alpha:]+-[[:alpha:]+-[[:alpha:]]+)' region`
				errorMap := GetErrorMapFromRegex(wrongRegionRegex, aerr.Message())
				fmt.Println("Bucket region given is incorrect, try us-east-1")
				fmt.Println(errorMap)
				os.Exit(1)
			}
			if aerr.Code() == "AuthorizationHeaderMalformed" {
				wrongRegionRegex := `the region '(?P<wrongRegion>[[:alpha:]+-[[:alpha:]+-[[:alpha:]]+)' is wrong; expecting '(?P<correctRegion>[[:alpha:]+-[[:alpha:]+-[[:alpha:]]+)'`
				errorMap := GetErrorMapFromRegex(wrongRegionRegex, aerr.Message())
				fmt.Println(errorMap)
				os.Exit(1)
			}
			fmt.Println(err)
			os.Exit(1)
		}
	}()

	return ch
}

// GetReader retrieves an appropriate reader for the given bucket and key
func (w *S3Wrapper) GetReader(bucket string, key string) (io.ReadCloser, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	resp, err := w.svc.GetObject(params)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// ListBuckets returns a list of bucket names and does a prefix
// filter based on s3Uri (of the form s3://<bucket-prefix>)
func (w *S3Wrapper) ListBuckets(s3Uri string) ([]string, error) {

	bucketPrefix, _ := parseS3Uri(s3Uri)
	results, err := w.svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	buckets := make([]string, 0, len(results.Buckets))
	for _, bucket := range results.Buckets {
		if *bucket.Name != "" && !strings.HasPrefix(*bucket.Name, bucketPrefix) {
			continue
		}
		buckets = append(buckets, *bucket.Name)
	}
	return buckets, nil
}
