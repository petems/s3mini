package cmd

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	humanize "github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/petems/s3mini/s3wrapper"
)

// lsCmd represents the ls command
var lsCmd = &cobra.Command{
	Use:   "ls <S3 URIs>",
	Short: "List S3 prefixes",
	Long:  ``,
	Args:  validateS3URIs(cobra.MinimumNArgs(1)),
	Run: func(cmd *cobra.Command, args []string) {
		recursive, err := cmd.Flags().GetBool("recursive")
		if err != nil {
			log.Fatal(err)
		}
		humanReadable, err := cmd.Flags().GetBool("human-readable")
		if err != nil {
			log.Fatal(err)
		}
		includeDates, err := cmd.Flags().GetBool("with-date")
		if err != nil {
			log.Fatal(err)
		}

		listChan, err := Ls(s3Client, args, recursive, delimiter, searchDepth, keyRegex)
		if err != nil {
			log.Fatal(err)
		}

		for listOutput := range listChan {
			if listOutput.IsPrefix {
				fmt.Printf("%10s %s\n", "DIR", listOutput.FullKey)
			} else {
				var size string
				if humanReadable {
					size = fmt.Sprintf("%10s", humanize.Bytes(uint64(listOutput.Size)))
				} else {
					size = fmt.Sprintf("%10d", listOutput.Size)
				}
				date := ""
				if includeDates {
					date = " " + (listOutput.LastModified).Format("2006-01-02T15:04:05")
				}
				fmt.Printf("%s%s %s\n", size, date, listOutput.FullKey)
			}
		}
	},
}

// Ls lists S3 keys and prefixes using svc, s3Uris specifies which S3 prefixes/keys to list, recursive tells whether or not to list everything
// under s3Uris, delimiter tells which character to use as the delimiter for listing prefixes, searchDepth determines how many prefixes to list
// before parallelizing list calls, keyRegex is a regex filter on Keys
func Ls(svc *s3.S3, s3Uris []string, recursive bool, delimiter string, searchDepth int, keyRegex string) (chan *s3wrapper.ListOutput, error) {
	wrap := s3wrapper.New(svc, maxParallel)
	outChan := make(chan *s3wrapper.ListOutput, 10000)

	slashRegex := regexp.MustCompile("/")
	bucketExpandedS3Uris := make([]string, 0, 1000)

	// transforms uris with partial or no bucket (e.g. s3://)
	// into a listable uri
	for _, uri := range s3Uris {
		// filters uris without bucket or partial bucket specified
		// s3 key/prefix queries will always have 3 slashes, where-as
		// bucket queries will always have 2 (e.g. s3://<bucket>/<prefix-or-key> vs s3://<bucket-prefix>)
		if len(slashRegex.FindAllString(uri, -1)) == 2 {
			buckets, err := wrap.ListBuckets(uri)
			if err != nil {
				return nil, err
			}
			for _, bucket := range buckets {
				// add the bucket back to the list of s3 uris in cases where
				// we are searching beyond the bucket
				if recursive || searchDepth > 0 {
					resp, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: aws.String(bucket)})
					if err != nil {
						return nil, err
					}
					// if the region is location constrained and not in the region we specified in our config
					// then don't list it, otherwise we will get an error from the AWS API
					if resp.LocationConstraint == nil || *resp.LocationConstraint == *svc.Client.Config.Region {
						bucketExpandedS3Uris = append(bucketExpandedS3Uris, s3wrapper.FormatS3Uri(bucket, ""))
					}
				} else {
					key := ""
					fullKey := s3wrapper.FormatS3Uri(bucket, "")
					outChan <- &s3wrapper.ListOutput{
						IsPrefix:     true,
						Key:          key,
						FullKey:      fullKey,
						LastModified: time.Time{},
						Size:         0,
						Bucket:       bucket,
					}
				}
			}
		} else {
			bucketExpandedS3Uris = append(bucketExpandedS3Uris, uri)
		}
	}
	s3Uris = bucketExpandedS3Uris

	go func() {
		defer close(outChan)

		for i := 0; i < searchDepth; i++ {
			newS3Uris := make([]string, 0)
			for itm := range wrap.ListAll(s3Uris, false, delimiter, keyRegex) {
				if itm.IsPrefix {
					newS3Uris = append(newS3Uris, strings.TrimRight(itm.FullKey, delimiter)+delimiter)
				} else {
					outChan <- itm
				}
			}
			s3Uris = newS3Uris
		}

		for itm := range wrap.ListAll(s3Uris, recursive, delimiter, keyRegex) {
			outChan <- itm
		}
	}()

	return outChan, nil
}

func init() {
	rootCmd.AddCommand(lsCmd)

	lsCmd.Flags().BoolP("recursive", "r", false, "Get all keys for this prefix")
	lsCmd.Flags().BoolP("human-readable", "H", false, "Output human-readable object sizes")
	lsCmd.Flags().BoolP("with-date", "d", false, "Include the last modified date")
}
