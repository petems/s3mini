package cmd

import (
	"errors"
	"io"
	"io/ioutil"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"gopkg.in/cheggaaa/pb.v1"

	"github.com/spf13/cobra"
)

type progressWriter struct {
	writer  io.WriterAt
	pb 			*pb.ProgressBar
}

func (pw *progressWriter) WriteAt(p []byte, off int64) (int, error) {
	pw.pb.Add(len(p))
	return pw.writer.WriteAt(p, off)
}

// cpCmd represents the get command
var cpCmd = &cobra.Command{
	Use:   "cp <S3 URI> <destination directory>",
	Short: "Download files from S3",
	Long:  ``,
	Args: func(cmd *cobra.Command, args []string) error {
    if len(args) < 2 {
      return errors.New("requires Two arguments")
		}
		err := validateS3URIString(string(args[0]))
		if err != nil {
			return err
		}
		return nil
  },
	Run: func(cmd *cobra.Command, args []string) {

		err := Download(s3Client, args[0], args[1])
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(cpCmd)
}

func getFileSize(svc *s3.S3, bucket string, prefix string) (filesize int64, error error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix),
	}

	resp, err := svc.HeadObject(params)
	if err != nil {
		return 0, err
	}

	return *resp.ContentLength, nil
}

func parseS3Uri(s3Uri string) (bucket string, prefix string) {
	s3UriParts := strings.Split(s3Uri, "/")
	prefix = strings.Join(s3UriParts[3:], "/")
	bucket = s3UriParts[2]
	return bucket, prefix
}

func parseFilename(keyString string) (filename string) {
	ss := strings.Split(keyString, "/")
	s := ss[len(ss)-1]
	return s
}

// Download downloads a file to the local filesystem using s3downloader
func Download(svc *s3.S3, s3Uri string, destination string) error {

	bucket, key := parseS3Uri(s3Uri)

	filename := parseFilename(key)

	temp, err := ioutil.TempFile(destination, "s3mini-")
	if err != nil {
		panic(err)
	}

	size, err := getFileSize(svc, bucket, key)

	if err != nil {
		panic(err)
	}

	bar := pb.New64(size).SetUnits(pb.U_BYTES)
	bar.Start()

	writer := &progressWriter{writer: temp, pb: bar}

	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	tempfileName := temp.Name()

	if _, err := downloader.Download(writer, params); err != nil {
		bar.Set64(bar.Total)
		log.Printf("Download failed! Deleting tempfile: %s", tempfileName)
		os.Remove(tempfileName)
		panic(err)
	}

	bar.FinishPrint(fmt.Sprintf("Downloaded %s", filename))

	if err := temp.Close(); err != nil {
		panic(err)
	}

	if err := os.Rename(temp.Name(), filename); err != nil {
		panic(err)
	}

	return nil
}
