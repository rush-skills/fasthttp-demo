package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	maxPartSize        = 10 * 1024 * 1024
	minPartSize		   = 5 * 1024 * 1024
	maxRetries         = 3
	awsBucketRegion    = "ap-south-1"
	awsBucketName      = "twohat-test-bucket"
)

var (
	addr     = flag.String("addr", ":8080", "TCP address to listen to")
	compress = flag.Bool("compress", false, "Whether to enable transparent response compression")
	awsAccessKeyID     = getEnv("AWS_ACCESS_KEY", "")
	awsSecretAccessKey = getEnv("AWS_SECRET", "")
	svc *s3.S3
	buffers  = make([]*bytes.Buffer, 10)
	completedParts = make([][]*s3.CompletedPart, 10)
	partNumbers = make([]int, 10)
	resps = make([]*s3.CreateMultipartUploadOutput, 10)
)

func main() {
	flag.Parse()

	h := requestHandler
	if *compress {
		h = fasthttp.CompressHandler(h)
	}

	creds := credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, "")
	_, err := creds.Get()
	if err != nil {
		fmt.Printf("bad credentials: %s", err)
	}
	cfg := aws.NewConfig().WithRegion(awsBucketRegion).WithCredentials(creds)
	svc = s3.New(session.New(), cfg)

	for i := 0; i < 10; i++ {
		x := new(bytes.Buffer)
		x.Grow(maxPartSize)
		buffers[i] = x

		timestamp := time.Now()
		date := timestamp.Format("2006-01-02")
		filePath := "chat/" + date + "/content_logs_" + date + "_" + strconv.Itoa(i + 1)
		input := &s3.CreateMultipartUploadInput{
			Bucket:      aws.String(awsBucketName),
			Key:         aws.String(filePath),
			ContentType: aws.String("application/json"),
		}

		resps[i], err = svc.CreateMultipartUpload(input)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		partNumbers[i] = 1
		fmt.Println("Created multipart upload request for file: " + filePath)
	}

	SetupCloseHandler()

	go bufferHandler()

	if err := fasthttp.ListenAndServe(*addr, h); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}

func bufferHandler() {
	for {
		for i := 0; i < 10; i++ {
			v := buffers[i]
			if v.Len() > minPartSize {
				flushBuffer(v, i)
			}
		}
	}
}

func flushBuffer(v *bytes.Buffer, i int) {
	d := v.Next(maxPartSize)
	if len(d) > 0 {
		completedPart, err := uploadPart(svc, resps[i], d, partNumbers[i])
		if err != nil {
			fmt.Println(err.Error())
			err := abortMultipartUpload(svc, resps[i])
			if err != nil {
				fmt.Println(err.Error())
			}
			return
		}
		partNumbers[i]++
		completedParts[i] = append(completedParts[i], completedPart)
	}
}
func requestHandler(ctx *fasthttp.RequestCtx) {
	body := ctx.PostBody()
	clientId := fastjson.GetInt(body, "client_id")

	fmt.Fprintf(ctx, "OK")

	b := buffers[clientId-1]
	b.Write(body)
	b.Write([]byte("\n"))
}

func completeMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return svc.CompleteMultipartUpload(completeInput)
}

func uploadPart(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	for tryNum <= maxRetries {
		uploadResult, err := svc.UploadPart(partInput)
		if err != nil {
			if tryNum == maxRetries {
				if aerr, ok := err.(awserr.Error); ok {
					return nil, aerr
				}
				return nil, err
			}
			fmt.Printf("Retrying to upload part #%v\n", partNumber)
			tryNum++
		} else {
			fmt.Printf("Uploaded part #%v\n", partNumber)
			return &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(partNumber)),
			}, nil
		}
	}
	return nil, nil
}

func abortMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	fmt.Println("Aborting multipart upload for UploadId#" + *resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := svc.AbortMultipartUpload(abortInput)
	return err
}

func SetupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal")
		CompleteUpload()
		os.Exit(0)
	}()
}

func CompleteUpload() {
	for i := 0; i < 10; i++ {
		v := buffers[i]
		flushBuffer(v, i)
		completeResponse, err := completeMultipartUpload(svc, resps[i], completedParts[i])
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println("Successfully uploaded file: " + completeResponse.String())
	}
}

func getEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}
