package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

func dailyReportSize(s3Client *s3.Client) error {
    buckets, err := listBuckets(s3Client)
    if err != nil {
        return fmt.Errorf("failed to list buckets: %w", err)
    }

    type bucketResult struct {
        name  string
        size  float64
    }

    results := make(chan bucketResult, len(buckets))
    g, ctx := errgroup.WithContext(context.Background())
    sem := make(chan struct{}, runtime.NumCPU())

    for _, bucketName := range buckets {
        sem <- struct{}{}	// Acquire a concurrency slot

        g.Go(func() error {
            defer func() { <-sem }() // Release slot when done

            // Respect context cancellation
            select {
            case <-ctx.Done():
                return nil
            default:
            }

            size := calculateBucketSize(s3Client, bucketName)
            results <- bucketResult{bucketName, size}
            return nil
        })
    }

    // Close results channel when all workers complete
    go func() {
        _ = g.Wait()
        close(results)
    }()

    var report strings.Builder
    for result := range results {
        report.WriteString(fmt.Sprintf("Bucket %s: %.2f GB\n", result.name, result.size),)
    }

    fmt.Println(report.String())
    return nil
}

func listBuckets(s3Client *s3.Client) ([]string, error) {
	result, err := s3Client.ListBuckets(context.TODO(), nil)
	if err != nil {
		return nil, err
	}

	bucketNames := make([]string, len(result.Buckets))
	for i, bucket := range result.Buckets {
		bucketNames[i] = *bucket.Name
	}

	return bucketNames, nil
}

func calculateBucketSize(s3Client *s3.Client, bucketName string) float64 {
	var totalSize int64

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("Error listing objects in bucket %s: %v", bucketName, err)
			continue
		}

		for _, obj := range page.Contents {
			totalSize += *obj.Size
		}
	}

	return float64(totalSize) / (1024 * 1024 * 1024)	// Convert to GB unit
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
    if err != nil {
        log.Fatal(err)
    }

	s3Client := s3.NewFromConfig(cfg)

	err = dailyReportSize(s3Client)
	if err != nil {
		log.Fatal(err)
	}
}
