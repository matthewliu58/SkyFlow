package gcs

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"cloud.google.com/go/storage"
)

type Compose struct {
	bucket   string
	credFile string
}

func NewCompose(
	bucket, credFile string,
	pre string, // Log prefix (keep consistent with previous)
	logger *slog.Logger, // Log instance (keep consistent with previous)
) *Compose {
	c := &Compose{
		bucket:   bucket,
		credFile: credFile,
	}
	// Same log printing logic as other init functions
	logger.Info("NewCompose", slog.String("pre", pre), slog.Any("Compose", *c))
	return c
}

func (c *Compose) ComposeFile(
	ctx context.Context,
	objectName string,
	parts []string,
	pre string,
	logger *slog.Logger,
) error {

	bucket := c.bucket
	//objectName := c.objectName
	credFile := c.credFile
	//parts := c.parts

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled: %w", ctx.Err())
		logger.Error("GCP ComposeTree canceled before connect", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	// 1. Special handling for single-file scenario (core optimization)
	if len(parts) == 1 {
		// Set GCP credential env var
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

		// Create GCS client
		ctx_, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		client, err := storage.NewClient(ctx_)
		if err != nil {
			logger.Error("create GCS client failed", slog.String("pre", pre), slog.Any("err", err))
			return fmt.Errorf("new storage client failed: %w", err)
		}
		defer client.Close()

		bkt := client.Bucket(bucket)
		partName := parts[0]

		// 1.1 Single file same name: No operation needed, return directly
		if partName == objectName {
			logger.Info("single file name matches final name, skip compose",
				slog.String("pre", pre),
				slog.String("object", objectName))
			return nil
		}

		// 1.2 Single file different name: Copy + delete source after success (with fault tolerance)
		logger.Info("start copy single file to final location",
			slog.String("pre", pre),
			slog.String("from", partName),
			slog.String("to", objectName))

		// 执行复制操作
		_, err = bkt.Object(objectName).CopierFrom(bkt.Object(partName)).Run(ctx_)
		if err != nil {
			logger.Error("copy single file failed",
				slog.String("pre", pre),
				slog.String("from", partName),
				slog.String("to", objectName),
				slog.Any("err", err))
			return fmt.Errorf("copy single file failed: %w", err)
		}
		logger.Info("copy single file success",
			slog.String("pre", pre),
			slog.String("from", partName),
			slog.String("to", objectName))

		// Delete source file after copy success (fault tolerance: delete failure only warns, doesn't interrupt)
		if delErr := bkt.Object(partName).Delete(ctx_); delErr != nil {
			logger.Warn("delete single source file failed (copy success)",
				slog.String("pre", pre),
				slog.String("partName", partName),
				slog.Any("err", delErr))
		} else {
			logger.Info("delete single source file success",
				slog.String("pre", pre),
				slog.String("partName", partName))
		}

		logger.Info("single file process completed",
			slog.String("pre", pre),
			slog.String("finalObject", objectName))
		return nil
	}

	// 2. Multi-file scenario: Original tree compose logic (preserved)
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)
	// Create GCS client
	//ctx_, cancel := context.WithTimeout(ctx, 1*time.Minute)
	//defer cancel()
	client, err := storage.NewClient(ctx)
	if err != nil {
		logger.Error("create GCS client failed", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("new storage client failed: %w", err)
	}
	defer client.Close()

	bkt := client.Bucket(bucket)
	current := parts // Uncomment to sort: util.SortPartStrings(parts)
	level := 0
	var tempObjects []string // Track all temp compose files

	// Tree compose: merge up to 32 chunks each time (GCS Compose API limit)
	for len(current) > 1 {
		var next []string

		for i := 0; i < len(current); i += 32 {
			end := i + 32
			if end > len(current) {
				end = len(current)
			}
			group := current[i:end]
			tmpObjectName := fmt.Sprintf("%s.compose.%d.%d", objectName, level, i)

			// Build list of objects to compose
			var objs []*storage.ObjectHandle
			for _, p := range group {
				objs = append(objs, bkt.Object(p))
			}

			// Execute compose operation
			if _, err := bkt.Object(tmpObjectName).ComposerFrom(objs...).Run(ctx); err != nil {
				logger.Error("compose temp object failed",
					slog.String("pre", pre),
					slog.String("tmpObjectName", tmpObjectName),
					slog.Int("level", level),
					slog.Any("group", group),
					slog.Any("err", err))
				return fmt.Errorf("compose temp object %s failed: %w", tmpObjectName, err)
			}

			next = append(next, tmpObjectName)
			tempObjects = append(tempObjects, tmpObjectName)
			logger.Info("compose temp object success",
				slog.String("pre", pre),
				slog.String("name", tmpObjectName),
				slog.Int("level", level),
				slog.Any("from", group))
		}

		current = next
		level++
	}

	// 3. Multi-file compose final step: temp file -> final file
	if err := finalizeObject(ctx, bkt, current[0], objectName); err != nil {
		logger.Error("finalize object failed", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("finalize object failed: %w", err)
	}

	// 4. Cleanup temp files and parts for multi-file scenario
	// 4.1 Delete intermediate temp files (exclude final temp file deleted in finalizeObject)
	for _, tmp := range tempObjects {
		if tmp != current[0] {
			if delErr := bkt.Object(tmp).Delete(ctx); delErr != nil {
				logger.Warn("delete temp object failed",
					slog.String("pre", pre),
					slog.String("tmp", tmp),
					slog.Any("err", delErr))
			}
		}
	}

	// 4.2 Delete original part files
	for _, p := range parts {
		if delErr := bkt.Object(p).Delete(ctx); delErr != nil {
			logger.Warn("delete part object failed",
				slog.String("pre", pre),
				slog.String("part", p),
				slog.Any("err", delErr))
		}
	}

	logger.Info("multi file compose success",
		slog.String("pre", pre),
		slog.String("finalObject", objectName))
	return nil
}

// finalizeObject Copy temp file to final location and delete temp file
// Only used for final step of multi-file compose
func finalizeObject(ctx context.Context, bkt *storage.BucketHandle, tempName, finalName string) error {
	// Copy temp file to final file
	_, err := bkt.Object(finalName).
		CopierFrom(bkt.Object(tempName)).
		Run(ctx)
	if err != nil {
		return fmt.Errorf("copy temp to final failed: %w", err)
	}

	// Delete temp file
	if err := bkt.Object(tempName).Delete(ctx); err != nil {
		return fmt.Errorf("delete temp object failed: %w", err)
	}
	return nil
}
