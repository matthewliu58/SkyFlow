package split

import (
	"fmt"
	"log/slog"
	"rigel-client/util"
	"strconv"
	"time"
)

type ChunkState struct {
	Index       string
	FileName    string
	NewFileName string
	ObjectName  string
	Offset      int64 // Start position
	Size        int64 // Length
	LastSend    time.Time
	Acked       int
}

func SplitFilebyRange(size int64, start, length int64, fileName, newFileName string, noSplit bool, chunks *util.SafeMap,
	pre string, logger *slog.Logger) (int64, error) {

	// -------------------------- 1. Core logic: handle length ≤ 0 for full-range split --------------------------
	var (
		rangeStart int64 // Final chunk start position (left-closed)
		rangeEnd   int64 // Final chunk end position (right-open)
	)

	// Case 1: length ≤ 0 → full-range split (from start to end of file)
	if length <= 0 {
		rangeStart = start
		rangeEnd = size // End position = total file size (right-open)
		//logger.Info("detected length ≤ 0, executing full-range split", slog.String("pre", pre),
		//	"start", rangeStart, "end", rangeEnd, "total_file_size", size)
	} else {
		// Case 2: length > 0 → specified range split
		rangeStart = start
		rangeEnd = start + length
	}

	// -------------------------- 2. Range validation (compatible with full/specified range) --------------------------
	if rangeStart < 0 {
		return 0, fmt.Errorf("start cannot be negative (current value: %d)", rangeStart)
	}
	if rangeStart >= size {
		return 0, fmt.Errorf("start(%d) exceeds total file size(%d), no data to split", rangeStart, size)
	}
	// Adjust end position (avoid exceeding total file size)
	if rangeEnd > size {
		logger.Warn("chunk range exceeds total file size, auto-truncated", slog.String("pre", pre),
			"requested_end", rangeEnd, "file_total_size", size)
		rangeEnd = size
	}
	// Validate effective chunk length (must be > 0)
	effectiveLength := rangeEnd - rangeStart
	if effectiveLength <= 0 {
		return 0, fmt.Errorf("no effective chunk data (start=%d, end=%d, file_size=%d)", rangeStart, rangeEnd, size)
	}

	// -------------------------- 3. Initialize chunk variables --------------------------
	var (
		offset int64 = rangeStart // Chunk start offset (from rangeStart)
		index  int   = 0          // Chunk index
	)

	// -------------------------- 4. Execute split (core modification: support noSplit) --------------------------
	// If noSplit enabled, generate 1 chunk covering entire effective range
	if noSplit {
		partName := fmt.Sprintf("%s.part.%05d", newFileName, index)
		// Store chunk info
		chunks.Set(strconv.Itoa(index), &ChunkState{
			Index:       strconv.Itoa(index),
			FileName:    fileName,
			NewFileName: newFileName,
			ObjectName:  partName,
			Offset:      rangeStart,
			Size:        effectiveLength,
			Acked:       0,
		})

		// Log output
		logger.Info("SplitFile (no-split mode)", slog.String("pre", pre),
			"partName", partName,
			"global_start", rangeStart,
			"global_end", rangeEnd,
			"part_size", effectiveLength,
			"range_start", rangeStart,
			"range_end", rangeEnd,
			"total_file_size", size)

		logger.Info("split completed (no-split mode)", slog.String("pre", pre),
			"total_chunks", 1,
			"range_start", rangeStart,
			"range_end", rangeEnd,
			"effective_length", effectiveLength)

		// Return entire effective length as chunk size in no-split mode
		return effectiveLength, nil
	}

	// Auto-select chunk size (based on effective length)
	chunkSize := util.AutoSelectChunkSize(effectiveLength)
	// Original split logic (executed when noSplit=false)
	for offset < rangeEnd {
		// Calculate current chunk size
		partSize := chunkSize
		if offset+partSize > rangeEnd {
			partSize = rangeEnd - offset
		}

		// Construct chunk name
		partName := fmt.Sprintf("%s.part.%05d", newFileName, index)

		// Store chunk info
		chunks.Set(strconv.Itoa(index), &ChunkState{
			Index:       strconv.Itoa(index),
			FileName:    fileName,
			NewFileName: newFileName,
			ObjectName:  partName,
			Offset:      offset,
			Size:        partSize,
			Acked:       0,
		})

		// Log output
		logger.Info("SplitFile", slog.String("pre", pre),
			"partName", partName,
			"global_start", offset,
			"global_end", offset+partSize,
			"part_size", partSize,
			"range_start", rangeStart,
			"range_end", rangeEnd,
			"total_file_size", size)

		offset += partSize
		index++
	}

	logger.Info("split completed", slog.String("pre", pre),
		"total_chunks", index,
		"range_start", rangeStart,
		"range_end", rangeEnd,
		"effective_length", effectiveLength)

	return chunkSize, nil
}
