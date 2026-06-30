package util

const (
	GCSCLoud   = "gcs-cloud"
	S3Cloud    = "s3-cloud"
	RemoteDisk = "remote-disk"
	LocalDisk  = "local-disk"
)

const (
	HeaderFileName         = "X-File-Name"  // Final merged file name
	HeaderFileSize         = "X-File-Size"  // Chunk size
	HeaderChunkName        = "X-Chunk-Name" // Custom name for single chunk
	HeaderXHops            = "X-Hops"
	HeaderXChunkIndex      = "X-Chunk-Index"
	HeaderXRateLimitEnable = "X-Rate-Limit-Enable"
	HeaderDestType         = "X-Dest-Type"
)

type ChunkMergeRequest struct {
	FinalFileName string   `json:"final_file_name" binding:"required"` // Final merged file name (required)
	ChunkNames    []string `json:"chunk_names" binding:"required"`     // Chunk names list (ordered, required)
	DeleteChunks  bool     `json:"delete_chunks,omitempty"`            // Whether to delete chunks (optional, default true)
}
