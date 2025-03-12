package filesystem

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/verification"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/blocks"
	"github.com/spf13/afero"
)

// SaveDataColumn saves dataColumns given a list of sidecars.
func (bs *BlobStorage) SaveDataColumn(verifiedRODataColumn blocks.VerifiedRODataColumn) error {
	startTime := time.Now()

	ident := identForDataColumnSidecar(verifiedRODataColumn)
	sszPath := bs.layout.sszPath(ident)
	exists, err := afero.Exists(bs.fs, sszPath)
	if err != nil {
		return errors.Wrap(err, "afero exists")
	}

	if exists {
		return nil
	}

	partialMoved := false
	partPath, err := bs.writeDataColumnPart(verifiedRODataColumn)

	// Ensure the partial file is deleted.
	defer func() {
		if partialMoved || partPath == "" {
			return
		}

		// It's expected to error if the save is successful.
		if err := bs.fs.Remove(partPath); err == nil {
			log.WithField("partPath", partPath).Debug("Removed partial file")
		}
	}()

	if err != nil {
		return err
	}

	// Atomically rename the partial file to its final name.
	err = bs.fs.Rename(partPath, sszPath)
	if err != nil {
		return errors.Wrap(err, "rename")
	}
	partialMoved = true

	if err := bs.layout.notify(ident); err != nil {
		return errors.Wrapf(err, "problem maintaining pruning cache/metrics for sidecar with root=%#x", verifiedRODataColumn.BlockRoot())
	}

	// Notify the data column notifier that a new data column has been saved.
	if bs.DataColumnFeed != nil {
		bs.DataColumnFeed.Send(RootIndexPair{
			Root:  verifiedRODataColumn.BlockRoot(),
			Index: verifiedRODataColumn.ColumnIndex,
		})
	}

	blobsWrittenCounter.Inc()
	blobSaveLatency.Observe(float64(time.Since(startTime).Milliseconds()))

	return nil
}

// GetColumn retrieves a single DataColumnSidecar by its root and index.
// Since BlobStorage only writes blobs that have undergone full verification, the return
// value is always a VerifiedRODataColumn.
func (bs *BlobStorage) GetColumn(root [32]byte, idx uint64) (blocks.VerifiedRODataColumn, error) {
	startTime := time.Now()

	ident, err := bs.layout.ident(root, idx)
	if err != nil {
		return verification.VerifiedRODataColumnError(err)
	}

	defer func() {
		blobFetchLatency.Observe(float64(time.Since(startTime).Milliseconds()))
	}()

	return verification.VerifiedRODataColumnFromDisk(bs.fs, root, bs.layout.sszPath(ident))
}

func (bs *BlobStorage) writeDataColumnPart(sidecar blocks.VerifiedRODataColumn) (ppath string, err error) {
	ident := identForDataColumnSidecar(sidecar)
	sidecarData, err := sidecar.MarshalSSZ()
	if err != nil {
		return "", errors.Wrap(err, "failed to serialize sidecar data")
	}
	if len(sidecarData) == 0 {
		return "", errSidecarEmptySSZData
	}

	if err := bs.fs.MkdirAll(bs.layout.dir(ident), directoryPermissions()); err != nil {
		return "", err
	}
	ppath = bs.layout.partPath(ident, fmt.Sprintf("%p", sidecarData))

	// Create a partial file and write the serialized data to it.
	partialFile, err := bs.fs.Create(ppath)
	if err != nil {
		return "", errors.Wrap(err, "failed to create partial file")
	}
	defer func() {
		cerr := partialFile.Close()
		// The close error is probably less important than any existing error, so only overwrite nil err.
		if cerr != nil && err == nil {
			err = cerr
		}
	}()

	n, err := partialFile.Write(sidecarData)
	if err != nil {
		return ppath, errors.Wrap(err, "failed to write to partial file")
	}
	if bs.fsync {
		if err := partialFile.Sync(); err != nil {
			return ppath, err
		}
	}

	if n != len(sidecarData) {
		return ppath, fmt.Errorf("failed to write the full bytes of sidecarData, wrote only %d of %d bytes", n, len(sidecarData))
	}

	return ppath, nil
}
