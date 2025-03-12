package filesystem

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	ssz "github.com/prysmaticlabs/fastssz"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/blockchain/kzg"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/db"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/verification"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/blocks"
	ethpb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/prysmaticlabs/prysm/v5/testing/require"
	"github.com/prysmaticlabs/prysm/v5/testing/util"
	"github.com/spf13/afero"
)

func TestBlobStorage_DataColumn_WithAllLayouts(t *testing.T) {
	for _, layout := range LayoutNames {
		t.Run(fmt.Sprintf("layout=%s", layout), func(t *testing.T) {
			sidecars := setupDataColumnTest(t)

			t.Run("no error for duplicate", func(t *testing.T) {
				fs, bs := NewEphemeralBlobStorageAndFs(t, WithLayout(layout))
				sidecar := sidecars[0]

				columnPath := bs.layout.sszPath(identForDataColumnSidecar(sidecar))
				data, err := ssz.MarshalSSZ(sidecar)
				require.NoError(t, err)

				require.NoError(t, bs.SaveDataColumn(sidecar))
				// No error when attempting to write twice.
				require.NoError(t, bs.SaveDataColumn(sidecar))

				content, err := afero.ReadFile(fs, columnPath)
				require.NoError(t, err)
				require.Equal(t, true, bytes.Equal(data, content))

				// Deserialize the DataColumnSidecar from the saved file data.
				saved := &ethpb.DataColumnSidecar{}
				err = saved.UnmarshalSSZ(content)
				require.NoError(t, err)

				// Compare the original Sidecar and the saved Sidecar.
				require.DeepSSZEqual(t, sidecar.DataColumnSidecar, saved)
			})

			t.Run("indices", func(t *testing.T) {
				bs := NewEphemeralBlobStorage(t, WithLayout(layout))
				sidecar := sidecars[2]
				require.NoError(t, bs.SaveDataColumn(sidecar))
				actual, err := bs.GetColumn(sidecar.BlockRoot(), sidecar.ColumnIndex)
				require.NoError(t, err)
				require.DeepEqual(t, sidecar, actual)

				expectedIdx := make(dataIndexMask, params.BeaconConfig().NumberOfColumns)
				expectedIdx[2] = true
				actualIdx := bs.Summary(actual.BlockRoot()).mask
				require.NoError(t, err)
				require.DeepEqual(t, expectedIdx, actualIdx)

				sidecar = sidecars[10]
				expectedIdx[10] = true
				require.NoError(t, bs.SaveDataColumn(sidecar))
				actual, err = bs.GetColumn(sidecar.BlockRoot(), sidecar.ColumnIndex)
				require.NoError(t, err)
				require.DeepEqual(t, sidecar, actual)
				actualIdx = bs.Summary(actual.BlockRoot()).mask
				require.NoError(t, err)
				require.DeepEqual(t, expectedIdx, actualIdx)
			})

			t.Run("write -> read -> delete", func(t *testing.T) {
				bs := NewEphemeralBlobStorage(t, WithLayout(layout))
				err := bs.SaveDataColumn(sidecars[0])
				require.NoError(t, err)

				expected := sidecars[0]
				actual, err := bs.GetColumn(expected.BlockRoot(), expected.ColumnIndex)
				require.NoError(t, err)
				require.DeepEqual(t, expected, actual)

				require.NoError(t, bs.Remove(expected.BlockRoot()))
				for i := range params.BeaconConfig().NumberOfColumns {
					_, err = bs.GetColumn(expected.BlockRoot(), uint64(i))
					require.Equal(t, true, db.IsNotFound(err))
				}
			})

			t.Run("clear", func(t *testing.T) {
				bs := NewEphemeralBlobStorage(t, WithLayout(layout))
				err := bs.SaveDataColumn(sidecars[0])
				require.NoError(t, err)
				res, err := bs.GetColumn(sidecars[0].BlockRoot(), sidecars[0].ColumnIndex)
				require.NoError(t, err)
				require.NotNil(t, res)
				require.NoError(t, bs.Clear())
				// After clearing, the blob should not exist in the db.
				_, err = bs.GetColumn(sidecars[0].BlockRoot(), sidecars[0].ColumnIndex)
				require.ErrorIs(t, err, os.ErrNotExist)
			})
		})
	}
}

func TestBlobStorage_DataColumn_WithMigrationFromFlatToByEpoch(t *testing.T) {
	sidecars := setupDataColumnTest(t)

	// Setup flat layout
	fs, bs := NewEphemeralBlobStorageAndFs(t, WithLayout(LayoutNameFlat))
	sidecar := sidecars[0]
	columnPath := bs.layout.sszPath(identForDataColumnSidecar(sidecar))
	data, err := ssz.MarshalSSZ(sidecar)
	require.NoError(t, err)
	require.NoError(t, bs.SaveDataColumn(sidecar))
	content, err := afero.ReadFile(fs, columnPath)
	require.NoError(t, err)
	require.Equal(t, true, bytes.Equal(data, content))

	// Setup by-epoch layout
	bs = NewWarmedEphemeralBlobStorageUsingFs(t, fs, WithLayout(LayoutNameByEpoch))

	// Verify data is the same
	columnPath = bs.layout.sszPath(identForDataColumnSidecar(sidecar))
	content, err = afero.ReadFile(fs, columnPath)
	require.NoError(t, err)
	require.Equal(t, true, bytes.Equal(data, content))
}

func TestBlobStorage_DataColumn_WithMigrationFromByEpochToFlat(t *testing.T) {
	sidecars := setupDataColumnTest(t)

	// Setup by-epoch layout
	fs, bs := NewEphemeralBlobStorageAndFs(t, WithLayout(LayoutNameFlat))
	for _, sidecar := range sidecars {
		require.NoError(t, bs.SaveDataColumn(sidecar))
	}
	columnPath := bs.layout.sszPath(identForDataColumnSidecar(sidecars[0]))
	content, err := afero.ReadFile(fs, columnPath)
	require.NoError(t, err)
	data, err := ssz.MarshalSSZ(sidecars[0])
	require.NoError(t, err)
	require.Equal(t, true, bytes.Equal(data, content))

	// Setup flat layout
	bs = NewWarmedEphemeralBlobStorageUsingFs(t, fs, WithLayout(LayoutNameByEpoch))

	// Verify data is the same
	columnPath = bs.layout.sszPath(identForDataColumnSidecar(sidecars[0]))
	content, err = afero.ReadFile(fs, columnPath)
	require.NoError(t, err)
	require.Equal(t, true, bytes.Equal(data, content))
}

func setupDataColumnTest(t *testing.T) []blocks.VerifiedRODataColumn {
	// load trusted setup
	err := kzg.Start()
	require.NoError(t, err)

	// Setup right fork epoch
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	params.OverrideBeaconConfig(cfg)

	_, scs := util.GenerateTestFuluBlockWithSidecar(t, [32]byte{}, 0, 1)
	return verification.FakeVerifyDataColumnSliceForTest(t, scs)
}
