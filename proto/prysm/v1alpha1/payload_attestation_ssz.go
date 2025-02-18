package eth

import (
	ssz "github.com/prysmaticlabs/fastssz"
	github_com_prysmaticlabs_prysm_v5_consensus_types_primitives "github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
)

// MarshalSSZ ssz marshals the PayloadAttestationData object
func (p *PayloadAttestationData) MarshalSSZ() ([]byte, error) {
	return ssz.MarshalSSZ(p)
}

// MarshalSSZTo ssz marshals the PayloadAttestationData object to a target array
func (p *PayloadAttestationData) MarshalSSZTo(buf []byte) (dst []byte, err error) {
	dst = buf

	// Field (0) 'BeaconBlockRoot'
	if size := len(p.BeaconBlockRoot); size != 32 {
		err = ssz.ErrBytesLengthFn("--.BeaconBlockRoot", size, 32)
		return
	}
	dst = append(dst, p.BeaconBlockRoot...)

	// Field (1) 'Slot'
	dst = ssz.MarshalUint64(dst, uint64(p.Slot))

	// Field (2) 'PayloadStatus'
	dst = ssz.MarshalUint8(dst, uint8(p.PayloadStatus))

	return
}

// UnmarshalSSZ ssz unmarshals the PayloadAttestationData object
func (p *PayloadAttestationData) UnmarshalSSZ(buf []byte) error {
	var err error
	size := uint64(len(buf))
	if size != 41 {
		return ssz.ErrSize
	}

	// Field (0) 'BeaconBlockRoot'
	if cap(p.BeaconBlockRoot) == 0 {
		p.BeaconBlockRoot = make([]byte, 0, len(buf[0:32]))
	}
	p.BeaconBlockRoot = append(p.BeaconBlockRoot, buf[0:32]...)

	// Field (1) 'Slot'
	p.Slot = github_com_prysmaticlabs_prysm_v5_consensus_types_primitives.Slot(ssz.UnmarshallUint64(buf[32:40]))

	// Field (2) 'PayloadStatus'
	p.PayloadStatus = github_com_prysmaticlabs_prysm_v5_consensus_types_primitives.PTCStatus(ssz.UnmarshallUint8(buf[40:41]))

	return err
}

// SizeSSZ returns the ssz encoded size in bytes for the PayloadAttestationData object
func (p *PayloadAttestationData) SizeSSZ() (size int) {
	size = 41
	return
}

// HashTreeRoot ssz hashes the PayloadAttestationData object
func (p *PayloadAttestationData) HashTreeRoot() ([32]byte, error) {
	return ssz.HashWithDefaultHasher(p)
}

// HashTreeRootWith ssz hashes the PayloadAttestationData object with a hasher
func (p *PayloadAttestationData) HashTreeRootWith(hh *ssz.Hasher) (err error) {
	indx := hh.Index()

	// Field (0) 'BeaconBlockRoot'
	if size := len(p.BeaconBlockRoot); size != 32 {
		err = ssz.ErrBytesLengthFn("--.BeaconBlockRoot", size, 32)
		return
	}
	hh.PutBytes(p.BeaconBlockRoot)

	// Field (1) 'Slot'
	hh.PutUint64(uint64(p.Slot))

	// Field (2) 'PayloadStatus'
	hh.PutUint8(uint8(p.PayloadStatus))

	hh.Merkleize(indx)
	return
}
