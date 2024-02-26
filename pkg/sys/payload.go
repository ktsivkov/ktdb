package sys

import (
	"errors"
)

func Size(payload []byte) (int64, error) {
	if int64(len(payload)) < IntByteSize {
		return 0, errors.New("payload has no size defined")
	}

	return BytesAsInt64(payload[:IntByteSize])
}

func Read(payload []byte) ([]byte, int64, error) {
	size, err := Size(payload)
	if err != nil {
		return nil, 0, err
	}
	totalSize := IntByteSize + size
	if int64(len(payload)) < totalSize {
		return nil, 0, errors.New("size of payload is larger than the payload itself")
	}
	return payload[IntByteSize:totalSize], totalSize, nil
}

func ReadAll(payload []byte) ([][]byte, error) {
	res := make([][]byte, 0)
	for len(payload) != 0 {
		bytes, consumed, err := Read(payload)
		if err != nil {
			return nil, err
		}
		payload = payload[consumed:]
		res = append(res, bytes)
	}

	return res, nil
}

func New(bytes []byte) []byte {
	size := int64(len(bytes))
	res := make([]byte, IntByteSize+size)
	copy(res, Int64AsBytes(size))
	copy(res[IntByteSize:], bytes)
	return res
}
