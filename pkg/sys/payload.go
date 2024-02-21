package sys

import (
	"errors"
)

func Size(payload []byte) (int, error) {
	if len(payload) < IntByteSize {
		return 0, errors.New("payload has no size defined")
	}

	return BytesAsInt(payload[:IntByteSize])
}

func Read(payload []byte) ([]byte, int, error) {
	size, err := Size(payload)
	if err != nil {
		return nil, 0, err
	}
	totalSize := IntByteSize + size
	if len(payload) < totalSize {
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
	size := len(bytes)
	res := make([]byte, IntByteSize+size)
	copy(res, IntAsBytes(size))
	copy(res[IntByteSize:], bytes)
	return res
}
