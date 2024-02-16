package grid_test

import (
	"github.com/pkg/errors"

	"ktdb/pkg/data"
)

type TestByteColMock byte

func (c TestByteColMock) TypeName(_ int) string {
	return "col_mock"
}

func (c TestByteColMock) Marshal(size int) ([]byte, error) {
	if c == 0x00 {
		return nil, errors.New("error")
	}
	res := make([]byte, size)
	copy(res, []byte{byte(c)})
	return res, nil
}

func (c TestByteColMock) Unmarshal(_ int, payload []byte) (data.Column, error) {
	if payload[0] == 0x00 {
		return nil, errors.New("error")
	}

	return TestByteColMock(payload[0]), nil
}
