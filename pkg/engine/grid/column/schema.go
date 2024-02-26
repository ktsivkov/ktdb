package column

import (
	"fmt"
	"unicode/utf8"

	"github.com/pkg/errors"

	"ktdb/pkg/sys"
)

type Schema struct {
	Type     Type
	Name     string
	Default  []byte
	Size     int
	Nullable bool
}

func (s *Schema) ValidateColumn(col Column) error {
	if s.Nullable == false && col == nil {
		return errors.Errorf("%s is not nullable", s.logDescriptor())
	}
	if col != nil && col.Type() != s.Type {
		return errors.Errorf("%s unsupported value=[type=%s]", s.logDescriptor(), col.Type().String())
	}
	return nil
}

func (s *Schema) Bytes() ([]byte, error) {
	typeBytes := sys.New(s.Type.Bytes())
	defaultBytes := sys.New(s.Default)
	nameBytes := sys.New([]byte(s.Name))
	columnSizeBytes := sys.New(sys.IntAsBytes(s.Size))
	nullableByte := sys.New(sys.BoolAsBytes(s.Nullable))
	return sys.ConcatSlices(typeBytes, defaultBytes, nameBytes, columnSizeBytes, nullableByte), nil
}

func (s *Schema) Load(payload []byte) error {
	payloads, err := sys.ReadAll(payload)
	if err != nil {
		return errors.Wrap(err, "deserialization failed")
	}
	if len(payloads) != 5 { // The payload of the columnSchema persists of 5 different sections, one for each field
		return errors.New("corrupted payload")
	}
	s.Type, err = new(Type).Load(payloads[0])
	if err != nil {
		return errors.Wrap(err, "could not load type")
	}
	if utf8.Valid(payloads[2]) == false {
		return errors.Errorf("could not load name")
	}
	s.Name = string(payloads[2])
	s.Size, err = sys.BytesAsInt(payloads[3])
	if err != nil {
		return errors.Wrap(err, "could not load column size")
	}
	s.Nullable, err = sys.BytesAsBool(payloads[4])
	if err != nil {
		return errors.Wrap(err, "could not load Nullable")
	}
	s.Default = payloads[1]

	if err = s.validate(); err != nil {
		return errors.Wrap(err, "validation failed")
	}

	return nil
}

func (s *Schema) paddingSize() int {
	if s.Nullable {
		return 1
	}
	return 0
}

func (s *Schema) PayloadSize() int {
	if s.Nullable {
		return s.Size + s.paddingSize()
	}
	return s.Size
}

func (s *Schema) ColumnBytes(col Column) ([]byte, error) {
	var payload []byte
	if col != nil {
		var err error
		payload, err = col.Bytes(s.Size)
		if err != nil {
			return nil, errors.Wrapf(err, "%s could not get column bytes", s.logDescriptor())
		}
	}
	return s.pack(payload), nil
}

func (s *Schema) Column(processor Processor, payload []byte) (Column, error) {
	typeProcessor, err := processor.TypeProcessor(s.Type)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not load type processor", s.logDescriptor())
	}

	payload = s.unpack(payload)
	if payload == nil && s.Nullable {
		return nil, nil
	}

	if !s.Nullable && payload == nil {
		return nil, errors.Errorf("%s corrupted data", s.logDescriptor())
	}

	res, err := typeProcessor.Load(s.Size, payload)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not load column", s.logDescriptor())
	}

	return res, nil
}

// pack returns the payload with a nullable padding if needed
func (s *Schema) pack(payload []byte) []byte {
	bytes := make([]byte, s.PayloadSize())
	if payload != nil {
		if s.Nullable {
			bytes[0] = 0xFF
		}
		copy(bytes[s.paddingSize():], payload) // TODO check if explicitly starting from [0:] introduces some overhead
	}

	return bytes
}

// unpack returns the payload removing the nullable padding in the process if exists
func (s *Schema) unpack(payload []byte) []byte {
	if !s.Nullable {
		return payload
	}

	if payload[0] == 0x00 {
		return nil
	}
	return payload[1:]
}

func (s *Schema) logDescriptor() string {
	return fmt.Sprintf("(column=[name=%s, type=%s])", s.Name, s.Type.Format(s.Size))
}

func (s *Schema) validate() error {
	if s.Name == "" {
		return errors.Errorf("%s name cannot be empty", s.logDescriptor())
	}
	if s.Type.Empty() {
		return errors.Errorf("%s type cannot be empty", s.logDescriptor())
	}
	return nil
}
