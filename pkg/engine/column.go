package engine

type Column interface {
	TypeIdentifier() string
	Type(size int) string
	Marshal(size int) ([]byte, error)
	Unmarshal(size int, payload []byte) (Column, error)
}
