package structure

import "context"

type Processor[T any] interface {
	List(ctx context.Context) ([]T, error)
	Create(ctx context.Context, name string) (T, error)
	Get(ctx context.Context, name string) (T, error)
	Delete(ctx context.Context) error
}
