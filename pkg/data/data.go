package data

import "reflect"

type Row []byte

type TypeLoaderFunc func(identifier string) (reflect.Type, error)
