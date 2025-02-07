package art

import (
	goart "github.com/plar/go-adaptive-radix-tree"

	"github.com/flower-corp/rosedb/server"
)

type RadixTreeInterface interface {
	Put(key []byte, value interface{}) (oldVal interface{}, updated bool)
	Get(key []byte) interface{}
	Delete(key []byte) (val interface{}, updated bool)
	Iterator() goart.Iterator
	PrefixScan(prefix []byte, count int) (keys [][]byte)
	Size() int
	SetDataType(dType server.DataType) *AdaptiveRadixTree
	DataType() server.DataType
}

type AdaptiveRadixTree struct {
	tree goart.Tree
	dataType server.DataType
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, value interface{}) (oldVal interface{}, updated bool) {
	return art.tree.Insert(key, value)
}

func (art *AdaptiveRadixTree) Get(key []byte) interface{} {
	value, _ := art.tree.Search(key)
	return value
}

func (art *AdaptiveRadixTree) Delete(key []byte) (val interface{}, updated bool) {
	return art.tree.Delete(key)
}

func (art *AdaptiveRadixTree) Iterator() goart.Iterator {
	return art.tree.Iterator()
}

func (art *AdaptiveRadixTree) PrefixScan(prefix []byte, count int) (keys [][]byte) {
	cb := func(node goart.Node) bool {
		if node.Kind() != goart.Leaf {
			return true
		}
		if count <= 0 {
			return false
		}
		keys = append(keys, node.Key())
		count--
		return true
	}

	if len(prefix) == 0 {
		art.tree.ForEach(cb)
	} else {
		art.tree.ForEachPrefix(prefix, cb)
	}
	return
}

func (art *AdaptiveRadixTree) Size() int {
	return art.tree.Size()
}

func (art *AdaptiveRadixTree) SetDataType(dType server.DataType) *AdaptiveRadixTree {
	art.dataType = dType
	return art
}

func (art *AdaptiveRadixTree) DataType() server.DataType {
	return art.dataType
}

//tree with expire timeStamp
type AdaptiveRadixTreeExpire struct {
	AdaptiveRadixTree
	ExpireAt int64
}

func NewARTExpire() *AdaptiveRadixTreeExpire {
	return &AdaptiveRadixTreeExpire{
		AdaptiveRadixTree: *NewART(),
	}
}
