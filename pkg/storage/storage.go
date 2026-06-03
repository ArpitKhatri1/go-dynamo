package storage

import "sync"

type Storage struct {
	storeLock  sync.RWMutex
	storageMap map[int]StorageItem // efficient O(1) lookup

	Handoffstorage *HandoffStorage
}

type HandoffStorage struct {
	handoffLock sync.RWMutex
	items       []HandoffItem
}

type HandoffItem struct {
	belongsToServer int //serverId
	item            StorageItem
}

type StorageItem struct {
	key   int
	value int
	//vector clock addition in the future
}

func CreateNewEmptyHandoffStorage() *HandoffStorage {
	return &HandoffStorage{
		items: []HandoffItem{},
	}
}

func CreateNewEmptyStorage() *Storage {
	handoffStorage := CreateNewEmptyHandoffStorage()
	return &Storage{
		storageMap:     make(map[int]StorageItem),
		Handoffstorage: handoffStorage,
	}
}

func (s *Storage) PutKey(key int, value int) StorageItem {
	// accquire lock , check if key exist , if already exist update it , if not store new key
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	item := StorageItem{key: key, value: value}

	s.storageMap[key] = item

	return item

}

func (s *Storage) GetKey(key int) int {
	s.storeLock.RLock()
	defer s.storeLock.Unlock()

	return s.storageMap[key].value
}

func (s *Storage) AddHandoffItem(intendendServer int, key int, value int) HandoffItem {
	s.Handoffstorage.handoffLock.Lock()
	defer s.Handoffstorage.handoffLock.Unlock()

	handoffItem := HandoffItem{
		belongsToServer: intendendServer,
		item: StorageItem{
			key:   key,
			value: value,
		},
	}

	s.Handoffstorage.items = append(s.Handoffstorage.items, handoffItem)
	return handoffItem
}
