package broker

import (
	"encoding/json"
	"fmt"
	"github.com/cockroachdb/pebble"
	"log"
)

type SubscriptionStore interface {
	listAllSubscriptions() []*Subscription

	addNewSubscription(subscription Subscription) bool

	removeSubscription(topic, clientID string) bool
}


func NewPebbleSubscriptionStore(dirName string) SubscriptionStore {
	db, err := pebble.Open(dirName, nil)
	if err != nil {
		fmt.Println("open da failed")
		return nil
	}

	subStore := &pebbleSubscriptionStore{
		SubStoreDir: dirName,
		dataBase:    db,
	}

	return subStore
}

// key-topic+client,value-Subscription
type pebbleSubscriptionStore struct {
	SubStoreDir string // 存储订阅数据的目录
	dataBase    *pebble.DB
}

func (pss *pebbleSubscriptionStore) listAllSubscriptions() []*Subscription {
	iter := pss.dataBase.NewIter(nil)

	var allSubscription []*Subscription
	for iter.First(); iter.Valid(); iter.Next() {
		val := iter.Value()
		sub := &Subscription{}
		err := json.Unmarshal(val, sub)
		if err != nil {
			fmt.Printf("unmarshal val filed %s\n", err)
			continue
		}

		allSubscription = append(allSubscription, sub)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
	return allSubscription
}

func (pss *pebbleSubscriptionStore) addNewSubscription(subscription Subscription) bool {
	for _, topic := range subscription.Topics {
		key := topic.Topic + "-" + string(subscription.ClientId)
		val, err := json.Marshal(subscription)
		if err != nil {
			fmt.Printf("marshal value failed %s", err)
			return false
		}
		err = pss.dataBase.Set([]byte(key), val, pebble.Sync)
		if err != nil {
			fmt.Printf("save to disk failed %s\n", err)
			return false
		}
	}
	return true
}

func (pss *pebbleSubscriptionStore) removeSubscription(topic, clientID string) bool {
	err := pss.dataBase.Delete([]byte(topic+"-"+clientID), pebble.Sync)
	if err != nil {
		fmt.Printf("delete topic failed %s\n", err)
		return false
	}
	return true
}
