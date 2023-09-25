package base

import (
	"context"
	"errors"
	"fmt"
	"github.com/tomasliu-agora/rtm2"
	"go.uber.org/zap"
	"sync"
)

type storageSub struct {
	events      chan *rtm2.StorageEvent
	ctx         context.Context
	cancel      context.CancelFunc
	initialized chan interface{}
	majorRev    int64
	data        map[string]*rtm2.MetadataItem
}

type userSub struct {
	events      chan *rtm2.StorageEvent
	ctx         context.Context
	cancel      context.CancelFunc
	initialized chan interface{}
	majorRev    int64
	data        map[string]*rtm2.MetadataItem
}

type storage struct {
	client *client

	subscribes     sync.Map
	subscribeUsers sync.Map

	lg *zap.Logger
}

func unify(channelName string, channelType rtm2.ChannelType) string {
	if channelType == rtm2.ChannelTypeStream {
		return "*s*" + channelName
	} else {
		return channelName
	}
}

func (s *storage) unifyChannelKey(channel string, channelType rtm2.ChannelType) string {
	return fmt.Sprintf("Groups/%s", unify(channel, channelType))
}

func (s *storage) unifyUserKey(userId string) string {
	return fmt.Sprintf("Users/%s", userId)
}

func (s *storage) GetChannelMetadataChan(channel string, channelType rtm2.ChannelType) (map[string]*rtm2.MetadataItem, <-chan *rtm2.StorageEvent, error) {
	s.lg.Debug("GetChannelMetadataChan")
	switch channelType {
	case rtm2.ChannelTypeStream:
		if _, ok := s.client.streamChannels.Load(channel); !ok {
			return nil, nil, errors.New("not joined")
		}
	case rtm2.ChannelTypeMessage:
		if _, ok := s.client.messageChannels.Load(channel); !ok {
			return nil, nil, errors.New("not joined")
		}
	default:
		s.lg.Warn("unknown channel type", zap.Int32("channelType", int32(channelType)))
		return nil, nil, errors.New("unknown channel type")
	}

	key := s.unifyChannelKey(channel, channelType)
	if value, ok := s.subscribes.Load(key); !ok {
		s.lg.Warn("not subscribed", zap.String("key", key))
		return nil, nil, errors.New("not subscribed")
	} else {
		sub := value.(*storageSub)
		<-sub.initialized
		return sub.data, sub.events, nil
	}
}

func (r *StorageChannelReq) setItems(data map[string]*rtm2.MetadataItem) {
	r.Items = nil
	for _, item := range data {
		r.Items = append(r.Items, &MetadataItem{Key: item.Key, Value: item.Value, Author: item.Author, Rev: item.Revision, UpdateTs: item.UpdateTs})
	}
}

func (s *storage) SetChannelMetadata(channel string, channelType rtm2.ChannelType, data map[string]*rtm2.MetadataItem, opts ...rtm2.StorageOption) error {
	s.lg.Debug("SetChannelMetadata")
	options := &rtm2.StorageOptions{MajorRev: -1}
	for _, opt := range opts {
		opt(options)
	}

	req := &StorageChannelReq{Channel: channel, ChannelType: int32(channelType), OpType: MetadataOpType_SET, MajorRev: options.MajorRev, Lock: options.Lock, RecordTs: options.RecordTs, RecordUid: options.RecordAuthor}
	req.setItems(data)
	_, errCode, err := s.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (s *storage) UpdateChannelMetadata(channel string, channelType rtm2.ChannelType, data map[string]*rtm2.MetadataItem, opts ...rtm2.StorageOption) error {
	s.lg.Debug("UpdateChannelMetadata")
	options := &rtm2.StorageOptions{MajorRev: -1}
	for _, opt := range opts {
		opt(options)
	}

	req := &StorageChannelReq{Channel: channel, ChannelType: int32(channelType), OpType: MetadataOpType_UPDATE, MajorRev: options.MajorRev, Lock: options.Lock, RecordTs: options.RecordTs, RecordUid: options.RecordAuthor}
	req.setItems(data)
	_, errCode, err := s.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (s *storage) RemoveChannelMetadata(channel string, channelType rtm2.ChannelType, data map[string]*rtm2.MetadataItem, opts ...rtm2.StorageOption) error {
	s.lg.Debug("RemoveChannelMetadata")
	options := &rtm2.StorageOptions{MajorRev: -1}
	for _, opt := range opts {
		opt(options)
	}

	req := &StorageChannelReq{Channel: channel, ChannelType: int32(channelType), OpType: MetadataOpType_REMOVE, MajorRev: options.MajorRev, Lock: options.Lock, RecordTs: options.RecordTs, RecordUid: options.RecordAuthor}
	req.setItems(data)
	_, errCode, err := s.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (s *storage) GetChannelMetadata(channel string, channelType rtm2.ChannelType) (int64, map[string]*rtm2.MetadataItem, error) {
	s.lg.Debug("GetChannelMetadata")
	req := &StorageChannelGetReq{Channel: channel, ChannelType: int32(channelType)}
	resp, errCode, err := s.client.invoker.OnReceived(req)
	if err != nil {
		return 0, nil, err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return 0, nil, err
	}
	getResp := resp.(*StorageChannelGetResp)
	rst := make(map[string]*rtm2.MetadataItem)
	for _, item := range getResp.Items {
		rst[item.Key] = &rtm2.MetadataItem{Key: item.Key, Value: item.Value, Author: item.Author, Revision: item.Rev, UpdateTs: item.UpdateTs}
	}
	return getResp.MajorRev, rst, nil
}

func (r *StorageUserReq) setItems(data map[string]*rtm2.MetadataItem) {
	r.Items = nil
	for _, item := range data {
		r.Items = append(r.Items, &MetadataItem{Key: item.Key, Value: item.Value, Author: item.Author, Rev: item.Revision, UpdateTs: item.UpdateTs})
	}
}

func (s *storage) SetUserMetadata(userId string, data map[string]*rtm2.MetadataItem, opts ...rtm2.StorageOption) error {
	s.lg.Debug("SetUserMetadata")
	options := &rtm2.StorageOptions{MajorRev: -1}
	for _, opt := range opts {
		opt(options)
	}

	req := &StorageUserReq{UserId: userId, OpType: MetadataOpType_SET, MajorRev: options.MajorRev, RecordTs: options.RecordTs, RecordUid: options.RecordAuthor}
	req.setItems(data)
	_, errCode, err := s.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (s *storage) UpdateUserMetadata(userId string, data map[string]*rtm2.MetadataItem, opts ...rtm2.StorageOption) error {
	s.lg.Debug("UpdateUserMetadata")
	options := &rtm2.StorageOptions{MajorRev: -1}
	for _, opt := range opts {
		opt(options)
	}

	req := &StorageUserReq{UserId: userId, OpType: MetadataOpType_UPDATE, MajorRev: options.MajorRev, RecordTs: options.RecordTs, RecordUid: options.RecordAuthor}
	req.setItems(data)
	_, errCode, err := s.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (s *storage) RemoveUserMetadata(userId string, data map[string]*rtm2.MetadataItem, opts ...rtm2.StorageOption) error {
	s.lg.Debug("RemoveUserMetadata")
	options := &rtm2.StorageOptions{MajorRev: -1}
	for _, opt := range opts {
		opt(options)
	}
	req := &StorageUserReq{UserId: userId, OpType: MetadataOpType_REMOVE, MajorRev: options.MajorRev, RecordTs: options.RecordTs, RecordUid: options.RecordAuthor}
	req.setItems(data)
	_, errCode, err := s.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (s *storage) GetUserMetadata(userId string) (int64, map[string]*rtm2.MetadataItem, error) {
	s.lg.Debug("GetUserMetadata")
	req := &StorageUserGetReq{UserId: userId}
	resp, errCode, err := s.client.invoker.OnReceived(req)
	if err != nil {
		return 0, nil, err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return 0, nil, err
	}
	getResp := resp.(*StorageUserGetResp)
	rst := make(map[string]*rtm2.MetadataItem)
	for _, item := range getResp.Items {
		rst[item.Key] = &rtm2.MetadataItem{Key: item.Key, Value: item.Value, Author: item.Author, Revision: item.Rev, UpdateTs: item.UpdateTs}
	}
	return getResp.MajorRev, rst, nil
}

func (s *storage) SubscribeUserMetadata(userId string) (map[string]*rtm2.MetadataItem, <-chan *rtm2.StorageEvent, error) {
	s.lg.Debug("SubscribeUserMetadata")
	sub := &userSub{events: make(chan *rtm2.StorageEvent, defaultChanSize), initialized: make(chan interface{}, 1), data: make(map[string]*rtm2.MetadataItem)}
	if value, ok := s.subscribeUsers.LoadOrStore(userId, sub); !ok {
		req := &StorageUserSubReq{UserId: userId}
		_, errCode, err := s.client.invoker.OnReceived(req)
		if err != nil {
			s.subscribeUsers.Delete(userId)
			return nil, nil, err
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			s.subscribeUsers.Delete(userId)
			return nil, nil, err
		}
		<-sub.initialized
		return sub.data, sub.events, nil
	} else {
		origin := value.(*userSub)
		return origin.data, origin.events, nil
	}
}

func (s *storage) UnsubscribeUserMetadata(userId string) error {
	s.lg.Debug("UnsubscribeUserMetadata")
	if value, ok := s.subscribeUsers.LoadAndDelete(userId); ok {
		sub := value.(*userSub)
		sub.cancel()
		s.lg.Debug("stop subscribe user", zap.String("userId", userId))
	} else {
		s.lg.Warn("not subscribed user", zap.String("userId", userId))
	}
	return nil
}

func (s *storage) subscribe(key string) {
	s.lg.Debug("subscribe")
	ctx, cancel := context.WithCancel(s.client.ctx)
	sub := &storageSub{events: make(chan *rtm2.StorageEvent, defaultChanSize), data: make(map[string]*rtm2.MetadataItem), initialized: make(chan interface{}, 1), ctx: ctx, cancel: cancel}
	if _, ok := s.subscribes.LoadOrStore(key, sub); ok {
		s.lg.Warn("already subscribe meta", zap.String("key", key))
	} else {
		s.lg.Info("subscribe meta", zap.String("key", key))
	}
}

func (s *storage) unsubscribe(key string) {
	s.lg.Debug("unsubscribe")
	if value, ok := s.subscribes.LoadAndDelete(key); ok {
		sub := value.(*storageSub)
		sub.cancel()
		s.lg.Debug("stop subscribe", zap.String("key", key))
	} else {
		s.lg.Warn("not subscribed", zap.String("key", key))
	}
}

func (e *StorageChannelEvent) toRTM2Event() *rtm2.StorageEvent {
	rst := &rtm2.StorageEvent{Items: make(map[string]*rtm2.MetadataItem)}
	rst.MajorRevision = e.MajorRev
	rst.EventType = rtm2.StorageEventType(e.EventType)
	for _, item := range e.Items {
		rst.Items[item.Key] = &rtm2.MetadataItem{Key: item.Key, Value: item.Value, Author: item.Author, Revision: item.Rev, UpdateTs: item.UpdateTs}
	}
	return rst
}

func (e *StorageUserEvent) toRTM2Event() *rtm2.StorageEvent {
	rst := &rtm2.StorageEvent{Items: make(map[string]*rtm2.MetadataItem)}
	rst.MajorRevision = e.MajorRev
	rst.EventType = rtm2.StorageEventType(e.EventType)
	for _, item := range e.Items {
		rst.Items[item.Key] = &rtm2.MetadataItem{Key: item.Key, Value: item.Value, Author: item.Author, Revision: item.Rev, UpdateTs: item.UpdateTs}
	}
	return rst
}

func (s *storage) onChannelEvent(e *StorageChannelEvent) {
	s.lg.Debug("on channel Event", zap.Any("event", e))
	key := s.unifyChannelKey(e.Channel, rtm2.ChannelType(e.ChannelType))
	if value, ok := s.subscribes.Load(key); ok {
		sub := value.(*storageSub)
		select {
		case <-sub.initialized:
			sub.events <- e.toRTM2Event()
			s.lg.Debug("put to channel", zap.Any("event", e))
		default:
			sub.majorRev = e.MajorRev
			for _, item := range e.Items {
				sub.data[item.Key] = &rtm2.MetadataItem{Key: item.Key, Value: item.Value, Author: item.Author, Revision: item.Rev, UpdateTs: item.UpdateTs}
			}
			close(sub.initialized)
			s.lg.Debug("store to snapshot and close channel", zap.Any("event", e))
		}
	} else {
		s.lg.Warn("unknown channel", zap.String("channel", e.Channel))
	}
}

func (s *storage) onUserEvent(e *StorageUserEvent) {
	s.lg.Debug("on user Event", zap.Any("event", e))
	if value, ok := s.subscribeUsers.Load(e.UserId); ok {
		sub := value.(*userSub)
		select {
		case <-sub.initialized:
			sub.events <- e.toRTM2Event()
			s.lg.Debug("put to channel", zap.Any("event", e))
		default:
			sub.majorRev = e.MajorRev
			for _, item := range e.Items {
				sub.data[item.Key] = &rtm2.MetadataItem{Key: item.Key, Value: item.Value, Author: item.Author, Revision: item.Rev, UpdateTs: item.UpdateTs}
			}
			close(sub.initialized)
			s.lg.Debug("store to snapshot and close channel", zap.Any("event", e))
		}
	} else {
		s.lg.Warn("unknown userId", zap.String("userId", e.UserId))
	}
}

func createStorage(cli *client) *storage {
	return &storage{client: cli, lg: cli.lg}
}
