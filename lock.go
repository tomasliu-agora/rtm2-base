package base

import (
	"context"
	"errors"
	"fmt"
	"github.com/tomasliu-agora/rtm2"
	"go.uber.org/zap"
	"sync"
)

type lockSub struct {
	ctx         context.Context
	cancel      context.CancelFunc
	initialized chan interface{}
	data        map[string]*rtm2.LockDetail
	events      chan *rtm2.LockEvent
}

type lock struct {
	client *client
	lg     *zap.Logger

	acquires   sync.Map
	subscribes sync.Map
}

func (l *lock) unifyLockKey(channel string, channelType rtm2.ChannelType, lock string) string {
	return fmt.Sprintf("Groups/%s/Users/%s", unify(channel, channelType), lock)
}

func (l *lock) GetLockChan(channel string, channelType rtm2.ChannelType) (map[string]*rtm2.LockDetail, <-chan *rtm2.LockEvent, error) {
	l.lg.Debug("GetLockChan")
	key := unify(channel, channelType)
	if value, ok := l.subscribes.Load(key); !ok {
		l.lg.Warn("not subscribed", zap.String("key", key))
		return nil, nil, errors.New("not subscribed")
	} else {
		sub := value.(*lockSub)
		<-sub.initialized
		return sub.data, sub.events, nil
	}
}

func (l *lock) Set(channel string, channelType rtm2.ChannelType, name string, ttl uint32) error {
	l.lg.Debug("lock set", zap.String("channel", channel), zap.Int("type", int(channelType)), zap.String("name", name), zap.Uint32("ttl", ttl))
	req := &LockSetReq{Channel: channel, ChannelType: int32(channelType), Lock: name, Ttl: int32(ttl)}
	_, errCode, err := l.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (l *lock) Get(channel string, channelType rtm2.ChannelType) (map[string]*rtm2.LockDetail, error) {
	l.lg.Debug("lock get", zap.String("channel", channel), zap.Int("type", int(channelType)))
	req := &LockGetReq{Channel: channel, ChannelType: int32(channelType)}
	resp, errCode, err := l.client.invoker.OnReceived(req)
	if err != nil {
		return nil, err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return nil, err
	}
	getResp := resp.(*LockGetResp)
	rst := make(map[string]*rtm2.LockDetail)
	for _, detail := range getResp.Locks {
		rst[detail.Lock] = &rtm2.LockDetail{Name: detail.Lock, Owner: detail.Owner, TTL: uint32(detail.Ttl)}
	}
	return rst, nil
}

func (l *lock) Remove(channel string, channelType rtm2.ChannelType, name string) error {
	l.lg.Debug("lock remove", zap.String("channel", channel), zap.Int("type", int(channelType)), zap.String("name", name))
	req := &LockRemoveReq{Channel: channel, ChannelType: int32(channelType), Lock: name}
	_, errCode, err := l.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (l *lock) Acquire(channel string, channelType rtm2.ChannelType, name string, retry bool) <-chan error {
	l.lg.Debug("lock acquire", zap.String("channel", channel), zap.Int("type", int(channelType)), zap.String("name", name), zap.Bool("retry", retry))
	rst := make(chan error, 1)
	key := l.unifyLockKey(channel, channelType, name)
	req := &LockAcquireReq{Channel: channel, ChannelType: int32(channelType), Lock: name, Retry: retry}
	if retry {
		if value, ok := l.acquires.LoadOrStore(key, rst); ok {
			origin := value.(chan error)
			return origin
		} else {
			err := l.client.invoker.OnAsyncReceived(req, func(_ interface{}, errCode int32, err error) {
				defer l.acquires.Delete(key)
				l.lg.Debug("on async received", zap.Int32("errCode", errCode), zap.Error(err))
				if err != nil {
					rst <- err
				} else if err = rtm2.ErrorFromCode(errCode); err != nil {
					rst <- err
				} else {
					rst <- nil
				}
			})
			if err != nil {
				rst <- err
				return rst
			}
		}
	} else {
		_, errCode, err := l.client.invoker.OnReceived(req)
		if err != nil {
			rst <- err
			l.acquires.Delete(key)
			return rst
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			rst <- err
			l.acquires.Delete(key)
			return rst
		}
		rst <- nil
	}
	return rst
}

func (l *lock) Release(channel string, channelType rtm2.ChannelType, name string) error {
	l.lg.Debug("lock release", zap.String("channel", channel), zap.Int("type", int(channelType)), zap.String("name", name))
	req := &LockReleaseReq{Channel: channel, ChannelType: int32(channelType), Lock: name}
	_, errCode, err := l.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	// TODO lock acquire.
	return nil
}

func (l *lock) Revoke(channel string, channelType rtm2.ChannelType, name string, owner string) error {
	l.lg.Debug("lock revoke", zap.String("channel", channel), zap.Int("type", int(channelType)), zap.String("name", name), zap.String("owner", owner))
	req := &LockRevokeReq{Channel: channel, ChannelType: int32(channelType), Lock: name, Owner: owner}
	_, errCode, err := l.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (l *lock) subscribe(key string) {
	l.lg.Debug("subscribe")
	ctx, cancel := context.WithCancel(l.client.ctx)
	sub := &lockSub{ctx: ctx, cancel: cancel, data: make(map[string]*rtm2.LockDetail), initialized: make(chan interface{}, 1), events: make(chan *rtm2.LockEvent, defaultChanSize)}
	if _, loaded := l.subscribes.LoadOrStore(key, sub); loaded {
		l.lg.Warn("subscribe duplicated", zap.String("key", key))
	} else {
		l.lg.Debug("subscribe lock", zap.String("key", key))
	}
}

func (l *lock) unsubscribe(key string) {
	l.lg.Debug("unsubscribe")
	if value, loaded := l.subscribes.LoadAndDelete(key); !loaded {
		l.lg.Warn("not subscribed", zap.String("key", key))
	} else {
		sub := value.(*lockSub)
		sub.cancel()
		l.lg.Debug("stop subscribe", zap.String("key", key))
	}
}

func (e *LockEvent) toRTM2Event() *rtm2.LockEvent {
	rst := &rtm2.LockEvent{}
	rst.Type = rtm2.LockEventType(e.EventType)
	for _, detail := range e.Locks {
		rst.Details = append(rst.Details, &rtm2.LockDetail{Name: detail.Lock, Owner: detail.Owner, TTL: uint32(detail.Ttl)})
	}
	return rst
}

func (l *lock) onEvent(event *LockEvent) {
	l.lg.Debug("on Event", zap.Any("event", event))

	key := unify(event.Channel, rtm2.ChannelType(event.ChannelType))
	if value, ok := l.subscribes.Load(key); ok {
		sub := value.(*lockSub)
		e := event.toRTM2Event()
		select {
		case <-sub.initialized:
			sub.events <- e
			l.lg.Debug("put to channel", zap.Any("event", event))
		default:
			if e.Type != rtm2.LockTypeSnapshot {
				l.lg.Warn("not snapshot for the first event")
				return
			}
			for _, detail := range e.Details {
				sub.data[detail.Name] = detail
			}
			close(sub.initialized)
			l.lg.Debug("store to snapshot and close channel", zap.Any("event", event))
		}
	} else {
		l.lg.Warn("unknown channel", zap.String("channel", event.Channel))
	}
}

func createLock(cli *client) *lock {
	return &lock{client: cli, lg: cli.lg}
}
