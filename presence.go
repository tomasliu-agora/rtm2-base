package base

import (
	"context"
	"errors"
	"fmt"
	"github.com/tomasliu-agora/rtm2"
	"go.uber.org/zap"
	"sync"
)

type presenceSub struct {
	ctx         context.Context
	cancel      context.CancelFunc
	initialized chan interface{}
	data        map[string]*rtm2.UserState
	events      chan *rtm2.PresenceEvent
}

type presence struct {
	client     *client
	subscribes sync.Map
	lg         *zap.Logger
}

func (p *presence) unifyChannelKey(channel string, channelType rtm2.ChannelType) string {
	return fmt.Sprintf("Groups/%s", unify(channel, channelType))
}

func (p *presence) GetPresenceChan(channel string, channelType rtm2.ChannelType) (map[string]*rtm2.UserState, <-chan *rtm2.PresenceEvent, error) {
	p.lg.Debug("GetPresenceChan")
	key := p.unifyChannelKey(channel, channelType)
	if value, ok := p.subscribes.Load(key); !ok {
		p.lg.Warn("not subscribed", zap.String("key", key))
		return nil, nil, errors.New("not subscribed")
	} else {
		sub := value.(*presenceSub)
		<-sub.initialized
		return sub.data, sub.events, nil
	}
}

func (p *presence) WhoNow(channel string, channelType rtm2.ChannelType, opts ...rtm2.PresenceOption) (map[string]*rtm2.UserState, string, error) {
	p.lg.Debug("WhoNow")
	options := &rtm2.PresenceOptions{}
	for _, opt := range opts {
		opt(options)
	}

	req := &PresenceWhoNowReq{Channel: channel, ChannelType: int32(channelType), WithState: options.State, WithUserId: options.UserId, Page: options.Page}
	resp, errCode, err := p.client.invoker.OnReceived(req)
	if err != nil {
		return nil, "", err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return nil, "", err
	}
	whoNowResp := resp.(*PresenceWhoNowResp)
	rst := make(map[string]*rtm2.UserState)
	for _, user := range whoNowResp.Users {
		us := &rtm2.UserState{UserId: user.UserId, State: make(map[string]string)}
		for _, item := range user.Items {
			us.State[item.Key] = item.Value
		}
		rst[user.UserId] = us
	}
	return rst, whoNowResp.NextPage, nil
}

func (p *presence) WhereNow(userId string) ([]*rtm2.ChannelInfo, error) {
	p.lg.Debug("WhereNow")
	req := &PresenceWhereNowReq{UserId: userId}
	resp, errCode, err := p.client.invoker.OnReceived(req)
	if err != nil {
		return nil, err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return nil, err
	}
	whereNowResp := resp.(*PresenceWhereNowResp)
	var infos []*rtm2.ChannelInfo
	for _, channel := range whereNowResp.Channels {
		infos = append(infos, &rtm2.ChannelInfo{Channel: channel.Channel, Type: rtm2.ChannelType(channel.ChannelType)})
	}
	return infos, nil
}

func (p *presence) SetState(channel string, channelType rtm2.ChannelType, data map[string]string) error {
	p.lg.Debug("SetState")
	req := &PresenceSetStateReq{Channel: channel, ChannelType: int32(channelType)}
	for key, value := range data {
		req.Items = append(req.Items, &UserStateItem{Key: key, Value: value})
	}
	_, errCode, err := p.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (p *presence) RemoveState(channel string, channelType rtm2.ChannelType, keys []string) error {
	p.lg.Debug("RemoveState")
	req := &PresenceRemoveStateReq{Channel: channel, ChannelType: int32(channelType), Keys: keys}
	_, errCode, err := p.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (p *presence) GetState(channel string, channelType rtm2.ChannelType, userId string) (map[string]string, error) {
	p.lg.Debug("GetState")
	req := &PresenceGetStateReq{Channel: channel, ChannelType: int32(channelType), UserId: userId}
	resp, errCode, err := p.client.invoker.OnReceived(req)
	if err != nil {
		return nil, err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return nil, err
	}
	getResp := resp.(*PresenceGetStateResp)
	rst := make(map[string]string)
	for _, item := range getResp.Items {
		rst[item.Key] = item.Value
	}
	return rst, nil
}

func (p *presence) subscribe(key string) {
	p.lg.Debug("subscribe")
	ctx, cancel := context.WithCancel(p.client.ctx)
	sub := &presenceSub{ctx: ctx, cancel: cancel, data: make(map[string]*rtm2.UserState), initialized: make(chan interface{}, 1), events: make(chan *rtm2.PresenceEvent, defaultChanSize)}
	if _, loaded := p.subscribes.LoadOrStore(key, sub); loaded {
		p.lg.Warn("subscribe duplicated", zap.String("key", key))
	} else {
		p.lg.Debug("subscribe presence", zap.String("key", key))
	}
}

func (p *presence) unsubscribe(key string) {
	p.lg.Debug("unsubscribe")
	if value, loaded := p.subscribes.LoadAndDelete(key); !loaded {
		p.lg.Warn("not subscribed", zap.String("key", key))
	} else {
		sub := value.(*presenceSub)
		sub.cancel()
		p.lg.Debug("stop subscribe", zap.String("key", key))
	}
}

func (e *PresenceEvent) toRTM2Event() *rtm2.PresenceEvent {
	rst := &rtm2.PresenceEvent{Items: make(map[string]string), States: make(map[string]map[string]string)}
	rst.Type = rtm2.PresenceEventType(e.EventType)
	rst.UserId = e.Publisher
	for _, item := range e.Items {
		rst.Items[item.Key] = item.Value
	}
	rst.Joined = e.Joined
	rst.Left = e.Left
	rst.Timeout = e.Timeout

	for _, us := range e.Snapshot {
		items := make(map[string]string)
		for _, item := range us.Items {
			items[item.Key] = item.Value
		}
		rst.States[us.UserId] = items
	}
	return rst
}

func (p *presence) onEvent(event *PresenceEvent) {
	p.lg.Debug("on Event", zap.Any("event", event))
	key := p.unifyChannelKey(event.Channel, rtm2.ChannelType(event.ChannelType))
	if value, ok := p.subscribes.Load(key); ok {
		sub := value.(*presenceSub)
		e := event.toRTM2Event()
		select {
		case <-sub.initialized:
			sub.events <- e
			p.lg.Debug("put to channel", zap.Any("event", event))
		default:
			if e.Type != rtm2.PresenceTypeSnapshot {
				p.lg.Warn("not snapshot for the first event")
				return
			}
			for userId, state := range e.States {
				us := &rtm2.UserState{UserId: userId, State: make(map[string]string)}
				for k, v := range state {
					us.State[k] = v
				}
				sub.data[userId] = us
			}
			close(sub.initialized)
			p.lg.Debug("store to snapshot and close channel", zap.Any("event", event))
		}
	} else {
		p.lg.Warn("unknown channel", zap.String("channel", event.Channel))
	}
}

func createPresence(cli *client) *presence {
	return &presence{client: cli, lg: cli.lg}
}
