package base

import (
	"github.com/tevino/abool/v2"
	"github.com/tomasliu-agora/rtm2"
	"go.uber.org/zap"
	"sync"
)

type stream struct {
	channel string
	client  *client
	options *rtm2.StreamOptions

	topicEvents     chan *rtm2.TopicEvent
	tc              chan string
	joined          *abool.AtomicBool
	topics          sync.Map
	topicSubscribes sync.Map

	lg *zap.Logger
}

type streamSub struct {
	mc      chan *rtm2.Message
	userIds sync.Map
}

func (s *stream) Join(opts ...rtm2.StreamOption) (map[string][]string, <-chan *rtm2.TopicEvent, <-chan string, error) {
	s.lg.Debug("Join")
	if s.joined.SetToIf(false, true) {
		for _, opt := range opts {
			opt(s.options)
		}
		s.lg.Debug("join channel", zap.String("token", s.options.Token), zap.Bool("metadata", s.options.Metadata), zap.Bool("presence", s.options.Presence), zap.Bool("lock", s.options.Lock))
		req := &StreamJoinReq{Channel: s.channel, Token: s.options.Token, Metadata: s.options.Metadata, Presence: s.options.Presence, Lock: s.options.Lock}
		if s.options.Metadata {
			s.client.storage.subscribe(s.client.storage.unifyChannelKey(s.channel, rtm2.ChannelTypeStream))
		}
		if s.options.Presence {
			s.client.presence.subscribe(s.client.presence.unifyChannelKey(s.channel, rtm2.ChannelTypeStream))
		}
		if s.options.Lock {
			s.client.lock.subscribe(unify(s.channel, rtm2.ChannelTypeStream))
		}
		_, errCode, err := s.client.invoker.OnReceived(req)
		if err != nil {
			return nil, nil, nil, err
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			return nil, nil, nil, err
		}
		snapshotEvent := <-s.topicEvents
		return snapshotEvent.Snapshot, s.topicEvents, s.tc, nil
	} else {
		return nil, nil, nil, rtm2.ERROR_CHANNEL_JOIN_FAILED
	}
}

func (s *stream) RenewToken(token string) error {
	s.lg.Debug("Renew Token")
	req := &RenewTokenReq{
		Token:   token,
		Channel: s.channel,
	}
	_, errCode, err := s.client.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (s *stream) Leave() error {
	s.lg.Debug("Leave")
	if s.joined.SetToIf(true, false) {
		s.lg.Debug("leave channel", zap.String("token", s.options.Token), zap.Bool("metadata", s.options.Metadata), zap.Bool("presence", s.options.Presence), zap.Bool("lock", s.options.Lock))
		// RTE leave channel
		if s.options.Metadata {
			s.client.storage.unsubscribe(s.client.storage.unifyChannelKey(s.channel, rtm2.ChannelTypeStream))
		}
		if s.options.Presence {
			s.client.presence.unsubscribe(s.client.presence.unifyChannelKey(s.channel, rtm2.ChannelTypeStream))
		}
		if s.options.Lock {
			s.client.lock.unsubscribe(unify(s.channel, rtm2.ChannelTypeStream))
		}

		req := &StreamLeaveReq{Channel: s.channel}
		_, errCode, err := s.client.invoker.OnReceived(req)
		if err != nil {
			return err
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			return err
		}
		s.client.streamChannels.Delete(s.channel)
		return nil
	} else {
		return rtm2.ERROR_CHANNEL_NOT_JOINED
	}
}

func (s *stream) ChannelName() string {
	return s.channel
}

func (s *stream) JoinTopic(topic string, opts ...rtm2.StreamOption) error {
	s.lg.Debug("JoinTopic")
	if s.joined.IsNotSet() {
		return rtm2.ERROR_CHANNEL_NOT_JOINED
	}
	options := &rtm2.StreamOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if _, ok := s.topics.LoadOrStore(topic, true); ok {
		s.lg.Error("already joined topic", zap.String("topic", topic))
		return rtm2.ERROR_CHANNEL_JOIN_TOPIC_FAILED
	}
	// RTE join topic
	req := &StreamJoinTopicReq{Channel: s.channel, Topic: topic, Qos: int32(options.QOS), Priority: int32(options.Priority), SyncMedia: options.SyncMedia, Meta: options.Meta}
	_, errCode, err := s.client.invoker.OnReceived(req)
	if err != nil {
		s.topics.Delete(topic)
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		s.topics.Delete(topic)
		return err
	}
	return nil
}

func (s *stream) PublishTopic(topic string, message []byte, opts ...rtm2.StreamOption) error {
	s.lg.Debug("PublishTopic")
	if s.joined.IsNotSet() {
		return rtm2.ERROR_CHANNEL_NOT_JOINED
	}
	if _, ok := s.topics.Load(topic); ok {
		options := &rtm2.StreamOptions{}
		for _, opt := range opts {
			opt(options)
		}
		// RTE publish topic
		req := &StreamMessageReq{Channel: s.channel, Topic: topic, Message: message, Type: int32(options.Type), SendTs: int64(options.SendTs), CustomType: options.CustomType}
		_, errCode, err := s.client.invoker.OnReceived(req)
		if err != nil {
			s.topics.Delete(topic)
			return err
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			s.topics.Delete(topic)
			return err
		}
		return nil
	} else {
		s.lg.Error("topic not joined", zap.String("topic", topic))
		return rtm2.ERROR_CHANNEL_PUBLISH_MESSAGE_FAILED
	}
}

func (s *stream) LeaveTopic(topic string) error {
	s.lg.Debug("LeaveTopic")
	if s.joined.IsNotSet() {
		return rtm2.ERROR_CHANNEL_NOT_JOINED
	}
	if _, ok := s.topics.LoadAndDelete(topic); ok {
		// RTE leave topic
		req := &StreamLeaveTopicReq{Channel: s.channel, Topic: topic}
		_, errCode, err := s.client.invoker.OnReceived(req)
		if err != nil {
			s.topics.Delete(topic)
			return err
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			s.topics.Delete(topic)
			return err
		}
		return nil
	} else {
		s.lg.Warn("topic not joined", zap.String("topic", topic))
		return nil
	}
}

func (s *stream) SubscribeTopic(topic string, userIds []string) (<-chan *rtm2.Message, error) {
	s.lg.Debug("SubscribeTopic")
	if s.joined.IsNotSet() {
		return nil, rtm2.ERROR_CHANNEL_NOT_JOINED
	}
	sub := &streamSub{mc: make(chan *rtm2.Message, defaultChanSize)}
	for _, userId := range userIds {
		sub.userIds.Store(userId, true)
	}
	users := make([]string, 0)
	if value, ok := s.topicSubscribes.LoadOrStore(topic, sub); ok {
		sub = value.(*streamSub)
		if len(userIds) > 0 {
			for _, userId := range userIds {
				if _, ok := sub.userIds.LoadOrStore(userId, true); !ok {
					users = append(users, userId)
				}
			}
		}
		s.lg.Debug("New user subscribed in topic", zap.String("topic", topic), zap.Strings("userIds", userIds))
	} else {
		users = userIds
		s.lg.Debug("New topic subscribed", zap.String("topic", topic), zap.Strings("userIds", userIds))
	}
	if len(users) > 0 || len(userIds) == 0 {
		// RTE sub topic
		req := &StreamSubTopicReq{Channel: s.channel, Topic: topic, UserIds: userIds}
		resp, errCode, err := s.client.invoker.OnReceived(req)
		if err != nil {
			return nil, err
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			return nil, err
		}

		subResp := resp.(*StreamSubTopicResp)
		s.lg.Info("sub resp on topic", zap.String("topic", topic), zap.Strings("succeed", subResp.Succeed), zap.Strings("failed", subResp.Failed))
		return sub.mc, nil
	} else {
		s.lg.Debug("No need to send request", zap.String("topic", topic), zap.Strings("userIds", userIds))
		return sub.mc, nil
	}
}

func (s *stream) UnsubscribeTopic(topic string, userIds []string) error {
	s.lg.Debug("UnsubscribeTopic")
	if s.joined.IsNotSet() {
		return rtm2.ERROR_CHANNEL_NOT_JOINED
	}
	users := make([]string, 0)
	if len(userIds) > 0 {
		if value, ok := s.topicSubscribes.Load(topic); ok {
			sub := value.(*streamSub)
			for _, userId := range userIds {
				if _, ok := sub.userIds.LoadAndDelete(userId); ok {
					users = append(users, userId)
				}
			}
			count := 0
			sub.userIds.Range(func(key, value interface{}) bool {
				count += 1
				return true
			})
			/*
				if count == 0 {
					s.topicSubscribes.Delete(topic)
					close(sub.mc)
					s.lg.Debug("topic not interested", zap.String("topic", topic), zap.Strings("userIds", userIds))
				}
			*/
			s.lg.Debug("user unsubscribed in topic", zap.String("topic", topic), zap.Strings("userIds", userIds))
		} else {
			s.lg.Warn("Topic not subscribed", zap.String("topic", topic), zap.Strings("userIds", userIds))
			return nil
		}
	}

	if len(users) > 0 || len(userIds) == 0 {
		// RTE sub topic
		req := &StreamUnsubTopicReq{Channel: s.channel, Topic: topic, UserIds: userIds}
		_, errCode, err := s.client.invoker.OnReceived(req)
		if err != nil {
			return err
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			return err
		}
	} else {
		s.lg.Debug("No need to send request", zap.String("topic", topic), zap.Strings("userIds", userIds))
	}
	/*
		else {
			if value, ok := s.topicSubscribes.LoadAndDelete(topic); ok {
				sub := value.(*streamSub)
				close(sub.mc)
				s.lg.Debug("topic not interested", zap.String("topic", topic), zap.Strings("userIds", userIds))
			}
		}
	*/
	return nil
}

func (s *stream) GetSubscribedUsers(topic string) ([]string, error) {
	s.lg.Debug("GetSubscribedUsers")
	if _, ok := s.topicSubscribes.Load(topic); ok {
		req := &StreamSubListReq{Channel: s.channel, Topic: topic}
		resp, errCode, err := s.client.invoker.OnReceived(req)
		if err != nil {
			s.topics.Delete(topic)
			return nil, err
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			s.topics.Delete(topic)
			return nil, err
		}
		subResp := resp.(*StreamSubListResp)
		return subResp.UserIds, nil
	}
	return nil, rtm2.ERROR_CHANNEL_TOPIC_NOT_SUBSCRIBED
}

func (s *stream) OnMessage(msg *StreamMessageEvent) {
	m := &rtm2.Message{UserId: msg.Publisher, Type: rtm2.MessageType(msg.Type), Message: msg.Message, CustomType: msg.CustomType}
	if value, ok := s.topicSubscribes.Load(msg.Topic); ok {
		ssub := value.(*streamSub)
		if _, exists := ssub.userIds.Load(msg.Publisher); exists {
			ssub.mc <- m
			s.lg.Debug("message", zap.String("topic", msg.Topic), zap.String("publisher", msg.Publisher), zap.Int("message", len(msg.Message)))
		}
	}
}

func (e *StreamTopicEvent) toRTM2Event() *rtm2.TopicEvent {
	rst := &rtm2.TopicEvent{}
	rst.Channel = e.Channel
	rst.Type = rtm2.TopicEventType(e.Type)
	rst.UserId = e.UserId
	switch rst.Type {
	case rtm2.TopicEventSnapshot:
		rst.Snapshot = make(map[string][]string)
		for _, info := range e.Infos {
			var ps []string
			for _, p := range info.Publishers {
				ps = append(ps, p.UserId)
			}
			rst.Snapshot[info.Topic] = ps
		}
	case rtm2.TopicEventJoin, rtm2.TopicEventLeave:
		if len(e.Infos) != 0 {
			rst.Topic = e.Infos[0].Topic
			rst.UserId = e.Infos[0].Publishers[0].UserId
		}
	}
	return rst
}

func (s *stream) OnTopicEvent(event *StreamTopicEvent) {
	s.lg.Debug("on topic event", zap.Any("event", event))
	s.topicEvents <- event.toRTM2Event()
}
