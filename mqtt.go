package mq

import (
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gitlab.beeps.cn/common/logs"
)

var (
	mClient mqtt.Client
	emqAddr = ""
	emqUser = ""
	emqPwd  = ""
)

var onConnectCallBack mqtt.OnConnectHandler = func(client mqtt.Client) {
	options := client.OptionsReader()
	clientId := options.ClientID()
	logs.Std.Debug("mqtt " + clientId + " client connect success ")
	for topic, callback := range PreList {
		if err := client.Subscribe(topic, 2, callback); err != nil {
			logs.Std.Error(err)
		}
	}
}

type PreModel map[string]func(client mqtt.Client, message mqtt.Message)

var PreList PreModel

func PreSub(preList PreModel) {
	PreList = preList
}

func getMqttOpts(ClientId string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions().AddBroker(emqAddr).SetClientID(ClientId)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetConnectTimeout(60 * time.Second)
	opts.SetUsername(emqUser)
	opts.SetPassword(emqPwd)
	opts.SetProtocolVersion(4)
	opts.SetAutoReconnect(true)
	opts.SetOnConnectHandler(onConnectCallBack)
	opts.AutoReconnect = true
	return opts
}

func InitMqtt(clientId, host, user, passwd string) {
	emqAddr = host
	emqUser = user
	emqPwd = passwd
	opts := getMqttOpts(clientId)
	mClient = mqtt.NewClient(opts)
	token := mClient.Connect()
	if token.WaitTimeout(1500*time.Millisecond) && token.Error() != nil {
		panic("mqtt init failed," + token.Error().Error())
	}
}

func Publish(topic string, payload interface{}) error {
	token := mClient.Publish(topic, 2, false, payload)
	if token.WaitTimeout(500*time.Millisecond) && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func PublishRetained(topic string, payload interface{}) error {
	token := mClient.Publish(topic, 2, true, payload)
	if token.WaitTimeout(500*time.Millisecond) && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func Subscribe(topic string, callback func(client mqtt.Client, message mqtt.Message)) error {
	token := mClient.Subscribe(topic, 2, callback)
	if token.WaitTimeout(1000*time.Millisecond) && token.Error() != nil {
		return token.Error()
	}
	return nil
}
