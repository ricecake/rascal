package rascal

import (
	"fmt"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"log"
	"os"
	"sync"
	"time"
)

type Rascal struct {
	name       string
	connection *amqp.Connection
	handler    func(amqp.Delivery)
	errChan    chan *amqp.Error
	active     bool
	connLock   sync.Mutex
}

func (this *Rascal) Connect() error {
	this.connLock.Lock()

	amqpURI := fmt.Sprintf("amqp://%s:%s@%s:%s/%s",
		viper.GetString("amqp.user"),
		viper.GetString("amqp.pass"),
		viper.GetString("amqp.host"),
		viper.GetString("amqp.port"),
		viper.GetString("amqp.vhost"),
	)
	var conn *amqp.Connection
	var err error
	config := amqp.Config{
		nil, // nil defaults to PlainAuth
		viper.GetString("amqp.vhost"),
		0, // 0 max channels means 2^16 - 1
		0, // 0 max bytes means unlimited
		// less than 1s uses the server's interval
		time.Duration(time.Duration(viper.GetInt("amqp.heartbeat")) * time.Second),
		nil,     // TLSClientConfig
		nil,     // table of properties client advertises to server, let library handle till we have a need
		"en_US", // Connection locale that we expect to always be en_US
		nil,     // nil - net.DialTimeout with a 30s connection and 30s deadline for TLS/AMQP handshake
	}
	conn, err = amqp.DialConfig(amqpURI, config)
	if err != nil {
		return err
	}
	log.Printf("connected to amqp://%s@%s:%s/%s\n",
		viper.GetString("amqp.user"),
		viper.GetString("amqp.host"),
		viper.GetString("amqp.port"),
		viper.GetString("amqp.vhost"),
	)

	this.errChan = make(chan *amqp.Error)
	go func() {
		for {
			err, open := <-this.errChan
			if open {
				if err != nil {
					log.Printf("AMQP ERROR: %s", err.Error())
					this.Cleanup()
					this.Connect()
					if this.active {
						this.Consume()
					}
				}
			}
			return
		}
	}()
	conn.NotifyClose(this.errChan)

	this.connection = conn

	hostname, hostnameErr := os.Hostname()
	if hostnameErr != nil {
		log.Printf("Error getting hostname: %s", hostnameErr.Error())
		panic(hostnameErr)
	}
	pid := os.Getpid()
	queueName := fmt.Sprintf("%s-%s-%d", viper.GetString("app.name"), hostname, pid)
	this.name = queueName

	ch, chanErr := this.connection.Channel()
	if chanErr != nil {
		return chanErr
	}

	if viper.IsSet("amqp.exchanges") {
		exchanges := viper.Get("amqp.exchanges").([]interface{})
		for _, exchangeData := range exchanges {
			exchange := make(map[string]interface{})

			for key, val := range exchangeData.(map[interface{}]interface{}) {
				exchange[key.(string)] = val
			}

			exchErr := ch.ExchangeDeclare(
				exchange["name"].(string),    // name
				exchange["type"].(string),    // type
				exchange["durable"].(bool),   // durable
				exchange["transient"].(bool), // auto-delete
				false, // internal
				false, // no-wait
				nil,   // arguments
			)
			if exchErr != nil {
				log.Printf("Error declaring exchange: %s", exchErr.Error())
				return exchErr
			}
		}
	}

	if viper.IsSet("amqp.queues") {
		queues := viper.Get("amqp.queues").([]interface{})
		for _, queueData := range queues {
			queue := make(map[string]interface{})

			for key, val := range queueData.(map[interface{}]interface{}) {
				queue[key.(string)] = val
			}

			_, queueErr := ch.QueueDeclare(
				queue["name"].(string),    // name
				queue["durable"].(bool),   // durable
				queue["transient"].(bool), // auto-delete
				queue["exclusive"].(bool), // exclusive
				false, // no-wait
				nil,   // arguments
			)
			if queueErr != nil {
				log.Printf("error declaring queue: %s", queueErr.Error())
				return queueErr
			}
			if binding, exists := queue["binding"]; exists {
				for _, bindData := range binding.([]interface{}) {
					bind := make(map[string]string)

					for key, val := range bindData.(map[interface{}]interface{}) {
						bind[key.(string)] = val.(string)
					}

					bindErr := ch.QueueBind(
						queue["name"].(string), // queue name
						bind["key"],            // routing key
						bind["exchange"],       // exchange
						false,
						nil,
					)
					if bindErr != nil {
						log.Printf("error binding queue to exhange: %s", bindErr.Error())
						return bindErr
					}
				}
			}

		}
	}

	ch.Close()

	this.connLock.Unlock()
	return nil
}

func (this *Rascal) Cleanup() {
	this.connection.Close()
	return
}

func (this *Rascal) SetHandler(callback func(amqp.Delivery)) {
	this.handler = callback
	return
}

func (this *Rascal) Consume() error {
	process := func() error {
		ch, chanErr := this.connection.Channel()
		if chanErr != nil {
			return chanErr
		}
		log.Printf("Worker channel successfully opened")

		msgChannel, consumeErr := ch.Consume(
			this.name, // queue
			this.name, // amqp consumer tag
			false,     // never auto ack here
			false,
			false, // no local
			false, // no wait
			nil,   // args
		)
		if consumeErr != nil {
			log.Printf("Error consuming: %s", consumeErr.Error())
			return consumeErr
		}

		for msg := range msgChannel {
			this.handler(msg)
		}
		log.Println("Worker Closing")
		return nil
	}
	for i := 0; i < viper.GetInt("general.workthreads"); i++ {
		go process()
	}
	this.active = true
	return nil
}
