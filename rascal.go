package rascal

import (
	"fmt"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

type Rascal struct {
	name       string
	connection *amqp.Connection
	handlers   map[string]func(amqp.Delivery, *amqp.Channel)
	errChan    chan *amqp.Error
	active     bool
	connLock   sync.Mutex
	Custom     string
	Default    string
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

	ch, chanErr := this.Channel()
	if chanErr != nil {
		return chanErr
	}

	if viper.IsSet("amqp.exchanges") {
		log.Println("declaring exchanges")
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

	this.handlers = make(map[string]func(amqp.Delivery, *amqp.Channel))

	if viper.GetBool("amqp.queue.auto-queue") {
		defaultQueue, qErr := ch.QueueDeclare(
			this.name,
			false,
			true,
			false,
			false,
			nil,
		)
		if qErr != nil {
			log.Printf("Error declaring auto-queue: %s", qErr.Error())
		}
		this.Default = defaultQueue.Name
	}
	if viper.IsSet("amqp.queue") {
		log.Println("declaring queue")
		queue, queueErr := ch.QueueDeclare(
			viper.GetString("amqp.queue.name"),    // name
			viper.GetBool("amqp.queue.durable"),   // durable
			viper.GetBool("amqp.queue.transient"), // auto-delete
			viper.GetBool("amqp.queue.exclusive"), // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if queueErr != nil {
			log.Printf("error declaring queue: %s", queueErr.Error())
			return queueErr
		}
		viper.Set("amqp.queue.name", queue.Name)
		this.Custom = queue.Name
	}
	if viper.IsSet("amqp.queue.bindings") {
		log.Println("declaring bindings...")
		bindData := viper.GetStringMap("amqp.queue.bindings")
		for exchange, bindings := range bindData {

			for _, binding := range bindings.([]interface{}) {
				bindErr := ch.QueueBind(
					viper.GetString("amqp.queue.name"), // queue name
					binding.(string),                   // routing key
					exchange,                           // exchange
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

	ch.Close()

	this.connLock.Unlock()
	return nil
}

func (this *Rascal) Cleanup() {
	this.connection.Close()
	return
}

func (this *Rascal) SetHandler(queueName string, callback func(amqp.Delivery, *amqp.Channel)) {
	this.handlers[queueName] = callback
	return
}

func (this *Rascal) Channel() (*amqp.Channel, error) {
	log.Println("getting new channel...")
	return this.connection.Channel()
}

func (this *Rascal) Consume() error {
	process := func(queueName string, handler func(amqp.Delivery, *amqp.Channel)) error {
		ch, chanErr := this.Channel()
		if chanErr != nil {
			return chanErr
		}
		log.Printf("Worker channel successfully opened")

		msgChannel, consumeErr := ch.Consume(
			queueName, // queue
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
			handler(msg, ch)
		}
		log.Println("Worker Closing")
		return nil
	}

	viper.SetDefault("general.workthreads", runtime.NumCPU())
	for queue, handler := range this.handlers {
		for i := 0; i < viper.GetInt("general.workthreads"); i++ {
			go process(queue, handler)
		}
	}
	this.active = true
	return nil
}
