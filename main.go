package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/go-redis/redis"
	"github.com/google/jsonapi"
	"github.com/nu7hatch/gouuid"
	"github.com/streadway/amqp"
)

type Config struct {
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	RmqAddr string
}

type Env struct {
	amqp   *amqp.Connection
	config *Config
	redis  *redis.Client
}

type Event struct {
	ID      string `jsonapi:"primary,event"`
	Topic   string `jsonapi:"attr,topic"`
	Payload string `jsonapi:"attr,payload"`
	Link    string `jsonapi:"attr,link"`
}

func collectConfig() (config Config) {
	var missingEnv []string

	// RMQ_ADDR
	config.RmqAddr = os.Getenv("RMQ_ADDR")
	if config.RmqAddr == "" {
		missingEnv = append(missingEnv, "RMQ_ADDR")
	}

	// REDIS_ADDR
	config.RedisAddr = os.Getenv("REDIS_ADDR")
	if config.RedisAddr == "" {
		missingEnv = append(missingEnv, "REDIS_ADDR")
	}

	// REDIS_PASSWORD
	config.RedisPassword = os.Getenv("REDIS_PASSWORD")

	// REDIS_DB
	var envRedisDB string = os.Getenv("REDIS_DB")

	if envRedisDB == "" {
		config.RedisDB = 0
	} else {
		i, err := strconv.Atoi(envRedisDB)
		panicOnError(err, "Error parsing REDIS_DB")
		config.RedisDB = i
	}

	// Validation
	if len(missingEnv) > 0 {
		var msg string = fmt.Sprintf("Environment variables missing: %v", missingEnv)
		log.Fatal(msg)
		panic(fmt.Sprint(msg))
	}

	return
}

func panicOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func makeErrorResponse(w http.ResponseWriter, status int, detail string, code int) {
	var codeTitle map[int]string
	codeTitle = make(map[int]string)
	codeTitle[1] = "Malformed JSON Body"
	codeTitle[2201] = "Missing Required Attribute"
	codeTitle[2202] = "Requested Relationship Not Found"

	var statusTitle map[int]string
	statusTitle = make(map[int]string)
	statusTitle[400] = "Bad Request"
	statusTitle[401] = "Unauthorized"
	statusTitle[404] = "Not Found"
	statusTitle[405] = "Method Not Allowed"
	statusTitle[406] = "Not Acceptable"
	statusTitle[409] = "Conflict"
	statusTitle[415] = "Unsupported Media Type"
	statusTitle[422] = "Unprocessable Entity"
	statusTitle[500] = "Internal Server Error"

	var title string
	var statusStr string = strconv.Itoa(status)
	var codeStr string

	// Get Title
	if code == 0 { // code 0 means no code
		title = statusTitle[status]
	} else {
		title = codeTitle[code]
		codeStr = strconv.Itoa(code)
	}

	// Send Response
	w.WriteHeader(status)
	jsonapi.MarshalErrors(w, []*jsonapi.ErrorObject{{
		Title:  title,
		Detail: detail,
		Status: statusStr,
		Code:   codeStr,
	}})

	return
}

func (env *Env) handleEvent(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", jsonapi.MediaType) // application/vnd.api+json

	if r.Method == "POST" {
		// Get event from body
		event := new(Event)
		if err := jsonapi.UnmarshalPayload(r.Body, event); err != nil {
			makeErrorResponse(w, 400, err.Error(), 1)
			return
		}

		// Validate or Assign UUID if not present
		if event.ID == "" {
			u4, err := uuid.NewV4()
			if err != nil {
				makeErrorResponse(w, 500, err.Error(), 0)
				return
			}
			event.ID = u4.String()
		} else {
			u, err := uuid.ParseHex(event.ID)
			_ = u
			if err != nil {
				makeErrorResponse(w, 400, err.Error(), 0)
				return
			}
		}

		log.Printf("New event: %v", event)

		// Build Response
		buf := bytes.NewBufferString("")
		if err := jsonapi.MarshalPayload(buf, event); err != nil {
			makeErrorResponse(w, 500, err.Error(), 0)
			return
		}

		// Send event to event bus
		ch, err := env.amqp.Channel()
		if err := jsonapi.MarshalPayload(buf, event); err != nil {
			makeErrorResponse(w, 500, fmt.Sprintf("Failed to open a channel: %s", err), 0)
			return
		}
		defer ch.Close()

		err = ch.ExchangeDeclare(
			"events", // name
			"topic",  // type
			true,     // durable
			false,    // auto-deleted
			false,    // internal
			false,    // no-wait
			nil,      // arguments
		)
		if err != nil {
			makeErrorResponse(w, 500, err.Error(), 0)
			return
		}

		body := buf.String()
		err = ch.Publish(
			"events",    // exchange
			event.Topic, // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})

		if err != nil {
			makeErrorResponse(w, 500, err.Error(), 0)
			return
		}

		// Send response
		if err := jsonapi.MarshalPayload(w, event); err != nil {
			makeErrorResponse(w, 500, err.Error(), 0)
		}
	}
}

func main() {
	config := collectConfig()

	// Connect to RabbitMQ
	conn, err := amqp.Dial(config.RmqAddr)
	panicOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	log.Println("Connected to RabbitMQ")

	// Connect to Redis
	client := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: config.RedisPassword, // no password set
		DB:       config.RedisDB,       // use default DB
	})

	pong, err := client.Ping().Result()
	_ = pong
	panicOnError(err, "Failed to connect to Redis")
	log.Println("Connected to Redis")

	// Build Environment
	env := &Env{amqp: conn, redis: client}

	http.HandleFunc("/event/v1", env.handleEvent)
	http.ListenAndServe(":8080", nil)
}
