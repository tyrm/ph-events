package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/jsonapi"
	"github.com/nu7hatch/gouuid"
	"github.com/streadway/amqp"
)

type Config struct {
	EventTTL time.Duration

	RedisAddr     string
	RedisDB       int
	RedisPassword string
	RedisPrefix   string

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
	Payload string `jsonapi:"attr,payload,omitempty"`
	Link    string `jsonapi:"attr,link,omitempty"`
}

type EventRIO struct {
	ID string `jsonapi:"primary,event"`
}

func collectConfig() (config Config) {
	var missingEnv []string

	// EVENT_TTL
	var envEventTTL string = os.Getenv("EVENT_TTL")

	if envEventTTL == "" {
		config.EventTTL = time.Hour
	} else {
		i, err := strconv.Atoi(envEventTTL)
		panicOnError(err, "Error parsing EVENT_TTL")
		config.EventTTL = time.Duration(i) * time.Second
	}

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

	// REDIS_PREFIX
	var envRedisPrefix string = os.Getenv("REDIS_PREFIX")

	if envRedisPrefix == "" {
		config.RedisPrefix = "ph:"
	} else {
		config.RedisPrefix = envRedisPrefix
	}

	// Validation
	if len(missingEnv) > 0 {
		var msg string = fmt.Sprintf("Environment variables missing: %v", missingEnv)
		log.Fatal(msg)
		panic(fmt.Sprint(msg))
	}

	return
}

func (env *Env) handleEvent(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", jsonapi.MediaType) // application/vnd.api+json

	if request.Method == "POST" {
		// Get event from eventString
		event := new(Event)
		if err := jsonapi.UnmarshalPayload(request.Body, event); err != nil {
			makeErrorResponse(response, 400, err.Error(), 1)
			return
		}

		// Check for topic
		if event.Topic == "" {
			makeErrorResponse(response, 422, "topic", 2201)
			return
		}

		// Validate or Assign UUID if not present
		if event.ID == "" {
			u4, err := uuid.NewV4()
			if err != nil {
				makeErrorResponse(response, 500, err.Error(), 0)
				return
			}
			event.ID = u4.String()
		} else {
			u, err := uuid.ParseHex(event.ID)
			_ = u
			if err != nil {
				makeErrorResponse(response, 400, err.Error(), 0)
				return
			}
		}

		log.Printf("New event: %s", event.ID)

		// Build Response
		var eventPayload bytes.Buffer
		if err := jsonapi.MarshalPayload(&eventPayload, event); err != nil {
			makeErrorResponse(response, 500, err.Error(), 0)
			return
		}

		// Cache event for Pollers
		// TODO use Redis hash
		err := env.redis.Set(fmt.Sprintf("%sevent:%s", env.config.RedisPrefix, event.ID), eventPayload.String(), env.config.EventTTL).Err()
		if err != nil {
			makeErrorResponse(response, 500, err.Error(), 0)
			return
		}

		// Send event to event bus
		ch, err := env.amqp.Channel()
		if err != nil {
			makeErrorResponse(response, 500, fmt.Sprintf("Failed to open a channel: %s", err), 0)
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
			makeErrorResponse(response, 500, err.Error(), 0)
			return
		}

		err = ch.Publish(
			"events",    // exchange
			event.Topic, // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        eventPayload.Bytes(),
			})
		if err != nil {
			makeErrorResponse(response, 500, err.Error(), 0)
			return
		}

		// Send response
		fmt.Fprint(response, eventPayload.String())

		return
	} else if request.Method == "GET" {
		eventKeys := env.redis.Keys(fmt.Sprintf("%sevent:*", env.config.RedisPrefix))

		// Build list of resource identifiers
		var recentEvents []*EventRIO
		for _, key := range eventKeys.Val() {
			var redisKey []string = strings.Split(key, ":")
			recentEvents = append(recentEvents, &EventRIO{redisKey[len(redisKey)-1]})
		}

		if err := jsonapi.MarshalPayload(response, recentEvents); err != nil {
			makeErrorResponse(response, 500, err.Error(), 0)
			return
		}

		return
	} else {
		makeErrorResponse(response, 405, request.Method, 0)
		return
	}
}

func makeErrorResponse(response http.ResponseWriter, status int, detail string, code int) {
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
	response.WriteHeader(status)
	jsonapi.MarshalErrors(response, []*jsonapi.ErrorObject{{
		Title:  title,
		Detail: detail,
		Status: statusStr,
		Code:   codeStr,
	}})

	return
}

func panicOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
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
	env := &Env{amqp: conn, redis: client, config: &config}

	http.HandleFunc("/event/v1", env.handleEvent)
	http.ListenAndServe(":8080", nil)
}
