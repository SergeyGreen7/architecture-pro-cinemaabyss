package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	log.Println("brokers:", brokers)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	go consume(brokers, "movie-events", "events-service")
	go consume(brokers, "user-events", "events-service")
	go consume(brokers, "payment-events", "events-service")

	http.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":true}`))
	})
	http.HandleFunc("/api/events/movie", func(w http.ResponseWriter, r *http.Request) {
		produce(writer, w, r, "movie-events")
	})
	http.HandleFunc("/api/events/user", func(w http.ResponseWriter, r *http.Request) {
		produce(writer, w, r, "user-events")
	})
	http.HandleFunc("/api/events/payment", func(w http.ResponseWriter, r *http.Request) {
		produce(writer, w, r, "payment-events")
	})

	port := getEnv("PORT", "8082")
	log.Printf("events service listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// w is http.ResponseWriter (interface), not *http.ResponseWriter
func produce(writer *kafka.Writer, w http.ResponseWriter, r *http.Request, topic string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("producing topic=%s value=%s", topic, string(body))

	err = writer.WriteMessages(r.Context(), kafka.Message{
		Topic: topic,
		Value: body,
		Time:  time.Now(),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status":"success"}`))
}

func consume(brokers []string, topic, groupID string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("consume %s: %v", topic, err)
			continue
		}
		log.Printf("consumed topic=%s value=%s", msg.Topic, string(msg.Value))
	}
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
