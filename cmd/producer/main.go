package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	//fmt.Println("Hello Go")
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("mensagem 4", "teste", producer, nil, deliveryChan)
	go DeliveryReport(deliveryChan)
	fmt.Println("Adriana")
	// e := <-deliveryChan
	// msg := e.(*kafka.Message)
	// if msg.TopicPartition.Error != nil{
	// 	fmt.Println("Erro ao enviar")
	// }else{
	// 	fmt.Println("Mensagem enviada: ", msg.TopicPartition)
	// }

	producer.Flush(1000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka:9092",
		"delivery.timeout.ms":"0",//tempo maxima de entrega 0 = infinito, a msg perde o sentido de ser entregue
		"acks" :"0", //0 manda a msg e não precisa saber que foi entregue, 1 aguarda que o lider retorne dizendo se recebeu, all, o lider e todos brokers sincronizados tenham recebido
		"enable.idempotence" :"false", //false: a msg pode chegar mais de uma vez, pode perder alguma msg. true: msg entregue uma vez e na ordem que se esperava acks, precisa ser all
	}
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Key:            key,
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mensagem enviada: ", ev.TopicPartition)
				//salva no bd que a msg foi processada
				//ex.: confima que uma transferencia bancaria ocorreu
			}
		}
	}
}
