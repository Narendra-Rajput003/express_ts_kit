

import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';

class KafkaConsumer {
  private kafka: Kafka;
  private consumer: Consumer;

  constructor(clientId: string, brokers: string[], groupId: string) {
    this.kafka = new Kafka({ clientId, brokers });
    this.consumer = this.kafka.consumer({ groupId });
  }

  async connect(): Promise<void> {
    await this.consumer.connect();
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
  }

  async subscribe(topic: string, fromBeginning: boolean = true): Promise<void> {
    await this.consumer.subscribe({ topic, fromBeginning });
  }

  async consume(messageHandler: (payload: EachMessagePayload) => Promise<void>): Promise<void> {
    await this.consumer.run({
      eachMessage: async (payload) => {
        try {
          await messageHandler(payload);
        } catch (error) {
          console.error('Error processing message:', error);
        }
      },
    });
  }
}

// Usage
const kafkaConsumer = new KafkaConsumer('my-app-consumer', ['localhost:9092'], 'my-group');

async function startConsuming() {
  try {
    await kafkaConsumer.connect();
    await kafkaConsumer.subscribe('my-topic');
    await kafkaConsumer.consume(async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        key: message.key?.toString(),
        value: message.value?.toString(),
        offset: message.offset,
      });
    });
  } catch (error) {
    console.error('Error in Kafka consumer:', error);
  }
}

startConsuming();

export { KafkaConsumer };
