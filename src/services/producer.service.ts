import { Kafka, Producer } from 'kafkajs';

class KafkaProducer {
  private kafka: Kafka;
  private producer: Producer;

  constructor(clientId: string, brokers: string[]) {
    this.kafka = new Kafka({ clientId, brokers });
    this.producer = this.kafka.producer();
  }

  async connect(): Promise<void> {
    await this.producer.connect();
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
  }

  async sendMessages(topic: string, messages: Array<{ key: string; value: string }>): Promise<void> {
    try {
      await this.producer.send({
        topic,
        messages,
      });
      console.log(`Messages sent successfully to topic: ${topic}`);
    } catch (error) {
      console.error(`Failed to send messages to topic ${topic}:`, error);
      throw error;
    }
  }
}

// Usage
const kafkaProducer = new KafkaProducer('my-app-producer', ['localhost:9092']);

async function produceMessages() {
  try {
    await kafkaProducer.connect();
    await kafkaProducer.sendMessages('my-topic', [
      { key: 'key1', value: 'Hello from Kafka!' },
      { key: 'key2', value: 'This is another message' },
    ]);
  } catch (error) {
    console.error('Error producing messages:', error);
  } finally {
    await kafkaProducer.disconnect();
  }
}

produceMessages();

export { KafkaProducer };
