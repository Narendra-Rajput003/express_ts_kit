
import { Kafka, Admin } from 'kafkajs';

class KafkaManager {
  private kafka: Kafka;
  private admin: Admin;

  constructor(clientId: string, brokers: string[]) {
    this.kafka = new Kafka({ clientId, brokers });
    this.admin = this.kafka.admin();
  }

  async connect(): Promise<void> {
    await this.admin.connect();
  }

  async disconnect(): Promise<void> {
    await this.admin.disconnect();
  }

  async createTopic(topicName: string, numPartitions: number = 3): Promise<void> {
    try {
      await this.admin.createTopics({
        topics: [{ topic: topicName, numPartitions }],
      });
      console.log(`Topic "${topicName}" created successfully!`);
    } catch (error) {
      console.error(`Failed to create topic "${topicName}":`, error);
      throw error;
    }
  }

  async listTopics(): Promise<string[]> {
    return await this.admin.listTopics();
  }
}

// Usage
const kafkaManager = new KafkaManager('my-app', ['localhost:9092']);

async function initializeKafka() {
  try {
    await kafkaManager.connect();
    await kafkaManager.createTopic('my-topic');
    const topics = await kafkaManager.listTopics();
    console.log('Available topics:', topics);
  } catch (error) {
    console.error('Kafka initialization failed:', error);
  } finally {
    await kafkaManager.disconnect();
  }
}

initializeKafka();

export { KafkaManager };
