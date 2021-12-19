package io.github.jasonheo

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.{AdminClient, ListOffsetsOptions, OffsetSpec, TopicDescription}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.{Assertion, FlatSpec, Matchers}

import java.util.{Collections, Properties}
import scala.concurrent.duration.DurationInt
import scala.collection.JavaConverters._

class EmbeddedKafkaTest extends FlatSpec with Matchers with EmbeddedKafka {
  "AdminClient API" must "work well with EmbeddedKafka" in {
    implicit val config = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2182)

    implicit val serializer: Serializer[String] = new StringSerializer()
    implicit val deserializer: Deserializer[String] = new StringDeserializer()

    withRunningKafka {
      val topic = "topic1"

      val numPartitions = 2
      val replicaFactor = 1

      // 참고: 명시적으로 topic을 생성하지 않더라도 produce/consume이 가능했다
      // 본인의 경우 partition 개수를 2개로 지정하기 위하여 명시적으로 topic을 생성했다
      createCustomTopic(topic, Map[String, String](), numPartitions, replicaFactor)

      EmbeddedKafka.withProducer[String, String, Unit]((producer: KafkaProducer[String, String]) => {
        produceRecord(topic, producer, 0, 100L)
        produceRecord(topic, producer, 0, 110L)
        produceRecord(topic, producer, 1, 200L)
        produceRecord(topic, producer, 1, 210L)
        produceRecord(topic, producer, 1, 220L)
      })

      EmbeddedKafka.withConsumer[String, String, Assertion]((consumer: KafkaConsumer[String, String]) => {
        consumer.subscribe(Collections.singletonList(topic))

        // 참고 1: EmbeddedKafka의 README에서 아래 block은 `eventually`로 감싸져있었다
        // 참고 2: EmbeddedKafka의 README에서 poll()의 timeout이 1초였으나, 환경에 따라 1초가 부족한 듯하여 넉넉히 10으로 늘렸다
        //  - 이는 "10초 동안 blocking이 되는 것"을 의미하지 않는다
        //  - 다만, 뭔가 실수가 있는 경우 message가 인입되지 않으므로 10초 동안 멈췄다가 Test가 실패할 수 있다
        //  - 예) 존재하지 않는 partition 번호를 적은 경우 등
        val records: Iterable[ConsumerRecord[String, String]] = consumer.poll(java.time.Duration.ofMillis(10.seconds.toMillis)).asScala

        records.foreach(record => {
          // println() 결과
          //
          // partition=1, timestamp=200, offset=0, key=9128, value=8911
          // partition=1, timestamp=210, offset=1, key=7444, value=5715
          // partition=1, timestamp=220, offset=2, key=6943, value=9530
          // partition=0, timestamp=100, offset=0, key=8490, value=5978
          // partition=0, timestamp=110, offset=1, key=3768, value=4133
          println(
            s"partition=${record.partition()}, " +
            s"timestamp=${record.timestamp()}, " +
            s"offset=${record.offset()}, " +
            s"key=${record.key()}, " +
            s"value=${record.value()}"
          )
        })

        records.size should be(5)
      })

      val adminClient: AdminClient = getAdminClient()

      getNumPartitions(adminClient, topic) should be(2)

      getOffsetOfTimestamp(adminClient, topic, partitionNum=0, timestamp=105L) should be(1)
      getOffsetOfTimestamp(adminClient, topic, partitionNum=1, timestamp=215L) should be(2)

      Thread.sleep(100000)

      adminClient.close()
    }
  }

  private def produceRecord(topic: String,
                            producer: KafkaProducer[String, String],
                            partitionNum: Int,
                            timestamp: Long): Unit = {
    val rnd = scala.util.Random

    val key = rnd.nextInt(10000).abs.toString
    val msg = rnd.nextInt(10000).abs.toString

    val record = new ProducerRecord[String, String](topic, partitionNum, timestamp, key, msg)

    producer.send(record)
  }

  private def getAdminClient(): AdminClient = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    val adminClient: AdminClient = AdminClient.create(props)

    adminClient
  }

  private def getNumPartitions(adminClient: AdminClient, topic: String): Int = {
    val topicMetaData: java.util.Map[String, TopicDescription] = adminClient
      .describeTopics(Collections.singletonList(topic))
      .all
      .get

    val numPartitionsOfTopic: Int = topicMetaData
      .get(topic)
      .partitions()
      .size()

    numPartitionsOfTopic
  }

  private def getOffsetOfTimestamp(adminClient: AdminClient,
                                   topic: String,
                                   partitionNum: Int,
                                   timestamp: Long): Long = {
    import scala.collection.JavaConverters._

    val topicPartition = new TopicPartition(topic, partitionNum)

    val topicPartitionOffsets: Map[TopicPartition, OffsetSpec] = Map(
      topicPartition -> OffsetSpec.forTimestamp(timestamp)
    )

    val offsets = adminClient.listOffsets(topicPartitionOffsets.asJava, new ListOffsetsOptions()).all.get

    val offset: Long = offsets.get(topicPartition).offset()

    offset
  }
}
