#include "rdkafkacpp.h"
#include <iostream>
#include <string>
#include <chrono>
#include <thread>

// CallBack
class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb(RdKafka::Message &message) {
    /* If message.err() is non-zero the message delivery failed permanently
     * for the message. */
    std::cerr << "[CALLBACK] ";
    if (message.err())
      std::cerr << "% Message delivery failed: " << message.errstr()
                << std::endl;
    else
      std::cerr << "% Message delivered to topic " << message.topic_name()
                << " [" << message.partition() << "] at offset "
                << message.offset() << std::endl;
  }
};

int main()
{
    std::string errmsg;
    ExampleDeliveryReportCb ex_dr_cb;
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    
    // configuration reference
    // https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    conf->set("bootstrap.servers", "127.0.0.1:9092,127.0.0.1:9093", errmsg);
    conf->set("dr_cb", &ex_dr_cb, errmsg);

    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errmsg);

    std::string topic = "newtopic";
    std::string message = "abcdefg";

    delete conf;

retry:
    RdKafka::ErrorCode err = producer->produce(
        topic,
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(message.data()), 
        message.length(),
        NULL, 0, 0, NULL, NULL
    );

    std::cerr << "[MAIN] ";
    if (err != RdKafka::ERR_NO_ERROR) {
        std::cerr << "Failed to produce to topic " 
                    << topic << ": "
                    << RdKafka::err2str(err) << std::endl;

        if (err == RdKafka::ERR__QUEUE_FULL) {
            producer->poll(1000 /*block for max 1000ms*/);
            goto retry;
        }
    } else {
        std::cerr << "Enqueued message (" 
                    << message.size() << " bytes) "
                    << "for topic " << topic << std::endl;
    }

    //producer->flush(10 * 1000 /* wait for max 10 seconds */);
    producer->flush(0);
    std::cerr << "[MAIN] ";
    if (producer->outq_len() > 0) {
        std::cerr << "% " 
                << producer->outq_len()
                << " message(s) were not delivered" << std::endl;
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));
    goto retry;
    
    delete producer;
    return 0;
}