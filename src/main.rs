use tokio::runtime;
use tokio::sync::mpsc::channel;
use tokio::time::{timeout, Duration};

use futures::prelude::*;

use chrono::prelude::*;
use clap::{App, Arg};
use futures_util::future::ready;
use log::{error, info, warn};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use std::io::Write;
use std::thread;

use trust_dns_proto::rr::record_type::RecordType;
use trust_dns_proto::xfer::dns_request::DnsRequestOptions;
use trust_dns_resolver::config::{ResolverConfig, ResolverOpts};
use trust_dns_resolver::TokioAsyncResolver;

use serde_derive::*;

pub fn setup_logger(log_thread: bool, rust_log: Option<&str>) {
    let mut builder = env_logger::Builder::new();
    builder
        .format(move |buf, record| {
            let thread_name = if log_thread {
                format!("(t: {}) ", thread::current().name().unwrap_or("unknown"))
            } else {
                "".to_string()
            };

            let local_time: DateTime<Local> = Local::now();
            let time_str = local_time.format("%H:%M:%S%.3f").to_string();
            writeln!(
                buf,
                "{} {}{} - {} - {}\n",
                time_str,
                thread_name,
                record.level(),
                record.target(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info);

    rust_log.map(|conf| builder.parse_filters(conf));

    builder.init();
}

#[derive(Serialize, Deserialize)]
struct StringValue {
    value: String,
}

#[derive(Serialize, Deserialize)]
struct TSValue {
    value: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[allow(non_snake_case)]
enum DomainCmds {
    ValidateDomain {
        id: StringValue,
        client: StringValue,
        term: StringValue,
        domain: StringValue,
        version: StringValue,
        ts: TSValue,
        source: StringValue,
        correlationId: StringValue,
    },
}

// Creates all the resources and runs the event loop. The event loop will:
fn run_async_processor(brokers: &str, group_id: &str, input_topic: &str, output_topic: &str) {
    info!(
        "Group: {:?}\nInput Topic: {:?}\nOutput Topic: {:?}",
        group_id, input_topic, output_topic
    );

    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        //.set("auto.offset.reset", "error")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[input_topic])
        .expect("Can't subscribe to specified topic");

    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();

    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // Get a new resolver with the google nameservers as the upstream recursive resolvers
    let mut opts = ResolverOpts::default();
    opts.cache_size = 0;
    //let resolver = Resolver::new(ResolverConfig::cloudflare(), opts).unwrap();
    let resolver = TokioAsyncResolver::tokio(ResolverConfig::cloudflare(), opts);

    let resolver = rt.block_on(resolver).expect("failed to connect resolver");
    rt.spawn(resolver.lookup("telefonica.com", RecordType::AAAA, DnsRequestOptions::default()));

    let output_topic = output_topic.to_string();

    info!("Starting... using #cpus = {:?}", num_cpus::get());

    let (mut s, mut r) = channel::<rdkafka::message::OwnedMessage>(1); // TODO: make it configurable
    let (mut s_ack, mut r_ack) = channel::<rdkafka::message::OwnedMessage>(1); // TODO: make it configurable

    let _worker = rt.spawn(async move {
        loop {
            match r.recv().await {
                // awaiting messages that could be raced against a timeout in order to get a clean way of stopping
                Some(msg) => {
                    info!(
                        "Worker receving msg with topic [{:?}] partition [{:?}] offset [{:?}]",
                        msg.topic(),
                        msg.partition(),
                        msg.offset()
                    );

                    let producer = producer.clone();

                    let (msg_pl, domain) = match msg.payload_view::<str>() {
                        Some(Ok(pl)) => match serde_json::from_str::<DomainCmds>(&pl) {
                            Ok(o) => match o {
                                DomainCmds::ValidateDomain {
                                    id: _,
                                    client: _,
                                    term: _,
                                    domain,
                                    version: _,
                                    source: _,
                                    ts: _,
                                    correlationId: _,
                                } => (pl, Ok(domain.value.to_owned())),
                            },
                            Err(serr) => (pl, Err(serr.to_string())),
                        },
                        Some(Err(_)) => ("", Err("Message payload is not a string".to_owned())),
                        None => ("", Err("No payload".to_owned())),
                    };

                    match domain {
                        Ok(domain) => {
                            let domain2 = domain.clone();
                            let tr = timeout(
                                    Duration::from_secs(1), // TODO: Make it configurable
                                    async {
                                        Ok::<_, String>(futures_util::future::join4(
                                            async {
                                                Ok::<_, String>(
                                                    match resolver
                                                        .lookup(domain2.clone(), RecordType::AAAA, DnsRequestOptions::default()).await
                                                    {
                                                        Ok(resolution) => {
                                                            format!("{:?}", resolution)
                                                        }
                                                        Err(err) => {
                                                            format!("Resolver Error [{:?}]", err)
                                                        }
                                                    },
                                                )
                                            },
                                            async {
                                                Ok::<_, String>(
                                                    match resolver
                                                        .lookup(domain2.clone(), RecordType::A, DnsRequestOptions::default()).await
                                                    {
                                                        Ok(resolution) => {
                                                            format!("{:?}", resolution)
                                                        }
                                                        Err(err) => {
                                                            format!("Resolver Error [{:?}]", err)
                                                        }
                                                    },
                                                )
                                            },
                                            async {
                                                Ok::<_, String>(
                                                    match resolver
                                                        .lookup(domain2.clone(), RecordType::MX, DnsRequestOptions::default()).await
                                                    {
                                                        Ok(resolution) => {
                                                            format!("{:?}", resolution)
                                                        }
                                                        Err(err) => {
                                                            format!("Resolver Error [{:?}]", err)
                                                        }
                                                    },
                                                )
                                            },
                                            async {
                                                Ok::<_, String>(
                                                    match resolver
                                                        .lookup(domain2.clone(), RecordType::NS, DnsRequestOptions::default()).await
                                                    {
                                                        Ok(resolution) => {
                                                            format!("{:?}", resolution)
                                                        }
                                                        Err(err) => {
                                                            format!("Resolver Error [{:?}]", err)
                                                        }
                                                    },
                                                )
                                            },
                                        )
                                        .await)
                                    },
                                )
                                .await;

                            if tr.is_ok() {
                                if let Some(r) = tr.iter().next() {
                                    match r {
                                    Ok((resolved_aaaa, resolved_a, resolved_mx, resolved_ns)) => {
                                        let resolved = format!(
                                            "topic [{:?}] partition [{:?}] offset [{:?}] domain [{}] resolutions: \nAAAA [{:?}] \nA [{:?}] \nMX [{:?}] \nNS [{:?}]",
                                            msg.topic(), msg.partition(), msg.offset(), domain, resolved_aaaa, resolved_a, resolved_mx, resolved_ns
                                        );

                                        info!("{}", resolved);
                                    },
                                    Err(te) => error!("Error processing with topic [{:?}] partition [{:?}] offset [{:?}] error [{:?}]", msg.topic(), msg.partition(), msg.offset(), te),
                                };
                            };
                        } else {
                            error!("Error processing with topic [{:?}] partition [{:?}] offset [{:?}] error [{:?}]", msg.topic(), msg.partition(), msg.offset(), tr.err());
                        };

                        let producer_future = producer
                            .send(
                                FutureRecord::to(&output_topic)
                                    .key("some key")
                                    .payload(&msg_pl.to_owned()),
                                0,
                            )
                            .then(move |result| {
                                match result {
                                    Ok(delivery) => {
                                        info!(
                                            "Producer: msg processed successfully with delivery [{:?}]",
                                            delivery
                                        );
                                    }
                                    Err(e) => error!("Producer: Error processing [{:?}]", e),
                                }

                                ready(())
                            });

                        producer_future.await;
                    },
                    Err(te) => error!("Error processing with topic [{:?}] partition [{:?}] offset [{:?}] error [{:?}]", msg.topic(), msg.partition(), msg.offset(), te),
                }

                info!("Sending to s_ack channel handler msg with topic [{:?}] partition [{:?}] offset [{:?}]", msg.topic(), msg.partition(), msg.offset());
                let _ = s_ack.send(msg).await;
            },
            None => error!("Getting None from channel receive"),
            }
        }
    });

    info!("Starting event loop");
    let _ = rt.block_on(async {
        let mut stream_processor = consumer.start();
        while let Some(value) = stream_processor.next().await {
            match value {
                Ok(borrowed_message) => {
                    let owned_message = borrowed_message.detach();

                    let _ = s.send(owned_message).await;

                    match r_ack.recv().await {
                        Some(msg) => {
                            info!("Recevinging from r_ack channel handler msg with topic [{:?}] partition [{:?}] offset [{:?}]", msg.topic(), msg.partition(), msg.offset());

                            info!(
                                "Committing msg topic [{}] partition [{}] offset [{}]",
                                borrowed_message.topic(),
                                borrowed_message.partition(),
                                borrowed_message.offset()
                            );

                            match consumer.commit_message(&borrowed_message, CommitMode::Sync) {
                            Ok(()) => (),
                            _ => error!("Error commiting msg topic [{}] partition [{}] offset [{}]",
                                borrowed_message.topic(),
                                borrowed_message.partition(),
                                borrowed_message.offset()),
                            };

                            info!(
                                "Committed msg topic [{}] partition [{}] offset [{}]",
                                borrowed_message.topic(),
                                borrowed_message.partition(),
                                borrowed_message.offset()
                            );
                        }
                        None => error!("Unexpected situation receiving 'None' from r_ack receive channel handler."),
                    }
                }
                Err(kafka_error) => {
                    warn!("Error while receiving from Kafka: {:?}", kafka_error);
                }
            }
        }
    });

    info!("Stream processing terminated");
}

fn main() {
    let matches = App::new("Domain Validator")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("An active Domain Validator in Rust [PoC-WIP]")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("input-topic")
                .long("input-topic")
                .help("Input topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("output-topic")
                .long("output-topic")
                .help("Output topic")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let input_topic = matches.value_of("input-topic").unwrap();
    let output_topic = matches.value_of("output-topic").unwrap();

    run_async_processor(brokers, group_id, input_topic, output_topic);
}
