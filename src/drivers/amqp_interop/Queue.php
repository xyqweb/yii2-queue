<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace yii\queue\amqp_interop;

use Enqueue\AmqpBunny\AmqpConnectionFactory as AmqpBunnyConnectionFactory;
use Enqueue\AmqpExt\AmqpConnectionFactory as AmqpExtConnectionFactory;
use Enqueue\AmqpLib\AmqpConnectionFactory as AmqpLibConnectionFactory;
use Enqueue\AmqpTools\DelayStrategyAware;
use Enqueue\AmqpTools\RabbitMqDlxDelayStrategy;
use Interop\Amqp\AmqpConnectionFactory;
use Interop\Amqp\AmqpConsumer;
use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpMessage;
use Interop\Amqp\AmqpQueue;
use Interop\Amqp\AmqpTopic;
use Interop\Amqp\Impl\AmqpBind;
use yii\base\Application as BaseApp;
use yii\base\Event;
use yii\base\NotSupportedException;
use yii\queue\cli\Queue as CliQueue;

/**
 * Amqp Queue.
 *
 * @author Maksym Kotliar <kotlyar.maksim@gmail.com>
 * @since 2.0.2
 */
class Queue extends CliQueue
{
    const ATTEMPT = 'yii-attempt';
    const TTR = 'yii-ttr';
    const DELAY = 'yii-delay';
    const PRIORITY = 'yii-priority';
    const ENQUEUE_AMQP_LIB = 'enqueue/amqp-lib';
    const ENQUEUE_AMQP_EXT = 'enqueue/amqp-ext';
    const ENQUEUE_AMQP_BUNNY = 'enqueue/amqp-bunny';

    /**
     * The connection to the borker could be configured as an array of options
     * or as a DSN string like amqp:, amqps:, amqps://user:pass@localhost:1000/vhost.
     *
     * @var string
     */
    public $dsn;
    /**
     * The message queue broker's host.
     *
     * @var string|null
     */
    public $host;
    /**
     * The message queue broker's port.
     *
     * @var string|null
     */
    public $port;
    /**
     * This is RabbitMQ user which is used to login on the broker.
     *
     * @var string|null
     */
    public $user;
    /**
     * This is RabbitMQ password which is used to login on the broker.
     *
     * @var string|null
     */
    public $password;
    /**
     * Virtual hosts provide logical grouping and separation of resources.
     *
     * @var string|null
     */
    public $vhost;
    /**
     * The time PHP socket waits for an information while reading. In seconds.
     *
     * @var float|null
     */
    public $readTimeout;
    /**
     * The time PHP socket waits for an information while witting. In seconds.
     *
     * @var float|null
     */
    public $writeTimeout;
    /**
     * The time RabbitMQ keeps the connection on idle. In seconds.
     *
     * @var float|null
     */
    public $connectionTimeout;
    /**
     * The periods of time PHP pings the broker in order to prolong the connection timeout. In seconds.
     *
     * @var float|null
     */
    public $heartbeat;
    /**
     * PHP uses one shared connection if set true.
     *
     * @var bool|null
     */
    public $persisted;
    /**
     * The connection will be established as later as possible if set true.
     *
     * @var bool|null
     */
    public $lazy;
    /**
     * If false prefetch_count option applied separately to each new consumer on the channel
     * If true prefetch_count option shared across all consumers on the channel.
     *
     * @var bool|null
     */
    public $qosGlobal;
    /**
     * Defines number of message pre-fetched in advance on a channel basis.
     *
     * @var int|null
     */
    public $qosPrefetchSize;
    /**
     * Defines number of message pre-fetched in advance per consumer.
     *
     * @var int|null
     */
    public $qosPrefetchCount;
    /**
     * Defines whether secure connection should be used or not.
     *
     * @var bool|null
     */
    public $sslOn;
    /**
     * Require verification of SSL certificate used.
     *
     * @var bool|null
     */
    public $sslVerify;
    /**
     * Location of Certificate Authority file on local filesystem which should be used with the verify_peer context option to authenticate the identity of the remote peer.
     *
     * @var string|null
     */
    public $sslCacert;
    /**
     * Path to local certificate file on filesystem.
     *
     * @var string|null
     */
    public $sslCert;
    /**
     * Path to local private key file on filesystem in case of separate files for certificate (local_cert) and private key.
     *
     * @var string|null
     */
    public $sslKey;
    /**
     * The queue used to consume messages from.
     *
     * @var string
     */
    public $queueName = 'interop_queue';
    /**
     * The exchange used to publish messages to.
     *
     * @var string
     */
    public $exchangeName = 'exchange';
    /**
     * Defines the amqp interop transport being internally used. Currently supports lib, ext and bunny values.
     *
     * @var string
     */
    public $driver = self::ENQUEUE_AMQP_LIB;
    /**
     * This property should be an integer indicating the maximum priority the queue should support. Default is 10.
     *
     * @var int
     */
    public $maxPriority = 10;
    /**
     * 队列类型
     * @var array
     */
    public $queueArguments = NULL;

    /**
     * The property contains a command class which used in cli.
     *
     * @var string command class name
     */
    public $commandClass = Command::class;

    /**
     * Amqp interop context
     *
     * @var AmqpContext
     */
    protected $context;

    /**
     * Amqp interop context
     *
     * @var AmqpConnectionFactory
     */
    protected $connection;

    /**
     * Amqp interop context
     *
     * @var AmqpContext
     */
    protected $consumerContext;

    /**
     * Amqp interop context
     *
     * @var AmqpConnectionFactory
     */
    protected $consumerConnection;
    /**
     * List of supported amqp interop drivers.
     *
     * @var string[]
     */
    protected $supportedDrivers = [self::ENQUEUE_AMQP_LIB, self::ENQUEUE_AMQP_EXT, self::ENQUEUE_AMQP_BUNNY];
    /**
     * The property tells whether the setupBroker method was called or not.
     * Having it we can do broker setup only once per process.
     *
     * @var bool
     */
    protected $setupBrokerDone = false;
    /**
     * The property tells whether the setupBroker method was called or not.
     * Having it we can do broker setup only once per process.
     *
     * @var bool
     */
    protected $consumerSetupBrokerDone = false;


    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        Event::on(BaseApp::class, BaseApp::EVENT_AFTER_REQUEST, function () {
            $this->close('all');
        });
    }

    /**
     * Listens amqp-queue and runs new jobs.
     */
    public function listen()
    {
        $listenQueue = '';
        $exception = null;
        while (true) {
            if (empty($listenQueue)) {
                $listenQueue = $this->queueName;
            } elseif ($listenQueue !== $this->queueName) {
                $this->queueName = $listenQueue;
                $this->consumerSetupBrokerDone = false;
            }
            try {
                $this->open('consumer');
                $this->setupBroker('consumer');
                $queue = $this->consumerContext->createQueue($this->queueName);
                $consumer = $this->consumerContext->createConsumer($queue);
                $subscriptionConsumer = $this->consumerContext->createSubscriptionConsumer();
                $subscriptionConsumer->subscribe($consumer, function (AmqpMessage $message, AmqpConsumer $consumer) {
                    if ($message->isRedelivered()) {
                        $consumer->acknowledge($message);
                        $this->redeliver($message);
                        return true;
                    }
                    $ttr = $message->getProperty(self::TTR);
                    $attempt = $message->getProperty(self::ATTEMPT, 1);
                    $this->writeLog('queue/queue_consumer.log', ' queueName:' . $this->queueName . ',messageId:' . $message->getMessageId() . ' payload:' . $message->getBody());
                    if ($this->handleMessage($message->getMessageId(), $message->getBody(), $ttr, $attempt)) {
                        $consumer->acknowledge($message);
                    } else {
                        $consumer->acknowledge($message);
                        $this->redeliver($message);
                    }
                    return true;
                });
                $subscriptionConsumer->consume(300000);
            } catch (\Exception $exception) {
            } catch (\Throwable $exception) {
            } finally {
                $this->close('consumer');
                sleep(1);
            }
            if (!is_null($exception)) {
                $this->logDriver->write('queue/queue_consumer.log', 'consumer process error ,restart in 1 second, error message:' . $exception->getMessage() . ',file:' . $exception->getFile() . ',line:' . $exception->getLine());
                $exception = null;
            }
        }
    }

    /**
     * @return AmqpContext
     */
    public function getContext()
    {
        $this->open();
        return $this->context;
    }

    /**
     * @inheritdoc
     *
     * @author xyq
     * @param string $payload
     * @param int $ttr
     * @param int $delay
     * @param mixed $priority
     * @return string|null
     * @throws \Interop\Queue\Exception
     */
    protected function pushMessage($payload, $ttr, $delay, $priority)
    {
        //在同一个连接内推送不同的队列时需要重新申明当前连接内队列名
        if (!empty($this->newQueueName) && $this->newQueueName !== $this->queueName) {
            $this->setupBrokerDone = false;
            $this->queueName = $this->newQueueName;
            $this->newQueueName = '';
        }
        $this->open();
        $this->setupBroker();
        $topic = $this->context->createTopic($this->exchangeName);

        $message = $this->context->createMessage($payload);
        $message->setDeliveryMode(AmqpMessage::DELIVERY_MODE_PERSISTENT);
        $message->setMessageId(uniqid('', true));
        $message->setTimestamp(time());
        $message->setProperty(self::ATTEMPT, 1);
        $message->setProperty(self::TTR, $ttr);
        $message->setRoutingKey($this->queueName . 'Key');

        $producer = $this->context->createProducer();

        if ($delay) {
            $message->setProperty(self::DELAY, $delay);
            $producer->setDeliveryDelay($delay * 1000);
        }

        if ($priority) {
            $message->setProperty(self::PRIORITY, $priority);
            $producer->setPriority($priority);
        }

        $producer->send($topic, $message);

        $messageId = $message->getMessageId();
        $this->writeLog('queue/queue_push.log', 'queueName:' . $this->queueName . ' messageId:' . $messageId . ' payload:' . $payload);
        return $messageId;
    }

    /**
     * @inheritdoc
     *
     * @param string $id
     * @return int|void
     * @throws NotSupportedException
     */
    public function status($id)
    {
        throw new NotSupportedException('Status is not supported in the driver.');
    }

    /**
     * Opens connection and channel
     * @param string $type
     */
    protected function open($type = 'push')
    {
        if (($type == 'consumer' && $this->consumerContext) || ($type == 'push' && $this->context)) {
            return;
        }
        switch ($this->driver) {
            case self::ENQUEUE_AMQP_LIB:
                $connectionClass = AmqpLibConnectionFactory::class;

                break;
            case self::ENQUEUE_AMQP_EXT:
                $connectionClass = AmqpExtConnectionFactory::class;

                break;
            case self::ENQUEUE_AMQP_BUNNY:
                $connectionClass = AmqpBunnyConnectionFactory::class;

                break;

            default:
                throw new \LogicException(sprintf('The given driver "%s" is not supported. Drivers supported are "%s"', $this->driver, implode('", "', $this->supportedDrivers)));
        }

        $config = [
            'dsn'                => $this->dsn,
            'host'               => $this->host,
            'port'               => $this->port,
            'user'               => $this->user,
            'pass'               => $this->password,
            'vhost'              => $this->vhost,
            'read_timeout'       => $this->readTimeout,
            'write_timeout'      => $this->writeTimeout,
            'connection_timeout' => $this->connectionTimeout,
            'heartbeat'          => $this->heartbeat,
            'persisted'          => $this->persisted,
            'lazy'               => $this->lazy,
            'qos_global'         => $this->qosGlobal,
            'qos_prefetch_size'  => $this->qosPrefetchSize,
            'qos_prefetch_count' => $this->qosPrefetchCount,
            'ssl_on'             => $this->sslOn,
            'ssl_verify'         => $this->sslVerify,
            'ssl_cacert'         => $this->sslCacert,
            'ssl_cert'           => $this->sslCert,
            'ssl_key'            => $this->sslKey,
        ];

        $config = array_filter($config, function ($value) {
            return null !== $value;
        });

        /** @var AmqpConnectionFactory $factory */
        $factory = new $connectionClass($config);
        if ('consumer' == $type) {
            $this->consumerConnection = $factory;
            $this->consumerContext = $factory->createContext();
            if ($this->consumerContext instanceof DelayStrategyAware) {
                $this->consumerContext->setDelayStrategy(new RabbitMqDlxDelayStrategy());
            }
        } else {
            $this->connection = $factory;
            $this->context = $factory->createContext();
            if ($this->context instanceof DelayStrategyAware) {
                $this->context->setDelayStrategy(new RabbitMqDlxDelayStrategy());
            }
        }
    }

    /**
     * 启动队列配置
     *
     * @author xyq
     * @param string $type
     */
    protected function setupBroker($type = 'push')
    {
        $queueArguments = ['x-max-priority' => $this->maxPriority];
        if (is_array($this->queueArguments) && !empty($this->queueArguments)) {
            $queueArguments = array_merge($queueArguments, $this->queueArguments);
        }
        $routingKey = $this->queueName . 'Key';
        if ($type == 'consumer') {
            if ($this->consumerSetupBrokerDone) {
                return;
            }
            $queue = $this->consumerContext->createQueue($this->queueName);
            $queue->addFlag(AmqpQueue::FLAG_DURABLE);
            $queue->setArguments($queueArguments);
            $this->consumerContext->declareQueue($queue);
            $topic = $this->consumerContext->createTopic($this->exchangeName);
            $topic->setType(AmqpTopic::TYPE_DIRECT);
            $topic->addFlag(AmqpTopic::FLAG_DURABLE);
            $this->consumerContext->declareTopic($topic);
            $this->consumerContext->bind(new AmqpBind($queue, $topic, $routingKey));
            $this->setupBrokerDone = true;
        } else {
            if ($this->setupBrokerDone) {
                return;
            }
            $queue = $this->context->createQueue($this->queueName);
            $queue->addFlag(AmqpQueue::FLAG_DURABLE);
            $queue->setArguments($queueArguments);
            $this->context->declareQueue($queue);

            $topic = $this->context->createTopic($this->exchangeName);
            $topic->setType(AmqpTopic::TYPE_DIRECT);
            $topic->addFlag(AmqpTopic::FLAG_DURABLE);
            $this->context->declareTopic($topic);
            $this->context->bind(new AmqpBind($queue, $topic, $routingKey));
            $this->setupBrokerDone = true;
        }
    }

    /**
     * Closes connection and channel
     * @param string $type
     */
    protected function close($type = 'all')
    {
        if ('consumer' == $type) {
            if ($this->consumerConnection) {
                $this->consumerConnection->close();
            }
            $this->consumerContext = $this->consumerConnection = null;
            $this->consumerSetupBrokerDone = false;
        } elseif ('push' == $type) {
            if ($this->connection) {
                $this->connection->close();
            }
            $this->context = $this->connection = null;
            $this->setupBrokerDone = false;
        } else {
            if ($this->consumerConnection) {
                $this->consumerConnection->close();
            }
            $this->consumerContext = $this->consumerConnection = null;
            $this->consumerSetupBrokerDone = false;
            if ($this->connection) {
                $this->connection->close();
            }
            $this->context = $this->connection = null;
            $this->setupBrokerDone = false;
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function redeliver(AmqpMessage $message)
    {
        $attempt = $message->getProperty(self::ATTEMPT, 1);
        ++$attempt;
        $newQueueName = $oldQueueName = $this->queueName;
        $delay = 0;
        if ($attempt > $this->maxFailNumber) {
            $newQueueName .= 'Error';
            $this->setupBrokerDone = false;
        } else {
            $delay = $this->reconsumeTime;
        }
        $this->queueName = $newQueueName;
        $this->open();
        $this->setupBroker();
        $topic = $this->context->createTopic($this->exchangeName);
        $newMessage = $this->context->createMessage($message->getBody(), $message->getProperties(), $message->getHeaders());
        $newMessage->setDeliveryMode($message->getDeliveryMode());
        $newMessage->setProperty(self::ATTEMPT, $attempt);
        $newMessage->setRoutingKey($this->queueName . 'Key');
        $producer = $this->context->createProducer();
        if ($delay) {
            $newMessage->setProperty(self::DELAY, $delay);
            $producer->setDeliveryDelay($delay * 1000);
        }
        $producer->send(
            $topic,
            $newMessage
        );
        $this->close('push');
        $this->writeLog('queue/redeliver_push.log', 'queueName:' . $this->queueName . ' messageId:' . $newMessage->getMessageId() . ' payload:' . $message->getBody());
        $this->queueName = $oldQueueName;
        unset($newMessage, $producer, $message);
    }
}
