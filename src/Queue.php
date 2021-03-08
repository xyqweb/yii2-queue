<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace yii\queue;

use Yii;
use yii\base\Application as BaseApp;
use yii\base\Component;
use yii\base\Event;
use yii\base\InvalidParamException;
use yii\di\Instance;
use yii\helpers\VarDumper;
use yii\queue\serializers\PhpSerializer;
use yii\queue\serializers\SerializerInterface;

/**
 * Base Queue
 *
 * @property null|int $workerPid
 * @since 2.0.2
 *
 * @author Roman Zhuravlev <zhuravljov@gmail.com>
 */
abstract class Queue extends Component
{
    /**
     * @event PushEvent
     */
    const EVENT_BEFORE_PUSH = 'beforePush';
    /**
     * @event PushEvent
     */
    const EVENT_AFTER_PUSH = 'afterPush';
    /**
     * @event ExecEvent
     */
    const EVENT_BEFORE_EXEC = 'beforeExec';
    /**
     * @event ExecEvent
     */
    const EVENT_AFTER_EXEC = 'afterExec';
    /**
     * @event ExecEvent
     */
    const EVENT_AFTER_ERROR = 'afterError';
    /**
     * @event ChangeQueueNameEvent
     */
    const EVENT_CHANGE_QUEUE_NAME = 'changeQueueName';
    /**
     * @see Queue::isWaiting()
     */
    const STATUS_WAITING = 1;
    /**
     * @see Queue::isReserved()
     */
    const STATUS_RESERVED = 2;
    /**
     * @see Queue::isDone()
     */
    const STATUS_DONE = 3;

    /**
     * @var bool whether to enable strict job type control.
     * Note that in order to enable type control, a pushing job must be [[JobInterface]] instance.
     * @since 2.0.1
     */
    public $strictJobType = true;
    /**
     * @var SerializerInterface|array
     */
    public $serializer = PhpSerializer::class;
    /**
     * @var int default time to reserve a job
     */
    public $ttr = 300;
    /**
     * @var int default attempt count
     */
    public $attempts = 1;
    /**
     * @var string new queue name
     */
    public $newQueueName = '';
    /**
     * @var int Length of retry interval
     */
    public $reconsumeTime = 60;

    /**
     * log driver object
     * @var object|null
     */
    protected $logDriver = null;

    /**
     * This property should be an integer indicating the maximum fail exec the queue should support. Default is 3
     *
     * @var int
     */
    public $maxFailNumber = 3;
    /**
     * @var null|string log driver name
     */
    public $log = NULL;
    /**
     * This property should be an string indicating the dependency complete event the queue should support，only support db transaction. Default is empty
     *
     * @var string
     */
    public $pushDependency = '';
    /**
     * This property should be an array. Default is empty
     *
     * @var array
     */
    protected $delayPushData = [];
    /**
     * This property should be an bool indicating the push dependency active the queue should support. Default is false
     *
     * @var bool
     */
    private $isActiveDependency = false;
    /**
     * @var int queue ttr
     */
    private $pushTtr;
    /**
     * @var int queue delay
     */
    private $pushDelay;
    /**
     * @var int queue priority
     */
    private $pushPriority;


    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        $this->serializer = Instance::ensure($this->serializer, SerializerInterface::class);
        if (is_string($this->log) && \Yii::$app->has($this->log)) {
            $logDriver = \Yii::$app->get($this->log);
            if (method_exists($logDriver, 'write')) {
                $this->logDriver = $logDriver;
                if (is_object($this->logDriver) && method_exists($this->logDriver, 'close')) {
                    Event::on(BaseApp::class, BaseApp::EVENT_AFTER_REQUEST, function () {
                        $this->logDriver->close();
                    });
                }
            }
            unset($logDriver);
        }
    }

    /**
     * Sets TTR for job execute.
     *
     * @param int|mixed $value
     * @return $this
     */
    public function ttr($value)
    {
        $this->pushTtr = $value;
        return $this;
    }

    /**
     * Sets delay for later execute.
     *
     * @param int|mixed $value
     * @return $this
     */
    public function delay($value)
    {
        $this->pushDelay = $value;
        return $this;
    }

    /**
     * Sets job priority.
     *
     * @param mixed $value
     * @return $this
     */
    public function priority($value)
    {
        $this->pushPriority = $value;
        return $this;
    }

    /**
     * 设置新的队列名称
     *
     * @author xyq
     * @param string $queueName
     * @return $this
     */
    public function queueName(string $queueName)
    {
        if (!empty($queueName)) {
            $this->newQueueName = $queueName;
        }
        return $this;
    }

    /**
     * Pushes job into queue
     *
     * @param JobInterface|mixed $job
     * @return string|null id of a job message
     */
    public function push($job)
    {
        $event = new PushEvent([
            'job'      => $job,
            'ttr'      => $job instanceof RetryableJobInterface
                ? $job->getTtr()
                : ($this->pushTtr ?: $this->ttr),
            'delay'    => $this->pushDelay ?: 0,
            'priority' => $this->pushPriority,
        ]);
        $this->pushTtr = null;
        $this->pushDelay = null;
        $this->pushPriority = null;

        $this->trigger(self::EVENT_BEFORE_PUSH, $event);
        if ($event->handled) {
            return null;
        }

        if ($this->strictJobType && !($event->job instanceof JobInterface)) {
            throw new InvalidParamException('Job must be instance of JobInterface.');
        }

        $message = $this->serializer->serialize($event->job);
        if (!empty($this->pushDependency) && 'db' == $this->pushDependency) {
            if (Yii::$app->db->getTransaction()) {
                $this->delayPushData[] = [
                    'event'     => $event,
                    'queueName' => $this->newQueueName,
                    'message'   => $message
                ];
                if (!$this->isActiveDependency) {
                    $this->isActiveDependency = true;
                    Event::on(\yii\db\Connection::class, \yii\db\Connection::EVENT_COMMIT_TRANSACTION, function () {
                        foreach ($this->delayPushData as $item) {
                            $this->queueName($item['queueName']);
                            $this->executePushEvent($item['event'],$item['message']);
                        }
                        $this->delayPushData = [];
                        $this->isActiveDependency = false;
                    });
                    Event::on(\yii\db\Connection::class, \yii\db\Connection::EVENT_ROLLBACK_TRANSACTION, function () {
                        $this->delayPushData = [];
                        $this->isActiveDependency = false;
                    });
                }
            } else {
                return $this->executePushEvent($event,$message);
            }
            return 'waiting for database transaction commit';
        } else {
            return $this->executePushEvent($event,$message);
        }
    }

    /**
     * execute Push Event
     *
     * @author xyq
     * @param $event
     * @param $message
     * @return string
     */
    private function executePushEvent($event,$message)
    {
        $event->id = $this->pushMessage($message, $event->ttr, $event->delay, $event->priority);
        $this->trigger(self::EVENT_AFTER_PUSH, $event);
        return $event->id;
    }

    /**
     * @param string $message
     * @param int $ttr time to reserve in seconds
     * @param int $delay
     * @param mixed $priority
     * @return string id of a job message
     */
    abstract protected function pushMessage($message, $ttr, $delay, $priority);

    /**
     * Uses for CLI drivers and gets process ID of a worker.
     *
     * @return null
     * @since 2.0.2
     */
    public function getWorkerPid()
    {
        return null;
    }

    /**
     * @param string $id of a job message
     * @param string $message
     * @param int $ttr time to reserve
     * @param int $attempt number
     * @return bool
     */
    protected function handleMessage($id, $message, $ttr, $attempt)
    {
        $job = $this->serializer->unserialize($message);
        if (!($job instanceof JobInterface)) {
            $dump = VarDumper::dumpAsString($job);
            throw new InvalidParamException("Job $id must be a JobInterface instance instead of $dump.");
        }

        $event = new ExecEvent([
            'id'      => $id,
            'job'     => $job,
            'ttr'     => $ttr,
            'attempt' => $attempt,
        ]);
        $this->trigger(self::EVENT_BEFORE_EXEC, $event);
        if ($event->handled) {
            return true;
        }
        try {
            if (method_exists($event->job, 'setMessageId')) {
                $event->job->setMessageId($id);
            }
            $result = $event->job->execute($this);
        } catch (\Exception $error) {
            $result = $this->handleError($event->id, $event->job, $event->ttr, $event->attempt, $error);
        } catch (\TypeError $error) {
            $result = $this->handleError($event->id, $event->job, $event->ttr, $event->attempt, $error);
        } catch (\Throwable $error) {
            $result = $this->handleError($event->id, $event->job, $event->ttr, $event->attempt, $error);
        }
        $this->trigger(self::EVENT_AFTER_EXEC, $event);
        return $result;
    }

    /**
     * @param string|null $id
     * @param JobInterface $job
     * @param int $ttr
     * @param int $attempt
     * @param \Exception|\Throwable $error
     * @return bool
     * @internal
     */
    public function handleError($id, $job, $ttr, $attempt, $error)
    {
        $errorData = [
            'id'          => $id,
            'job'         => $job,
            'ttr'         => $ttr,
            'attempt'     => $attempt,
            'error_msg'   => $error->getMessage(),
            'error_file'  => $error->getFile(),
            'error_line'  => $error->getLine(),
            'error_trace' => $error->getTraceAsString(),
            'retry'       => $job instanceof RetryableJobInterface
                ? $job->canRetry($attempt, $error)
                : $attempt < $this->attempts,
        ];
        $this->writeLog('queue/execute_error.log', $errorData);
        return !$errorData['retry'];
    }

    /**
     * 写入日志
     *
     * @author xyq
     * @param string $fileName
     * @param $content
     */
    protected function writeLog(string $fileName, $content)
    {
        if (method_exists($this->logDriver, 'write')) {
            $this->logDriver->write($fileName, $content);
        }
    }

    /**
     * @param string $id of a job message
     * @return bool
     */
    public function isWaiting($id)
    {
        return $this->status($id) === self::STATUS_WAITING;
    }

    /**
     * @param string $id of a job message
     * @return bool
     */
    public function isReserved($id)
    {
        return $this->status($id) === self::STATUS_RESERVED;
    }

    /**
     * @param string $id of a job message
     * @return bool
     */
    public function isDone($id)
    {
        return $this->status($id) === self::STATUS_DONE;
    }

    /**
     * @param string $id of a job message
     * @return int status code
     */
    abstract public function status($id);
}
