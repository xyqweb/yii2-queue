<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace yii\queue;

use yii\base\Component;
use yii\base\InvalidArgumentException;
use yii\di\Instance;
use yii\helpers\Console;
use yii\helpers\VarDumper;
use yii\queue\serializers\PhpSerializer;
use yii\queue\serializers\SerializerInterface;

/**
 * Base Queue.
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

    public $queueName = 'queue';

    private $pushTtr;
    private $pushDelay;
    private $pushPriority;


    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        $this->serializer = Instance::ensure($this->serializer, SerializerInterface::class);
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
     * Sets queue name.
     *
     * @param string $value
     * @return $this
     */
    public function queueName($value)
    {
        $this->queueName = $value;
        $this->resetQueueParams();
        return $this;
    }

    /**
     * Pushes job into queue.
     *
     * @param JobInterface|mixed $job
     * @return string|null id of a job message
     */
    public function push($job)
    {
        $this->initParams();
        
        $event = new PushEvent([
            'job' => $job,
            'ttr' => $this->pushTtr ?: (
            $job instanceof RetryableJobInterface
                ? $job->getTtr()
                : $this->ttr
            ),
            'delay' => $this->pushDelay ?: 0,
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
            throw new InvalidArgumentException('Job must be instance of JobInterface.');
        }

        $message = $this->serializer->serialize($event->job);
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
    protected function handleMessage($id, $message, $ttr, $attempt, $reconsumeTime=60)
    {
        $job = $this->serializer->unserialize($message);
        if (!($job instanceof JobInterface)) {
            $dump = VarDumper::dumpAsString($job);
            throw new InvalidArgumentException("Job $id must be a JobInterface instance instead of $dump.");
        }
        $event = new ExecEvent([
            'id' => $id,
            'job' => $job,
            'ttr' => $ttr,
            'attempt' => $attempt,
        ]);
        $this->trigger(self::EVENT_BEFORE_EXEC, $event);
        Console::output("Begin execute " . get_class($job));
        if ($event->handled) {
            return true;
        }
        $return = $result = true;
        $error = '';
        try {
            $res = $event->job->execute($this);
            ($res === false) ? $result = false : true;
        } catch (\Exception $error) {
            $return = $this->handleError($event->id, $event->job, $event->ttr, $event->attempt, $error);
            $result = false;
        } catch (\Throwable $error) {
            $return = $this->handleError($event->id, $event->job, $event->ttr, $event->attempt, $error);
            $result = false;
        }
        
        $this->trigger(self::EVENT_AFTER_EXEC, $event);
        if ($result === false && strpos(get_called_class(), 'amqp_interop')) {
            $this->handleFailMessage($message, $ttr, $reconsumeTime, null);
        }
        Console::output(get_class($job) . " execute " . ($result ? 'success ' : 'fail ') . $error);
        return $return;
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
        $event = new ErrorEvent([
            'id' => $id,
            'job' => $job,
            'ttr' => $ttr,
            'attempt' => $attempt,
            'error' => $error,
            'retry' => $job instanceof RetryableJobInterface
                ? $job->canRetry($attempt, $error)
                : $attempt < $this->attempts,
        ]);
        $this->trigger(self::EVENT_AFTER_ERROR, $event);
        return !$event->retry;
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

    /**
     *
     * 针对rabbitMQ的错误处理
     * @author xyq
     * @param $message
     * @param $ttr
     * @param $delay
     * @param $priority
     * @return bool
     */
    public function handleFailMessage($message, $ttr, $delay, $priority)
    {
        $this->initParams();
        echo $message;
        $key = md5($message);
        $num = 0;
        
        //redis缓存错误次数
        try {
            $cache = \Yii::$app->redis;
            $num = $cache->incr($key);
        }catch (\Exception $e){
            Console::output('redis connect fail!');
        }
        
        if($num >= $this->maxFailNum){
            $queueName       = $this->queueName;
            $routingKey      = $this->routingKey;
            $this->queueName = $this->errorQueueName;
            $this->routingKey=$this->errorRoutingKey;
            $this->pushMessage($message, $ttr, 0, $priority);
            $this->queueName = $queueName;
            $this->routingKey=$routingKey;
            return true;
        }
        $this->pushMessage($message, $ttr, $delay, $priority);
        $cache->expire($key, 3600);
    }
    
    public function resetQueueParams()
    {
        $this->routingKey = null;
        $this->errorQueueName = null;
        $this->errorRoutingKey = null;
    }
    
    abstract protected function initParams();
}
