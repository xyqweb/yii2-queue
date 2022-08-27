<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace yii\queue\cli;

use Symfony\Component\Process\Exception\ProcessTimedOutException;
use Symfony\Component\Process\Process;
use yii\console\Controller;
use yii\console\ExitCode;

/**
 * Class Command
 *
 * @author Roman Zhuravlev <zhuravljov@gmail.com>
 */
abstract class Command extends Controller
{
    /**
     * @var Queue
     */
    public $queue;
    /**
     * @var bool verbose mode of a job execute. If enabled, execute result of each job
     * will be printed.
     */
    public $verbose = false;
    /**
     * @var array additional options to the verbose behavior.
     * @since 2.0.2
     */
    public $verboseConfig = [
        'class' => VerboseBehavior::class,
    ];
    /**
     * @var bool isolate mode. It executes a job in a child process.
     */
    public $isolate = true;
    /**
     * @var null|int The periods of time PHP pings the broker in order to prolong the connection timeout. In seconds..
     */
    public $heartbeat = null;

    /**
     * @inheritdoc
     */
    public function options($actionID)
    {
        $options = parent::options($actionID);
        if ($this->canVerbose($actionID)) {
            $options[] = 'verbose';
        }
        if ($this->canIsolate($actionID)) {
            $options[] = 'isolate';
        }
        $options[] = 'heartbeat';
        return $options;
    }

    /**
     * @inheritdoc
     */
    public function optionAliases()
    {
        return array_merge(parent::optionAliases(), [
            'v' => 'verbose',
        ]);
    }

    /**
     * @param string $actionID
     * @return bool
     * @since 2.0.2
     */
    abstract protected function isWorkerAction($actionID);

    /**
     * @param string $actionID
     * @return bool
     */
    protected function canVerbose($actionID)
    {
        return $actionID === 'exec' || $actionID === 'exec-file' || $actionID === 'timeout' || $this->isWorkerAction($actionID);
    }

    /**
     * @param string $actionID
     * @return bool
     */
    protected function canIsolate($actionID)
    {
        return $this->isWorkerAction($actionID);
    }

    /**
     * @inheritdoc
     */
    public function beforeAction($action)
    {
        if ($this->canVerbose($action->id) && $this->verbose) {
            $this->queue->attachBehavior('verbose', ['command' => $this] + $this->verboseConfig);
        }

        if ($this->canIsolate($action->id) && $this->isolate) {
            $this->queue->messageHandler = function ($id, $message, $ttr, $attempt) {
                return $this->handleMessage($id, $message, $ttr, $attempt);
            };
        } else {
            $this->queue->messageHandler = null;
        }

        return parent::beforeAction($action);
    }

    /**
     * Executes a job.
     * The command is internal, and used to isolate a job execution. Manual usage is not provided.
     *
     * @param string|null $id of a message
     * @param int $ttr time to reserve
     * @param int $attempt number
     * @param int $pid of a worker
     * @return int exit code
     * @internal It is used with isolate mode.
     */
    public function actionExec($id, $ttr, $attempt, $pid)
    {
        if ($this->queue->execute($id, file_get_contents('php://stdin'), $ttr, $attempt, $pid)) {
            return ExitCode::OK;
        }

        return ExitCode::UNSPECIFIED_ERROR;
    }

    /**
     * golang push a job file
     *
     * @author xyq
     * @param $queue
     * @param $filename
     * @return int exit code
     */
    public function actionExecFile($queue, $filename)
    {
        if (empty($queue) || !ctype_alnum($queue)) {
            echo 'Error：queue name not exist';
            $this->queue->writeLog('queue/queue_consumer.log', ['queue' => $queue, 'filename' => $filename]);
            return ExitCode::NOINPUT;
        }
        $jobData = $this->formatFile($filename);
        if(is_string($jobData)){
            echo $jobData;
            $this->queue->writeLog('queue/queue_consumer.log', ['queue' => $queue, 'filename' => $filename]);
            return ExitCode::DATAERR;
        }
        $this->queue->writeLog('queue/queue_consumer.log', $jobData);
        if ($this->queue->execute($jobData['messageId'], $jobData['body'], $jobData['ttr'], $jobData['attempt'], $jobData['pid'])) {
            return ExitCode::OK;
        }
        $this->queue->redeliverJob($jobData);
        return ExitCode::OK;
    }

    /**
     * golang push a job file
     *
     * @author xyq
     * @param $filename
     * @return int exit code
     */
    public function actionTimeout($filename)
    {
        $jobData = $this->formatFile($filename);
        if (is_string($jobData)) {
            echo $jobData;
            $this->queue->writeLog('queue/exec_timeout.log', ['data' => $jobData, 'filename' => $filename]);
            return ExitCode::DATAERR;
        }
        $this->queue->redeliverJob($jobData);
        return ExitCode::OK;
    }

    /**
     * 加密文件并格式化内容
     *
     * @author xyq
     * @param $filename
     * @return array|string
     */
    protected function formatFile($filename)
    {
        $filename = base64_decode($filename);
        if (!is_string($filename) || !file_exists($filename)) {
            return 'Error：file is not exist';
        }
        $data = file_get_contents($filename);
        if (empty($data)) {
            return 'Error：file is empty';
        }
        $data = json_decode($data, true);
        if (!is_array($data) || empty($data)) {
            return 'queue data is not array';
        }
        return $data;
    }

    /**
     * Handles message using child process.
     *
     * @param string|null $id of a message
     * @param string $message
     * @param int $ttr time to reserve
     * @param int $attempt number
     * @return bool
     * @throws
     * @see actionExec()
     */
    protected function handleMessage($id, $message, $ttr, $attempt)
    {
        $ttr = floatval(is_numeric($ttr) ? $ttr : 300);
        // Executes child process
        $cmd = strtr('php yii queue/exec "id" "ttr" "attempt" "pid"', [
            'php' => PHP_BINARY,
            'yii' => $_SERVER['SCRIPT_FILENAME'],
            'queue' => $this->uniqueId,
            'id' => $id,
            'ttr' => $ttr,
            'attempt' => $attempt,
            'pid' => $this->queue->getWorkerPid(),
        ]);
        foreach ($this->getPassedOptions() as $name) {
            if (in_array($name, $this->options('exec'), true)) {
                $cmd .= ' --' . $name . '=' . $this->$name;
            }
        }
        if (!in_array('color', $this->getPassedOptions(), true)) {
            $cmd .= ' --color=' . $this->isColorEnabled();
        }

        $process = new Process($cmd, null, null, $message, $ttr);
        try {
            $process->run(function ($type, $buffer) {
                if ($type === Process::ERR) {
                    $this->stderr($buffer);
                } else {
                    $this->stdout($buffer);
                }
            });
        } catch (ProcessTimedOutException $error) {
            $job = $this->queue->serializer->unserialize($message);
            return $this->queue->handleError($id, $job, $ttr, $attempt, $error);
        }

        return $process->isSuccessful();
    }
}
