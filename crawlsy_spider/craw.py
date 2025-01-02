import time

import redis
from rq import Queue, Worker, Retry


class CrawLsy:

    def __init__(self,
                 key: str,
                 host: str = 'localhost',
                 port=6379,
                 db=0,
                 password=None,
                 maxsize: int = None,
                 is_async=True):
        """
        初始化调度器实例

        :param key: 任务key
        :param host: redis host
        :param port: redis port
        :param db: redis db
        :param password: redis password
        :param maxsize: redis中最大任务数
        :param is_async: 设置为 False 时 submit 就会执行，适合本地调试时使用
        """
        self.redis = redis.Redis(host=host, port=port, db=db, password=password)

        self._queue = Queue(
            key,
            connection=self.redis,
            is_async=is_async
        )

        self.maxsize = maxsize

    def submit(self, func, *args, timeout: int = None, ttl: int = None, result_ttl: int = 500, retry: int = None):
        """
        提交一个函数及其参数到队列中执行。
    
        如果指定了timeout，則該函数必须在指定的时间内完成，否则会抛出异常。
        如果队列的大小超过了最大限制（maxsize），则会等待直到队列的大小减少。
    
        :param func: 要执行的函数。
        :param args: 函数的参数。
        :param timeout: 任务超时时间，超时后将会被标记为 failed 状态。
        :param ttl: 任务加入队列后，被取消之前的等待执行时间；超过该时间后任务会被取消执行。如果设置为 -1，任务将永远不会被取消，一直等待。
        :param result_ttl: 在 Redis 中存储的任务结果的过期时间。
        :param retry: 在 Redis 中存储的任务结果的过期时间。
        :return: 无。
        """
        while self.maxsize is not None and self._queue.count > self.maxsize:
            time.sleep(5)

        if retry is not None:
            retry = Retry(max=retry, interval=[10, 30, 60])

        return self._queue.enqueue(
            func,
            *args,
            timeout=timeout,
            ttl=ttl,
            result_ttl=result_ttl,
            retry=retry
        )

    def delete(self, delete_jobs: bool = True):
        """
        删除所有任务

        :param delete_jobs:
        :return:
        """
        self._queue.delete(delete_jobs=delete_jobs)

    def run_work(self):
        """
        以 work 模式启动

        :return:
        """
        w = Worker([self._queue, ], connection=self.redis)
        w.work()

    def workers(self):
        """
        返回当前所有 work 节点
        :return:
        """
        return Worker.all(queue=self._queue)

    @property
    def worker_count(self):
        """
        返回当前所有 work 节点数量
        :return:
        """
        return Worker.count(connection=self.redis, queue=self._queue)

    @property
    def jobs(self):
        """
        返回当前所有任务
        :return:
        """
        return self._queue.jobs

    @property
    def job_ids(self):
        """
        返回当前所有任务 id
        :return:
        """
        return self._queue.job_ids

    def __len__(self):
        return self._queue.count

    def __repr__(self):
        return f'<CrawLsy: {self._queue.name}>'

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.redis.close()
