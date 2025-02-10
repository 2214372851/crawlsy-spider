import datetime
import logging
import inspect
import pickle
import time
from typing import Self, Optional

from crawlsy_spider.core import Status
import uuid
from concurrent.futures import ProcessPoolExecutor

import redis

from crawlsy_spider.utils import as_string, import_attribute

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CrawLsy:
    def __init__(self,
                 name: str,
                 host: str,
                 db: int,
                 password: Optional[str],
                 port: int = 6379,
                 prefetch_count: int = 1,
                 concurrency: int = 1):
        self._name = name
        self._prefetch_count = prefetch_count
        self._concurrency = concurrency
        self._pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
        )
        self._conn = redis.Redis(connection_pool=self._pool)
        self._queue_name = f"crawlery:queue:{self._name}"
        self._work_running = True
        self._work_stat = None
        self._run_work_count = 0
        self._success_work_count = 0
        self._failed_work_count = 0

    def put(self, func, *args, **kwargs) -> str:
        job_id = str(uuid.uuid4())
        if inspect.isfunction(func) or inspect.isbuiltin(func):
            func_name = '{0}.{1}'.format(func.__module__, func.__qualname__)
        elif isinstance(func, str):
            func_name = func
        else:
            raise TypeError('Expected a callable or a string, but got: {0}'.format(func))
        args_map = {
            "func": func_name,
            "args": pickle.dumps(args),
            "kwargs": pickle.dumps(kwargs),
            "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "started_at": '',
            "ended_at": '',
            "status": Status.PENDING.value,
        }
        self._conn.hset(
            name=f"crawlery:job:{job_id}",
            mapping=args_map
        )
        self._conn.rpush(self._queue_name, job_id)
        return job_id

    def get(self, executor):
        try:
            job_ids = self._conn.lpop(self._queue_name, count=self._prefetch_count)
            if not job_ids:
                time.sleep(5)
                logger.info("No job in queue")
                return
            works = {}
            for job_id in job_ids:
                job_id = as_string(job_id)
                self._conn.hset(
                    f"crawlery:job:{job_id}",
                    mapping={
                        "status": Status.RUNNING.value,
                        "started_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                )
                job_mapping: dict[bytes, bytes] = self._conn.hgetall(f"crawlery:job:{job_id}")
                job_maps = {}
                for k, v in job_mapping.items():
                    key = as_string(k)
                    job_maps[key] = pickle.loads(v) if key in {'args', 'kwargs'} else as_string(v)
                future = executor.submit(import_attribute(job_maps['func']), *job_maps['args'],
                                         **job_maps['kwargs'])
                works[job_id] = future
                logger.info(f"Job {job_id} started")
                self._run_work_count += 1
            for job_id, future in works.items():
                job_id = as_string(job_id)
                try:
                    result = future.result()
                    self._conn.hset(
                        f"crawlery:job:{job_id}",
                        mapping={
                            "status": Status.SUCCEED.value,
                            "result": str(pickle.dumps(result)),
                            "ended_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                    )
                    logger.info(f"Job {job_id} succeed")
                    self._success_work_count += 1
                except Exception as e:
                    self._conn.hset(
                        f"crawlery:job:{job_id}",
                        mapping={
                            "status": Status.FAILED.value,
                            "error": str(pickle.dumps(e)),
                            "ended_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                    )
                    logger.error(f"Job {job_id} failed: {e}", exc_info=True)
                    self._failed_work_count += 1
        except KeyboardInterrupt:
            self._work_running = False
            executor.shutdown(wait=True)
            logger.info('Shutdown...')
        except Exception as e:
            logger.error(e, exc_info=True)

    def status(self, job_id: str) -> str:
        return as_string(self._conn.hget(f"crawlery:job:{job_id}", "status"))

    def close(self):
        self._conn.close()
        self._pool.close()

    def work(self):
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('<%(asctime)s> (%(name)s) [%(levelname)s] - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        with ProcessPoolExecutor(max_workers=self._concurrency) as executor:
            while self._work_running:
                self.get(executor)

    def __len__(self) -> int:
        return self._conn.llen(self._queue_name)

    def is_failed(self, job_id: str) -> bool:
        return as_string(self.status(job_id)) == Status.FAILED.value

    def is_succeed(self, job_id: str) -> bool:
        return as_string(self.status(job_id)) == Status.SUCCEED.value

    def is_pending(self, job_id: str) -> bool:
        return as_string(self.status(job_id)) == Status.PENDING.value

    def is_running(self, job_id: str) -> bool:
        return as_string(self.status(job_id)) == Status.RUNNING.value

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()