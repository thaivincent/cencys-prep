import threading
import random
import queue
import string
import time
from dataclasses import dataclass, replace
from typing import Callable, Any
import json
import sqlite3
from scanner_stream import MockScannerStream
from typing import Iterator
from serializer import ScanRecord, unpackMessage
import binascii
import gzip

SENTINEL = object()
MAX_RETRIES = 3

POISON_PILL_ERRORS = (binascii.Error,
                      gzip.BadGzipFile,
                      json.JSONDecodeError,
                      ValueError
)


@dataclass(frozen=True, slots=True)
class Job:
    id: str
    payload: bytes
    retry_count: int = 0
    on_success: Callable[[Any], None] | None = None
    on_failure: Callable[[Any], None] | None = None

@dataclass(frozen=True, slots=True)
class DBWriteJob:
    query: str
    params: tuple[Any, ...]
    callback: Callable[[Any], None] | None = None

@dataclass(frozen=True, slots=True)
class DLQJob:
    id:str
    payload: bytes
    error: str
    timestamp: float
    retry_count:int

def generateID(length = 5):
    return "".join(random.choices(string.digits,k=length))

def sendToDLQ(job:Job, err:str, dlq:queue.Queue) -> None:
    dlqjob = DLQJob(job.id, job.payload, err, time.time(), job.retry_count)
    dlq.put(dlqjob)

def expBackoff(job:Job) -> None:
    retry_time = 0.05 * (2 ** job.retry_count)
    time.sleep(retry_time)

def producer(stream: Iterator[bytes], q:queue.Queue) -> None:
    for packet in stream:
        task_id = generateID()
        f = lambda : safe_print("Success")
        f2 = lambda: safe_print("Failure")
        job = Job(task_id, payload=packet, on_success=f, on_failure=f2)
        q.put(job)

def consumer(job_queue:queue.Queue, db_queue: queue.Queue, dlq:queue.Queue) -> None:
    while True:
        task = job_queue.get()
        # using try..finally guarentees that the q.join() doesnt get stuck
        try:
            if task is SENTINEL:
                safe_print(f"Consumer Recieved SENTINEL, Shutting down worker") 
                break

            success = False
            current_task = task
            while current_task.retry_count <= MAX_RETRIES:
                try:
                    safe_print(f"Consumer processing job: {current_task.id}, retry_count: {current_task.retry_count}")
                    record = unpackMessage(current_task.payload)

                    #Create a dbjob and push to db_queue
                    query = """ INSERT INTO scans (id, version, payload, updated_at)
                                VALUES (?, ?, ?, ?)
                                ON CONFLICT(id) DO UPDATE SET
                                    version = excluded.version,
                                    payload = excluded.payload,
                                    updated_at = excluded.updated_at
                                WHERE excluded.version >= scans.version;
                            """
                    params = (record.id, record.version, json.dumps(record.to_dict()),record.updated_at)

                    writeJob = DBWriteJob(query=query, params=params, callback=current_task.on_success)
                    db_queue.put(writeJob)
                    time.sleep(random.uniform(.01,.05))
                    success = True
                    break
                except Exception as e:
                    if isinstance(e,POISON_PILL_ERRORS):
                        sendToDLQ(job=current_task, err=str(e), dlq=dlq)
                        safe_print(f"Sent Job {current_task.id}, to dlq, error: {str(e)}")
                        break

                    elif current_task.retry_count < MAX_RETRIES:
                        current_task = replace(current_task, retry_count = current_task.retry_count + 1)
                        safe_print(f"Retrying Job {current_task.id}, attempt: {current_task.retry_count}")
                        expBackoff(job=current_task)
                    else:
                        sendToDLQ(job=current_task, err=str(e), dlq=dlq)
                        safe_print(f"Sent Job {current_task.id}, to dlq, error: {str(e)}")
                        break
                
            if not success and current_task.on_failure != None:
                current_task.on_failure()
        finally:
            job_queue.task_done()

def writer(db_file:str, db_queue:queue.Queue) -> None:
    db = sqlite3.connect(db_file)
    cursor = db.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    with db:
        db.execute("""CREATE TABLE IF NOT EXISTS scans (
                        id TEXT PRIMARY KEY,
                        version INTEGER,
                        payload TEXT,
                        updated_at REAL
        );
        """)
    while True:
        write_job = db_queue.get()
        try:
            if write_job is SENTINEL:
                safe_print(f"Writer Recieved SENTINEL, Shutting down worker") 
                db.close()
                break
            with db:
                cursor.execute(write_job.query, write_job.params)
                time.sleep(random.uniform(.01,.05))
                if write_job.callback != None:
                    write_job.callback()
        except Exception as e:
            safe_print(f"[DB Error] {e}")
        finally:
            db_queue.task_done() 


print_lock = threading.Lock()

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def main():
    job_queue = queue.Queue(maxsize=10)
    dead_letter_queue = queue.Queue(maxsize=100)

    db_queue = queue.Queue(maxsize=100)
    db_file = "app.db"

    stream = MockScannerStream()
    producer_thread = threading.Thread(target=producer, args=(stream,job_queue),name="Producer")

    consumer_thread = threading.Thread(target=consumer, args=(job_queue,db_queue,dead_letter_queue,), name = "Consumer")
    writer_thread = threading.Thread(target=writer, args=(db_file,db_queue,), name = "DB Writer")
    
    consumer_thread.start()
    writer_thread.start()

    producer_thread.start()
    producer_thread.join()

    job_queue.put(SENTINEL)
    job_queue.join()
    consumer_thread.join()

    db_queue.put(SENTINEL)
    db_queue.join()
    writer_thread.join()

    safe_print(f"\n--- DLQ Audit Summary (Total: {dead_letter_queue.qsize()}) ---")
    while not dead_letter_queue.empty():
        dead_item = dead_letter_queue.get()
        safe_print(f"  [DLQ Item] ID: {dead_item.id} | Retries: {dead_item.retry_count} | Error: {dead_item.error}")

if __name__ == "__main__":
    main()    


