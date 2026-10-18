import threading
import random
import queue
import string
import time
from dataclasses import dataclass, replace
from typing import Callable, Any
import json
import sqlite3

SENTINEL = object()
MAX_RETRIES = 3


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
    params: tuple[str]
    callback: Callable[[Any], None] | None = None

def generateID(length = 5):
    return "".join(random.choices(string.digits,k=length))

def generateData(is_corrupt = False):
    if is_corrupt:
        return b"{malformed json}"
    payload = {
        "ip": f"192.168.1.{random.randint(1, 254)}",
        "port": random.choice([80, 443, 22, 8080]),
        "status": "OPEN",
        "timestamp": time.time(),
    }
    return json.dumps(payload).encode("utf-8")

def producer(producer_id : int, count: int, q:queue.Queue) -> None:
    for _ in range(count):
        is_corrupt = (random.random() < 0.01)
        task_id = generateID()
        payload = generateData(is_corrupt)
        f = lambda : safe_print("Successful write to DB")
        f2 = lambda: safe_print("Failure")

        job = Job(task_id, payload=payload, on_success=f, on_failure=f2)
        q.put(job)

def consumer(job_queue:queue.Queue, db_queue: queue.Queue) -> None:
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
                    j = json.loads(task.payload.decode("utf-8"))

                    #Create a dbjob and push to db_queue
                    query = """INSERT INTO scans (id, ip, port, status, timestamp)
                               VALUES (?, ?, ?, ?, ?)
                            """
                    params = (current_task.id, j['ip'], j['port'],j['status'],j['timestamp'])
                    callback = current_task.on_success

                    writeJob = DBWriteJob(query=query, params=params, callback=callback)
                    db_queue.put(writeJob)
                    time.sleep(random.uniform(.01,.05))
                    success = True
                    break
                except Exception as e:
                    if current_task.retry_count < MAX_RETRIES:
                        current_task = replace(current_task, retry_count = current_task.retry_count + 1)
                        safe_print(f"Retrying Job {current_task.id}, attempt: {current_task.retry_count}")
                        time.sleep(0.02)
                    else:
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
                        ip TEXT,
                        port INTEGER,
                        status TEXT,
                        timestamp REAL
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
    num_producers = 2
    tasks_per_producer = 5

    db_queue = queue.Queue(maxsize=100)
    db_file = "app.db"

    producers = [threading.Thread(target=producer, args=(i,tasks_per_producer,job_queue),name=f"Producer-{i}") for i in range(num_producers)]

    consumer_thread = threading.Thread(target=consumer, args=(job_queue,db_queue,), name = "Consumer")
    writer_thread = threading.Thread(target=writer, args=(db_file,db_queue,), name = "DB Writer")
    
    consumer_thread.start()
    writer_thread.start()
    for p in producers:
        p.start()

    for p in producers:
        p.join()

    job_queue.put(SENTINEL)
    job_queue.join()
    consumer_thread.join()

    db_queue.put(SENTINEL)
    db_queue.join()
    writer_thread.join()

if __name__ == "__main__":
    main()    


