import asyncio
import random
import csv
from datetime import datetime
from pathlib import Path
import aiofiles
import json


# -----------------------------
# Konfiguráció
# -----------------------------
EVENT_BATCH_SIZE = 50      # Hány eseményt tartunk memóriában, mielőtt CSV-be írunk
SNAPSHOT_EVERY = 200       # Hány esemény után készítünk snapshot-ot. Ha vége az eseménynek, akkor is (lásd: def stop())
ASYNC_WRITE_DELAY = 0.01   # Ha nagyon gyorsan jönnek az események, kis késleltetés a batch írás előtt

# ==========================================================
# Pipeline Aggregator | Aggregator Snapshot Json IO
# ==========================================================
"""
    Singleton: egyszer van csak egy példány.

        Feladata:

        Összegyűjteni a delta értékeket a counterektől.

        Ha az összeg eléri a threshold-ot (10_000), létrehoz egy “completed batch”-et, a maradékot megtartja.

        Snapshot-ot tud készíteni és vissza tudja állítani a korábbi állapotot (save_snapshot() / load_snapshot()).
"""

class PipelineAggregator:
    _instance = None

    def __new__(cls, threshold=10_000, snapshot_file="aggregator_snapshot.json"):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init(threshold, snapshot_file)
        return cls._instance

    def _init(self, threshold, snapshot_file):
        self.threshold = threshold
        self.current_total = 0
        self.completed_batches = []
        self.lock = asyncio.Lock()
        self.snapshot_file = Path(snapshot_file)

    async def add(self, value: int):
        async with self.lock:
            self.current_total += value

            while self.current_total >= self.threshold:
                self.completed_batches.append(self.threshold)
                self.current_total -= self.threshold

    # -------------------------
    # Snapshot mentés
    # -------------------------
    async def save_snapshot(self):
        async with self.lock:
            tmp = self.snapshot_file.with_suffix(".tmp")
            data = {
                "current_total": self.current_total,
                "completed_batches": self.completed_batches,
                "timestamp": datetime.now().isoformat()
            }
            async with aiofiles.open(tmp, "w") as f:
                await f.write(json.dumps(data))
            tmp.replace(self.snapshot_file)

    # -------------------------
    # Snapshot visszaállítás
    # -------------------------
    async def load_snapshot(self):
        if self.snapshot_file.exists():
            async with aiofiles.open(self.snapshot_file, "r") as f:
                content = await f.read()
                data = json.loads(content)
                async with self.lock:
                    self.current_total = data.get("current_total", 0)
                    self.completed_batches = data.get("completed_batches", [])


# ==========================================================
# Async Counter Worker (ultra gyors, async batch IO)
# ==========================================================
class AsyncCounterWorker:
    _instances = {}

    def __new__(cls, name, *args, **kwargs):
        if name not in cls._instances:
            cls._instances[name] = super().__new__(cls)
            cls._instances[name]._initialized = False
        return cls._instances[name]

    def __init__(self, name, num_workers=2):
        if getattr(self, "_initialized", False):
            return

        self.name = name
        self.event_file = Path(f"{name}_events.csv")
        self.snapshot_file = Path(f"{name}_snapshot.csv")

        self._queue = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._value = 0
        self._event_id = 0
        self._event_buffer = []
        self._num_workers = num_workers
        self._workers = []
        self._flush_task = None

        self._init_files()
        self._initialized = True

    # -------------------------
    # Fájl inicializálás
    # -------------------------
    def _init_files(self):
        if not self.event_file.exists():
            with open(self.event_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["id", "timestamp", "delta"])
        if not self.snapshot_file.exists():
            with open(self.snapshot_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "value", "last_event_id"])
                writer.writerow([datetime.now().isoformat(), 0, 0])

    # -------------------------
    # Async setup (recovery + workers)
    # -------------------------
    async def setup(self):
        await self._recover()
        self._workers = [asyncio.create_task(self._run()) for _ in range(self._num_workers)]

    # -------------------------
    # Recovery
    # -------------------------
    """
        Counter értékét visszaállítja az előző futásból.
        Betölti a legfrissebb snapshotot → gyorsan visszaáll az utolsó ismert állapotra
        Minden esemény megvan immutable logban → ellenőrizhető a teljes log történet
    """
    # -------------------------
    async def _recover(self):
        # snapshot
        if self.snapshot_file.exists():
            async with aiofiles.open(self.snapshot_file, "r") as f:
                lines = await f.read()
                rows = list(csv.reader(lines.splitlines()))[1:]
                if rows:
                    last = rows[-1]
                    self._value = int(last[1])
                    self._event_id = int(last[2])

        # event replay
        if self.event_file.exists():
            async with aiofiles.open(self.event_file, "r") as f:
                lines = await f.read()
                rows = list(csv.reader(lines.splitlines()))[1:]
                for row in rows:
                    eid, _, delta = int(row[0]), row[1], int(row[2])
                    if eid > self._event_id:
                        self._value += delta
                        self._event_id = eid

    # -------------------------
    # Worker (queue feldolgozás)
    # -------------------------
    async def _run(self):
        while True:
            delta = await self._queue.get()
            if delta is None:
                self._queue.task_done()
                # flush minden maradék
                await self._flush_events()
                return

            async with self._lock:
                self._value += delta
                self._event_id += 1
                self._event_buffer.append((self._event_id, datetime.now().isoformat(), delta))

                # Snapshot
                if self._event_id % SNAPSHOT_EVERY == 0:
                    await self._write_snapshot()

            # Async flush, ha elérjük a batch méretet
            if len(self._event_buffer) >= EVENT_BATCH_SIZE:
                if not self._flush_task or self._flush_task.done():
                    self._flush_task = asyncio.create_task(self._flush_events())

            self._queue.task_done()

    # -------------------------
    # Publikus API
    # -------------------------
    async def add_amount(self, amount: int):
        await self._queue.put(amount)

    async def subtract_amount(self, amount: int):
        await self._queue.put(-amount)

    async def get_value_now(self):
        async with self._lock:
            return self._value

    async def stop(self):
        for _ in self._workers:
            await self._queue.put(None)
        await self._queue.join()
        await asyncio.gather(*self._workers)
        # flush minden maradék
        if self._flush_task and not self._flush_task.done():
            await self._flush_task
        await self._write_snapshot()

    # -------------------------
    # Async fájl IO
    # -------------------------
    async def _flush_events(self):
        if not self._event_buffer:
            return
        buffer_copy = self._event_buffer[:]
        self._event_buffer.clear()

        async with aiofiles.open(self.event_file, "a") as f:
            for row in buffer_copy:
                await f.write(f"{row[0]},{row[1]},{row[2]}\n")

    async def _write_snapshot(self):
        tmp = self.snapshot_file.with_suffix(".tmp")
        async with aiofiles.open(tmp, "w") as f:
            await f.write("timestamp,value,last_event_id\n")
            await f.write(f"{datetime.now().isoformat()},{self._value},{self._event_id}\n")
        tmp.replace(self.snapshot_file)

# ==========================================================
# Producer
# ==========================================================
async def producer(name: str, counter: AsyncCounterWorker, min_sleep=0.01, max_sleep=0.05):
    for _ in range(500):  # sok esemény
        await asyncio.sleep(random.uniform(min_sleep, max_sleep))  # egyes producer gyorsabb, mások lassabbak.

        if random.choice([True, False]):  # ADD
            amount = random.randint(10_000, 100_000)
            await counter.add_amount(amount)
            #print(f"{name} -> {counter.name} ADD {amount}")
        else:  # SUB
            amount = random.randint(1_000, 10_000)
            await counter.subtract_amount(amount)
            #print(f"{name} -> {counter.name} SUB -{amount}")


# ==========================================================
# Monitor
# ==========================================================
async def monitor(counter: AsyncCounterWorker):
    while True:
        await asyncio.sleep(0.1)
        print(f"[{counter.name}] Élő érték: {await counter.get_value_now()}")

# ==========================================================
# Pipeline Consumer
# ==========================================================
async def pipeline_consumer(name: str, queue: asyncio.Queue, aggregator: PipelineAggregator):
    while True:
        value = await queue.get()
        print(f"[{name}] Queue-ból kinyert érték:", value)
        await aggregator.add(value)
        queue.task_done()

# ==========================================================
# Pipeline Feeder
# ==========================================================
async def pipeline_feeder(counter, queue, interval=0.1):
    last = 0
    while True:
        await asyncio.sleep(interval)
        current = await counter.get_value_now()
        delta = current - last
        if delta != 0:
            await queue.put(delta)
            last = current

# ==========================================================
# Batch consumer
# ==========================================================
async def batch_consumer(queue: asyncio.Queue):
    while True:
        macro_batch = await queue.get()
        print(f"MACRO BATCH TARTALOM: {macro_batch}", flush=True)
        queue.task_done()


# ==========================================================
# Main
# ==========================================================

async def main_continuous():
    # -----------------------------
    # Counter-ek setup
    # -----------------------------
    counters = {n: AsyncCounterWorker(n) for n in ["A", "B", "C"]}
    await asyncio.gather(*(c.setup() for c in counters.values()))

    """
    AsyncCounterWorker példány készül (A, B, C).

        setup() visszaállítja az előző futásból a snapshotból és az event logból:

        Betölti a legfrissebb snapshotot (_value és _event_id).

        Végigolvassa a CSV-t (events.csv) és “visszajátssza” az eseményeket, hogy a counter pontosan ott folytassa, 
        ahol korábban abbahagyta.

        Minden counterhez létrejönnek a worker taskok, amik figyelik a queue-t
    """


    # -----------------------------
    # Producer konfiguráció
    # -----------------------------
    producer_configs = [
        ("A", 3, 0.005, 0.02),
        ("B", 2, 0.01, 0.05),
        ("C", 2, 0.01, 0.05),
    ]

    """
        Több producer task fut párhuzamosan, mindegyik véletlenszerűen ADD vagy SUB értékeket generál-
    """
    producers = [
        asyncio.create_task(
            producer(f"P{i}_{name}", counters[name], min_sleep, max_sleep)
        )
        for name, count, min_sleep, max_sleep in producer_configs
        for i in range(count)
    ]

    monitors = [asyncio.create_task(monitor(c)) for c in counters.values()]

    # -----------------------------
    # Pipeline és aggregator
    # -----------------------------
    pipelines = {n: asyncio.Queue() for n in counters}
    aggregator = PipelineAggregator(threshold=10_000)

    # Macro batch-ek 100 batchenként kerülnek ki a batch_queue-ba. (PipelineAggregator)
    batch_queue = asyncio.Queue()

    await aggregator.load_snapshot()

    consumers = [
        asyncio.create_task(pipeline_consumer(name, q, aggregator))
        for name, q in pipelines.items()
    ]

    # -----------------------------
    # Pipeline feeder taskok
    # -----------------------------
    feeders = [
        asyncio.create_task(pipeline_feeder(counters[n], pipelines[n]))
        for n in counters
    ]

    # -----------------------------
    # AGGREGATOR WATCHER
    # -----------------------------
    async def aggregator_watcher():
        last_sent = 0
        while True:
            await asyncio.sleep(0.2)
            async with aggregator.lock:
                total = len(aggregator.completed_batches)
                while total - last_sent >= 100:
                    macro = aggregator.completed_batches[last_sent:last_sent + 100]
                    await batch_queue.put(macro)
                    last_sent += 100

    """
        Figyeli, hogy 100 batch összegyűlt-e, és ha igen, beteszi a batch_queue-ba macro batch-ként.
    """
    watcher_task = asyncio.create_task(aggregator_watcher())

    # -----------------------------
    # Batch consumer
    # -----------------------------
    batch_task = asyncio.create_task(batch_consumer(batch_queue))

    # -----------------------------
    # Snapshot task
    # -----------------------------
    async def periodic_snapshot(aggregator, interval=5.0):
        while True:
            await asyncio.sleep(interval)
            await aggregator.save_snapshot()
            print(f"[{datetime.now().isoformat()}] Aggregator snapshot mentve.")

    snapshot_task = asyncio.create_task(periodic_snapshot(aggregator))

    try:
        print("Folyamatos futás... Ctrl+C a leállításhoz")
        while True:
            await asyncio.sleep(1)

    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nLeállítás folyamatban…")

        for p in producers:
            p.cancel()
        await asyncio.gather(*producers, return_exceptions=True)

        for c in counters.values():
            await c.stop()

        print("Pipeline kifuttatása…")

        for q in pipelines.values():
            await q.join()

        await aggregator.save_snapshot()    

        for m in monitors:
            m.cancel()

        await asyncio.gather(*monitors, return_exceptions=True)

        await aggregator.save_snapshot()
        snapshot_task.cancel()

        print("KÉSZ BATCH-EK:", len(aggregator.completed_batches))
        print("AKTUÁLIS MARADÉK:", aggregator.current_total)


        """
            Counter-ek: minden delta-t hozzáadnak a saját _value-hoz, immutable logban tárolják.

            Pipeline feeder: kiszámolja a delta-t az utolsó snapshothoz képest, és átadja az aggregator-nak.

            Aggregator: összegzi a delta-kat, minden threshold-nál készít batch-et. A maradékot megtartja.

            Watcher: 100 batchenként létrehoz macro batch-t.
        
        """

if __name__ == "__main__":
    try:
        asyncio.run(main_continuous()) # Létrehoz egy async event loop-ot
    except KeyboardInterrupt:
        print("\nProgram megszakítva Ctrl+C-vel.")
