### 🧠 Architecture Overview

#### 🧵 **Thread 1 – Streamlit (Main UI Thread)**

* Streamlit runs your `main()` function top-to-bottom.
* It continuously re-renders the dashboard every few seconds (`while True: ... time.sleep(2)` loop).
* It **reads** the shared data (`records`, `answer_ids`) to show updates.
* This thread handles **UI rendering only**, not Kafka I/O.

#### ⚙️ **Thread 2 – Kafka Listener (Background Thread)**

* Created via:

  ```python
  thread = threading.Thread(target=kafka_listener, daemon=True)
  thread.start()
  ```
* This runs **independently** of Streamlit’s UI thread.
* It continuously consumes new messages from Kafka.
* For each message, it:

  * Deserializes JSON.
  * Checks for duplicate `answer_id`.
  * Adds the new record to the shared list `records`.

---

### 🔒 **Thread Safety**

We wrap access to shared data with a **threading.Lock**, e.g.:

```python
with lock:
    records.append(row)
```

and in Streamlit:

```python
with lock:
    df = pd.DataFrame(records)
```

This ensures that the Streamlit thread doesn’t try to read `records` while Kafka is writing to it — preventing race conditions.

---

### 🧩 Summary Diagram

```
 ┌──────────────────────┐       ┌────────────────────────┐
 │ Streamlit UI Thread  │       │ Kafka Listener Thread  │
 │                      │       │                        │
 │ while True:          │       │ for msg in consumer:   │
 │   lock → read data   │◄──────┼── lock → write data    │
 │   render dashboard   │       │                        │
 │   sleep(2s)          │       │                        │
 └──────────────────────┘       └────────────────────────┘
```