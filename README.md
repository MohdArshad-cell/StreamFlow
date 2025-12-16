````markdown
# 🌊 StreamFlow - Distributed Notification Engine

StreamFlow is a fault-tolerant, asynchronous notification system designed for high throughput and reliability. It utilizes **Apache Kafka** for decoupled event streaming, **MongoDB** for persistent logging, and implements a robust **Resilience Strategy** using Exponential Backoff and Dead Letter Queues (DLQ) to ensure zero data loss.

## 🚀 Tech Stack
- **Core:** Java 17, Spring Boot 3.4
- **Messaging:** Apache Kafka (with Zookeeper)
- **Database:** MongoDB (NoSQL Persistence)
- **Resilience:** Spring Retry (Exponential Backoff policies)
- **Containerization:** Docker & Docker Compose
- **Documentation:** OpenAPI (Swagger UI)

## 🏗️ System Architecture

The system decouples producers and consumers to handle massive traffic spikes gracefully without crashing downstream services.

```mermaid
graph LR
    User[User / API Client] -->|POST /notify| API[Producer Service]
    API -->|Async Event| Kafka{Kafka Topic: user-notifications}
    
    subgraph "Microservices Cluster"
        Kafka -->|Consume| Consumer[Worker Service]
        Consumer -->|Success| DB[(MongoDB)]
        Consumer --x|Transient Fail| Retry[Exponential Backoff]
        Retry -->|Max Retries Exceeded| DLQ{Dead Letter Queue}
    end
    
    DLQ -.->|Manual Inspection| Admin[DevOps Dashboard]
````

### Key Features

1.  **Event-Driven:** Fully asynchronous communication prevents blocking the main API thread.
2.  **Resilience (Circuit Breaker Logic):**
      * **Transient Failures:** If the consumer encounters a network blip, it retries **3 times** with **Exponential Backoff** (1s wait, then 2s wait).
      * **Permanent Failures:** If all retries fail, the message is **not lost**. It is automatically moved to a **Dead Letter Queue (DLQ)** (`notifications-dlq`) for later inspection.
3.  **Zero Data Loss:** Critical notifications are never dropped, ensuring 100% reliability for business-critical alerts.

## 🛠️ How to Run

### 1\. Start Infrastructure

Make sure Docker Desktop is running, then spin up Kafka, Zookeeper, and MongoDB:

```bash
docker-compose up -d
```

### 2\. Run the Application

Navigate to the project folder and start the Spring Boot app:

```bash
cd core/core
./mvnw.cmd spring-boot:run
```

### 3\. Explore the API

Once the server is running, access the interactive API documentation:
👉 **[http://localhost:9090/swagger-ui.html](https://www.google.com/search?q=http://localhost:9090/swagger-ui.html)**

-----

## 🧪 Fault Tolerance Proof (The "Crash Test")

We simulated a system failure by sending a "poison pill" message (`error`) to the system. The logs demonstrate the **Exponential Backoff** and **DLQ Fallback** mechanisms kicking in to protect data integrity.

### Scenario:

1.  **User** sends a notification with the message text `"error"`.
2.  **Consumer** detects the error and throws an exception.
3.  **Spring Retry** attempts to process it 3 times with increasing delays.
4.  **Recovery:** After the 3rd failure, the system captures the event and routes it to the `notifications-dlq` topic.

### Console Output Evidence:

```text
🔄 Processing Attempt: error
... (1 second wait) ...
🔄 Processing Attempt: error
... (2 second wait) ...
🔄 Processing Attempt: error
❌ All Retries Failed. Moving to DLQ...
⚠️ DLQ Received Bad Message: FAILED: error
```

## 📝 API Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/notify?message={text}` | Triggers an asynchronous notification event. |

## 👨‍💻 Author

**Mohd Arshad**
***

### ⚡ Critical Final Steps
1.  **Paste this** into your `FlashTix-Backend` README file.
2.  **Fix the Typo:** Rename that `AI-Powered-Carrer-Catalyst` repository to `AI-Powered-**Career**-Catalyst`.
3.  **Rename StreamFlow:** Rename `stream-flow-` to `stream-flow`.

Once you do this, your GitHub is **interview-ready**. Do you want me to write the **Cold DM** to send to recruiters now?
