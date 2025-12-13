# 🌊 StreamFlow - Distributed Notification Engine

StreamFlow is a fault-tolerant, asynchronous notification system built to handle high-throughput event streaming. It utilizes **Apache Kafka** for decoupled communication, **MongoDB** for persistence, and implements a **Dead Letter Queue (DLQ)** pattern to ensure zero data loss during processing failures.

## 🏗️ System Architecture

```mermaid
graph LR
    User[User / API Client] -->|POST /notify| API[Spring Boot Producer]
    API -->|Async Event| Kafka{Kafka Topic: user-notifications}
    
    subgraph "Microservices Cluster"
        Kafka -->|Consume| Consumer[Notification Service]
        Consumer -->|Success| DB[(MongoDB)]
        Consumer --x|Failure| DLQ{Dead Letter Queue}
    end
    
    DLQ -.->|Manual Retry / Alert| Admin[DevOps / Admin]
    
    style DLQ fill:#f9f,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5
    style DB fill:#bbf,stroke:#333,stroke-width:2px


🚀 Key Features
Event-Driven Architecture: Decoupled Producer and Consumer services using Kafka.

Fault Tolerance: Automatic error detection moving failed messages to a Dead Letter Queue (DLQ).

Scalability: Dockerized infrastructure managing Zookeeper, Kafka, and MongoDB containers.

Persistence: Asynchronous logging of all successful notifications to NoSQL storage.

🛠️ Tech Stack
Core: Java 21, Spring Boot 3.4

Messaging: Apache Kafka, Zookeeper

Database: MongoDB

Infrastructure: Docker Compose



⚡ How to Run
Start Infrastructure

Bash

docker-compose up -d
Run the Application

Bash

./mvnw spring-boot:run
Test the API (Happy Path)

Bash

curl -X POST "http://localhost:9090/api/notify?message=Hello_World"
Result: Message saved to MongoDB.

Test Fault Tolerance (DLQ)

Bash

curl -X POST "http://localhost:9090/api/notify?message=Testing_error"
Result: Consumer crash simulated -> Message moved to notifications-dlq