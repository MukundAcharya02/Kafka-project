# Kafka-Project 

[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x%2B-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.x%2B-red.svg)](https://kafka.apache.org/)
[![Java](https://img.shields.io/badge/Java-21-blue.svg)](https://www.oracle.com/java/technologies/)
[![Maven](https://img.shields.io/badge/Maven-3.x-orange.svg)](https://maven.apache.org/)

An event-driven microservices architecture built with **Spring Boot**, **Apache Kafka**, and **Java 21**. This project leverages a multi-module Maven structure to share domain-level events across microservices cleanly and efficiently.

---

##  Project Architecture & Flow

This project implements an **Event-Driven Architecture (EDA)** where microservices communicate asynchronously through Apache Kafka. 

### Current System Flow
1. **Core Library (`core`)**: Contains the shared Event DTO models (`ProductEventCreate`) which act as a strict schema agreement between microservices.
2. **Kafka Brokers**: Mediate communication asynchronously. The consumer listens to the `product-created-events-topic`.
3. **Email Notification Microservice (`EmailNotificationMicroservice`)**:
   - Configured with custom Java-based Kafka consumer configurations utilizing Spring Kafka.
   - Subscribes to the `product-created-events-topic` as part of the `product-created-events` consumer group.
   - Deserializes incoming JSON payloads into the shared `ProductEventCreate` DTO using `JacksonJsonDeserializer`.
   - Processes the event inside `ProductCreatedEventHandler` and triggers business logic (simulating email dispatch / logging).

### System Topology
The following Mermaid diagram shows the current architecture as well as the **planned future integration** of the Producer microservice:

```mermaid
graph TD
    %% Define Styles
    classDef core fill:#e1f5fe,stroke:#039be5,stroke-width:2px;
    classDef consumer fill:#e8f5e9,stroke:#43a047,stroke-width:2px;
    classDef producer fill:#fff3e0,stroke:#fb8c00,stroke-width:2px,stroke-dasharray: 5 5;
    classDef kafka fill:#ffebee,stroke:#e53935,stroke-width:2px;

    %% Nodes
    subgraph Core Shared Module
        DTO[ProductEventCreate DTO]
    end
    
    subgraph Future State
        PROD[Product/Order Microservice <br><b>(Future Producer)</b>]
    end

    subgraph Messaging Layer
        KB[Kafka Broker <br><i>product-created-events-topic</i>]
    end

    subgraph Consumer Microservice
        CONS[Email Notification Microservice <br><b>(Consumer)</b>]
        HAND[ProductCreatedEventHandler]
    end

    %% Relations
    PROD -.->|1. Serializes & Publishes event| KB
    KB --->|2. Delivers event stream| CONS
    CONS --->|3. Dispatches payload| HAND

    %% Dependency Links
    PROD -.->|Imports| DTO
    CONS --->|Imports| DTO

    %% Apply Styles
    class DTO core;
    class CONS,HAND consumer;
    class PROD producer;
    class KB kafka;
```

---

##  Future Producer Integration

> [!NOTE]  
> Currently, the system functions as a high-performance **Consumer** waiting for inbound events. 

In the next phase of development, a **dedicated Producer Microservice** (e.g., a *Product Service* or *Order Service*) will be added to the cluster. 

* **The Flow:** When a new product is created in the future service, it will import the `core` library, instantiate the `ProductEventCreate` DTO, and publish the event payload to the `product-created-events-topic` on the Kafka broker.
* **Seamless Integration:** Because the `EmailNotificationMicroservice` is already subscribed and configured to trust payloads originating from `com.mukund.core`, the notification consumer will automatically start receiving, logging, and processing real-time events as soon as the producer goes live!

---

##  Repository Structure

```directory
Kafka-Project/
├── core/                               # Core shared module (JAR packaging)
│   ├── src/main/java/com/mukund/core/
│   │   └── ProductEventCreate.java     # Shared event DTO model
│   └── pom.xml                         # Core Maven configuration
│
├── EmailNotificationMicroservice/       # Notification Consumer microservice
│   ├── src/main/java/com/mukund/EmailNotificationMicroservice/
│   │   ├── handler/
│   │   │   └── ProductCreatedEventHandler.java  # Kafka handler for processing events
│   │   ├── KafkaConsumerConfig.java             # Spring Kafka Consumer configuration
│   │   └── EmailNotificationMicroserviceApplication.java
│   ├── src/main/resources/
│   │   └── application.properties       # Consumer property definitions
│   └── pom.xml                         # Microservice Maven configuration
└── README.md
```

---

##  Configuration Properties

The **Email Notification Microservice** configuration is managed via `application.properties`:

| Property | Value | Description |
| :--- | :--- | :--- |
| `spring.application.name` | `EmailNotificationMicroservice` | Name of the microservice app. |
| `spring.port` | `0` | Port `0` instructs Spring to bind to a random free port (prevents port conflicts). |
| `spring.kafka.consumer.bootstrap-servers` | `localhost:9092,localhost:9094` | The addresses of the running Apache Kafka brokers. |
| `spring.kafka.consumer.group-id` | `product-created-events` | The unique identifier for the Kafka consumer group. |
| `spring.kafka.consumer.properties.spring.json.trusted.packages` | `com.mukund.core` | Instructs the deserializer to trust and instantiate types from the core library. |

---

##  Getting Started

### Prerequisites
* **Java Development Kit (JDK) 21** or higher.
* **Apache Kafka** running locally or in a Docker container (listening on ports `9092` and `9094`).
* **Maven 3.8+** installed.

### 1. Build and Install the Core Library
Since `EmailNotificationMicroservice` depends on the `core` module, you must first compile and install `core` to your local Maven repository (`.m2`):
```bash
# Navigate to the core directory
cd core

# Build and install to local Maven repository
mvn clean install
```

### 2. Build the Consumer Microservice
Once the core library is installed locally, build the Email Notification Microservice:
```bash
# Navigate to the microservice directory
cd ../EmailNotificationMicroservice

# Package the application
mvn clean package
```

### 3. Running the Service
Ensure your local Apache Kafka cluster is running on `localhost:9092,localhost:9094` and start the Spring Boot application:
```bash
mvn spring-boot:run
```

---

##  Sample Output
When a `ProductEventCreate` event is published to `product-created-events-topic`, you will see output in the microservice logs matching the following format:
```text
********* Sample Product Name
[INFO] Received a new event: Sample Product Name prod-1234-xyz 99.99
```
