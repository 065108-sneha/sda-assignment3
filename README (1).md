# Real-Time Retail Transaction Analytics

## Streaming Data Analytics — Assignment 3

A Kafka-to-MySQL-to-Grafana analytics pipeline for real-time retail transaction monitoring in the E-Commerce & Retail domain.

**Submitted by:** Sneha Umar Vaishya  
**Roll No.:** 065108  
**Institution:** Fore School of Management  
**Course:** Streaming Data Analytics  
**Assignment:** 3

---

## 1. Project Overview

This project extends the Kafka-based retail transaction streaming pipeline developed in Assignments 1 and 2 into an analytical dashboard solution.

The final workflow is:

```text
POS / Retail Transaction Data
          ↓
transactions.csv
          ↓
Python Kafka Producer
          ↓
Apache Kafka
          ↓
retail-transactions
          ↓
Python Kafka Consumer
          ↓
MySQL
          ↓
Grafana
          ↓
Real-Time Retail Transaction Analytics Dashboard
```

The project demonstrates how retail transaction events can be streamed through Kafka, stored in a structured MySQL database, and transformed into management-oriented visual insights using Grafana.

---

## 2. Business Context

Retail businesses continuously generate transaction data containing:

- Transaction details
- Store information
- Product information
- Product category
- Quantity
- Unit price
- Transaction value
- Payment method
- Timestamp

The objective is to convert these transaction events into a dashboard that can support quick monitoring of revenue, transaction volume, store performance, category performance and payment behavior.

---

## 3. Project Objectives

- Consume retail transaction events from Apache Kafka.
- Transfer Kafka events into MySQL.
- Verify successful storage of all transaction records.
- Connect Grafana to the MySQL database.
- Build a management-oriented retail analytics dashboard.
- Calculate key retail KPIs.
- Analyze revenue by category.
- Analyze transactions by store.
- Analyze revenue by payment method.
- Examine the relationship between quantity and unit price.
- Compare store revenue composition across categories.

---

## 4. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Kafka consumer and data transfer |
| Apache Kafka | Real-time event streaming |
| Docker / Docker Compose | Local infrastructure |
| MySQL | Analytical data storage |
| Grafana | Dashboard and visualization |
| CSV | Transaction input dataset |
| JSON | Kafka message serialization |
| VS Code | Development environment |

---

## 5. Dataset

The project uses **100 simulated POS transaction records**.

### Transaction Schema

| Field | Description |
|---|---|
| `transaction_id` | Unique transaction identifier |
| `timestamp` | Transaction event timestamp |
| `store_id` | Store identifier |
| `product_id` | Product identifier |
| `category` | Product category |
| `quantity` | Number of units purchased |
| `unit_price` | Price per unit |
| `total_amount` | Transaction revenue |
| `payment_method` | Payment method |

---

## 6. Kafka Configuration

**Kafka Topic:**

```text
retail-transactions
```

**Kafka Server:**

```text
localhost:9094
```

The Kafka stream was established in Assignment 2 and reused as the input stream for the Assignment 3 downstream analytics layer.

---

## 7. MySQL Database

Database:

```text
retail_streaming
```

Table:

```text
transactions
```

The final MySQL table contains all nine transaction fields.

### Verification

The MySQL database was independently queried and confirmed:

```text
Total transactions = 100
```

---

## 8. Grafana Dashboard

Grafana was connected to MySQL using the `Assignment3-MySQL` data source.

The dashboard contains multiple visualization types rather than relying only on bar charts.

### KPI Panels

- Total Revenue
- Total Transactions
- Average Transaction Value
- Total Units Sold

### Analytical Panels

- Revenue by Category
- Transactions by Store
- Revenue by Payment Method
- Quantity vs Unit Price
- Store Revenue Mix by Category

---

## 9. Key Results

| KPI | Result |
|---|---:|
| Total Revenue | ₹1,973,714 |
| Total Transactions | 100 |
| Average Transaction Value | ₹19,737.14 |
| Total Units Sold | 194 |

### Category Revenue

| Category | Revenue |
|---|---:|
| Electronics | ₹1,654,959 |
| Home | ₹151,953 |
| Fashion | ₹99,351 |
| Beauty | ₹67,451 |

### Payment Method Revenue

| Payment Method | Revenue | Share |
|---|---:|---:|
| Credit Card | ₹695,753 | 35% |
| UPI | ₹544,053 | 28% |
| Card | ₹395,855 | 20% |
| Debit Card | ₹338,053 | 17% |

---

## 10. Business Insights

### Electronics Dominates Revenue

Electronics generated the largest category revenue at approximately ₹1.65 million, making it the dominant revenue contributor in the project dataset.

### S002 Shows the Highest Store Revenue

The store-category analysis shows S002 as the largest revenue-generating store in the sample, with electronics contributing ₹892,977.

### Credit Card Has the Largest Payment Revenue Share

Credit Card contributes approximately 35% of total payment-method revenue, followed by UPI at 28%.

### Quantity and Unit Price

The Quantity vs Unit Price visualization shows that observed quantities are concentrated between 1 and 3 units across a wide range of product prices. The dataset does not show a simple linear relationship between price and quantity.

---

## 11. Validation

The implementation was validated at multiple stages:

```text
Kafka events consumed:       100 / 100
MySQL records stored:       100
Grafana connection:         Database Connection OK
Dashboard transactions:     100
Dashboard revenue:          ₹1,973,714
Dashboard units sold:       194
```

---

## 12. Execution Flow

### Step 1 — Start Infrastructure

Start the Kafka and MySQL Docker services.

### Step 2 — Verify Kafka

Confirm that the `retail-transactions` topic is available.

### Step 3 — Run Consumer

From the Assignment 3 project directory:

```bash
python consumer.py
```

The consumer reads Kafka events and transfers them into MySQL.

### Step 4 — Verify MySQL

Check the transaction count:

```sql
USE retail_streaming;

SELECT COUNT(*) AS total_transactions
FROM transactions;
```

Expected result:

```text
100
```

### Step 5 — Connect Grafana

Configure the MySQL data source in Grafana.

Data source name:

```text
Assignment3-MySQL
```

Verify that Grafana displays:

```text
Database Connection OK
```

### Step 6 — Build Dashboard

Create Grafana panels using SQL queries against:

```text
retail_streaming.transactions
```

---

## 13. Project Structure

```text
SDA_Assignment_3/
│
├── consumer.py
├── requirements.txt
│
└── README.md
```

The upstream Assignment 2 project contains the transaction dataset and Kafka producer.

---

## 14. Business Value

The architecture demonstrates how a retail organization can move from raw transaction events to an analytical management view.

The same architecture can be extended to:

- Continuous POS transactions
- Store-level monitoring
- Product-level performance
- Category demand monitoring
- Revenue alerts
- Inventory integration
- Customer clickstream integration
- Real-time anomaly detection

---

## 15. Limitations

- The project uses a simulated dataset of 100 records.
- Kafka is implemented in a local academic environment.
- The dashboard focuses on POS transaction data.
- Clickstream and inventory streams from the broader Assignment 1 architecture are not implemented in this Assignment 3 dashboard.
- Predictive forecasting and automated anomaly detection are outside the current scope.

---

## 16. Future Scope

- Apache Spark Structured Streaming
- Window-based real-time aggregations
- Real-time revenue alerts
- Low-stock alerts
- Clickstream integration
- Inventory integration
- Product-level drill-down
- Demand forecasting
- Anomaly detection
- Larger production-like Kafka workloads
- Additional Kafka partitions and replication

---

## 17. Conclusion

This project completes the downstream analytics stage of the retail streaming pipeline developed across Assignments 1–3.

The final architecture successfully demonstrates:

```text
Kafka → Python Consumer → MySQL → Grafana
```

The system successfully consumed 100 Kafka transaction events, stored 100 records in MySQL, connected Grafana to the database, and presented the resulting data through a multi-visual retail analytics dashboard.

---

## Author

**Sneha Umar Vaishya**  
PGDM — Business Data Analytics  
Fore School of Management  
Roll No. 065108
