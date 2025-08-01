# Customer Microservice – 10 GB Dataset Edition

A lightweight Flask microservice that exposes CRUD, analytics, and stress-testing endpoints backed by a **10 GB MongoDB “customers” collection**.  
Designed to be launched independently on **three VMs** behind a load-balancer and hammered by JMeter connection-storm test plans.

---

## 1 . Features

* Singleton `MongoClient` with connection-pool tuning (no per-request close bug 🎉)
* Complete customer CRUD + batch operations
* Rich analytics (status, country, purchase stats)
* Built-in stress endpoints
  * `/stress/large-payload` – generate/process payloads up to 1 MB
  * `/stress/connection-storm` – run mixed operations to churn connections
  * `/stress/heavy-computation` – CPU & aggregation torture
* Health & metrics endpoints (`/health`, `/metrics`)
* Configuration via **`config.properties`** or environment variables
* Production-ready logging
* Ready-made **JMeter 5.5** test plan and runner (`customer_storm_test.jmx`, `run_test.sh`)

---

## 2 . Repository Layout

```
customer-microservice/
├── customer_api.py              # Flask application
├── config.properties            # Service configuration (per-VM)
├── customer_storm_test.jmx      # JMeter test plan
├── jmeter_test.properties       # JMeter variables
├── run_test.sh                  # Test runner (local & distributed)
└── README.md                    # ← you are here
```

---

## 3 . Prerequisites

| Component | Version | Notes |
|-----------|---------|-------|
| Python    | 3.9+    | Install via system package or `pyenv` |
| MongoDB   | 4.4+    | Atlas or self-hosted; contains the 10 GB dataset (`customers_db.customers`) |
| JMeter    | 5.5     | Required for provided test plan |
| Git       | any     | To clone onto each VM |

Optional: `Docker` & `docker-compose` for containerisation.

---

## 4 . Quick-Start (Single Host)

```bash
# 1. Clone & cd
git clone https://your.git.repo/customer-microservice.git
cd customer-microservice

# 2. Create venv & install deps
python -m venv venv
source venv/bin/activate
pip install -r <(python - <<EOF
print('\n'.join([
    "flask",
    "flask-cors",
    "pymongo>=4.5",
    "psutil",
    "bson"
]))
EOF
)

# 3. Configure
cp config.properties config.local.properties
$EDITOR config.local.properties     # set connection_uri, instance_id, etc.

# 4. Launch
python customer_api.py
```

Visit `http://localhost:5000/health` to confirm status.

---

## 5 . Configuration Guide

The service loads `config.properties` at startup; every key can be overridden by an env var of the same name (upper-case).

Key highlights:

| Key | Description | Default |
|-----|-------------|---------|
| connection_uri | MongoDB URI (`mongodb+srv://...`) | `mongodb://localhost:27017` |
| database_name  | DB containing the 10 GB data     | `customers_db` |
| collection_name| Target collection                | `customers` |
| write_concern  | MongoDB write concern            | `majority` |
| max_pool_size  | Connection pool upper bound      | `100` |
| api_port       | Flask listening port             | `5000` |
| instance_id    | Friendly name per VM             | auto-generated |

**Large payload limit:** `max_payload_size_kb` (default `1024`).

---

## 6 . API Reference (abridged)

### 6.1 CRUD

| Method | Path | Notes |
|--------|------|-------|
| GET    | `/customers` | query: `limit`, `skip`, `status`, `country`, `search` |
| GET    | `/customers/{customer_id}` | fetch one |
| POST   | `/customers` | create |
| PUT    | `/customers/{customer_id}` | update |
| DELETE | `/customers/{customer_id}` | delete |
| POST   | `/customers/batch` | create many (≤1000) |

### 6.2 Analytics

* `GET /customers/analytics/summary`
* `GET /customers/analytics/purchases`

### 6.3 Stress & Diagnostics

| Path | Purpose |
|------|---------|
| `/stress/large-payload?size_kb=512` | generate payload |
| `POST /stress/large-payload` | echo + metadata |
| `/stress/batch-customers?count=10&size_kb=50` | insert random docs |
| `/stress/heavy-computation?depth=3&complexity=medium` | nested aggs |
| `/stress/connection-storm?operations=20&concurrency=5` | mixed ops |
| `/health` & `/metrics` | liveness / performance |

---

## 7 . Deploying on 3 VMs

### 7.1 Steps per VM

```bash
# 1. Clone repo
git clone https://your.git.repo/customer-microservice.git
cd customer-microservice

# 2. Adjust config
cp config.properties config.properties
sed -i "s/instance_id=.*/instance_id=customer-vm1/" config.properties   # vm2/vm3 accordingly
sed -i "s/api_port=.*/api_port=5000/" config.properties                 # change if port clash

# 3. Create Python venv & install deps (as Quick-Start)

# 4. Create systemd service (optional)
sudo tee /etc/systemd/system/customer.service >/dev/null <<EOF
[Unit]
Description=Customer API
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/customer-microservice
ExecStart=/home/ubuntu/customer-microservice/venv/bin/python customer_api.py
Environment=PYTHONUNBUFFERED=1
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable --now customer
```

Open port 5000 (or your custom) in the VM firewall / security group.

### 7.2 Load-Balancer

Point your LB (NGINX, HAProxy, cloud LB) to the three VM IPs on port 5000. Health endpoint: `/health`.

---

## 8 . JMeter Testing

### 8.1 Local single-machine

```bash
./run_test.sh --scenario storm --threads 20 --duration 120
```

HTML report auto-generated in `test-results/*/report/index.html`.

### 8.2 Distributed (2 JMeter client VMs)

1. Install JMeter 5.5 on each client (`/opt/apache-jmeter`).
2. On **server VMs** run JMeter in server mode:

```bash
jmeter-server -Dserver.rmi.localport=7000
```

3. From controller node:

```bash
./run_test.sh \
  --mode distributed \
  --remote 10.0.0.11,10.0.0.12 \
  --scenario all \
  --threads 50 --duration 300
```

### 8.3 Customising

* Edit `jmeter_test.properties` – host/port, payload size, storm intensity.
* Pass overrides: `-Jpayload_size=1024`.

---

## 9 . Monitoring & Logs

* **Service logs**: stdout/systemd → `journalctl -u customer.service`.
* **Connection stats**: `GET /metrics` (`connection_stats` section).
* **JMeter logs**: `test-results/logs/*.log`.
* **HTML reports**: `test-results/<scenario>/report`.

---

## 10 . Troubleshooting

| Symptom | Possible Cause | Resolution |
|---------|----------------|------------|
| `Cannot use MongoClient after close` | Using old service version with teardown bug | Ensure you are running **customer_api.py** from this project |
| `ECONNREFUSED` from JMeter | Service not listening / port blocked | Verify `systemctl status customer`, check firewall |
| High 5xx under load | Mongo pool exhausted | Increase `max_pool_size`, ensure VM has adequate CPU |
| Payload size errors | `413 Payload Too Large` | Raise `max_payload_size_kb` in config |
| JMeter “Unrecognized SSL message” | Testing HTTPS without TLS offloading | Set `protocol=https` and configure cert/truststore |

---

## 11 . Appendix

### 11.1 Loading the 10 GB Dataset

If not already present, run the provided data loader (published separately) pointing at `customers_db.customers`. All microservice analytics assume that schema.

### 11.2 Security Tips

* Set strong MongoDB user/password; store via env vars.
* When using Atlas/SSL: set `tls_enabled=true` and `tls_ca_file_path=/path/ca.pem`.
* Run behind reverse proxy (NGINX) for rate-limiting & TLS termination.

---

Happy testing! Open issues & pull requests welcome.  
— *Customer Microservice Team*
