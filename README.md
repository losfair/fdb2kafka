# fdb2kafka

Ship consistent logs from FoundationDB to Kafka.

## Usage

Example Kubernetes configuration:

```yaml
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  namespace: default
  name: fdb2kafka
spec:
  selector:
    matchLabels:
      app: fdb2kafka
  replicas: 1
  serviceName: fdb2kafka
  template:
    metadata:
      labels:
        app: fdb2kafka
    spec:
      containers:
      - name: app
        image: "ghcr.io/losfair/fdb2kafka:v0.1.1"
        command: ["/fdb2kafka", "run"]
        env:
        - name: SERVICE_KAFKA_SERVER
          value: redpanda.core.svc.cluster.local:9092
        - name: SERVICE_KAFKA_TOPIC
          value: com.example.yourapp.eventlog
        - name: SERVICE_CURSOR_PATH
          value: '["yourapp", "logcursor", "fdb2kafka"]' # FoundationDB tuple segments
        - name: SERVICE_LOG_PATH
          value: '["yourapp", "event"]' # FoundationDB tuple segments
        - name: SERVICE_KEY_QUERY
          value: data.user # gabs (https://github.com/Jeffail/gabs) style json path selector
        volumeMounts:
        - name: fdbconfig
          mountPath: /etc/foundationdb
          readOnly: true
      volumes:
      - name: fdbconfig
        hostPath:
          path: /etc/foundationdb
          type: Directory
```
