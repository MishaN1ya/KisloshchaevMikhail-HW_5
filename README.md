## Часть 1 — Hadoop + Spark: обработка JSON и запись в S3

### Шаг 1 — Создание кластера Yandex Data Processing
Развёрнут кластер Hadoop + Spark через сервис Yandex Data Processing.  
Компоненты: HDFS, YARN, SPARK, ZEPPELIN.

![Кластер Data Processing]()

---

### Шаг 2 — Создание JSON файла и загрузка в HDFS
На мастер-хосте кластера создан файл `data.json` с тестовыми данными (10 записей).  
Файл загружен в HDFS по пути `/data/data.json`.

```bash
sudo -u hdfs hdfs dfs -mkdir -p /data
sudo -u hdfs hdfs dfs -chmod 777 /data
hdfs dfs -put ~/data.json /data/data.json
```

![Загрузка в HDFS]()

---

### Шаг 3 — Чтение JSON в Zeppelin через PySpark
В Apache Zeppelin создан ноутбук `json_to_s3`.  
Данные прочитаны из HDFS с помощью Spark.

```python
%spark.pyspark
df = spark.read.json("hdfs:///data/data.json")
df.printSchema()
df.show()
```

![Чтение данных в Zeppelin]()

---

### Шаг 4 — Запись результата в S3
Данные записаны в бакет Object Storage в формате JSON.

```python
%spark.pyspark
df.write.mode("overwrite").json("s3a://dataproc-bucket/output/data/")
print("Записано в S3!")
```

![Запись в S3]()

---

### Шаг 5 — Проверка данных в S3
Данные прочитаны обратно из S3 для проверки корректности записи.

```python
%spark.pyspark
df_check = spark.read.json("s3a://dataproc-bucket/output/data/")
df_check.show()
```

![Проверка данных из S3]()

---

## Часть 2 — NoSQL: Kafka → StoreDoc через Data Transfer

### Шаг 1 — Создание кластера Kafka
Создан кластер Managed Service for Apache Kafka.  
Топик: `sensors`, пользователь: `mkf-user`.

![Кластер Kafka]()

---

### Шаг 2 — Создание кластера StoreDoc
Создан кластер Yandex StoreDoc (MongoDB).  
База данных: `db1`, пользователь: `mmg-user`.

![Кластер StoreDoc]()

---

### Шаг 3 — Создание эндпоинтов Data Transfer
Создано два эндпоинта:
- **Источник:** Apache Kafka, топик `sensors`, схема данных JSON
- **Приёмник:** Yandex StoreDoc, база `db1`

![Эндпоинты]()

---

### Шаг 4 — Создание и активация трансфера
Создан трансфер типа **Репликация**.  
Трансфер активирован и перешёл в статус **Реплицируется**.

![Трансфер активен]()

---

### Шаг 5 — Отправка тестовых данных в Kafka
С виртуальной машины отправлены тестовые данные в топик `sensors` с помощью утилиты `kcat`.

```bash
jq -rc . sample.json | kcat -P \
   -b :9091 \
   -t sensors \
   -k key \
   -X security.protocol=SASL_SSL \
   -X sasl.mechanisms=SCRAM-SHA-512 \
   -X sasl.username="mkf-user" \
   -X sasl.password="" \
   -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt -Z
```

![Отправка данных]()

---

### Шаг 6 — Проверка данных в StoreDoc
Данные успешно доставлены в коллекцию `sensors` базы `db1` кластера StoreDoc.

```javascript
db.sensors.find()
```

![Данные в StoreDoc]()