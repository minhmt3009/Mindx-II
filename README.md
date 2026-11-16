# Bank Marketing Data Pipeline with Airflow

Dự án ETL pipeline sử dụng Apache Airflow để xử lý dữ liệu Bank Marketing từ CSV vào Data Warehouse.

## 📊 Dữ liệu

File `data/Bank.csv` chứa dữ liệu về chiến dịch marketing ngân hàng với 17 cột:
- **age**: Tuổi khách hàng
- **job**: Nghề nghiệp
- **marital**: Tình trạng hôn nhân (married/single/divorced)
- **education**: Trình độ học vấn
- **default**: Có nợ xấu không (yes/no)
- **balance**: Số dư tài khoản
- **housing**: Có vay mua nhà không (yes/no)
- **loan**: Có vay tiêu dùng không (yes/no)
- **contact**: Phương thức liên hệ
- **day**: Ngày liên hệ trong tháng
- **month**: Tháng liên hệ (jan-dec)
- **duration**: Thời lượng cuộc gọi (giây)
- **campaign**: Số lần liên hệ trong chiến dịch này
- **pdays**: Số ngày từ lần liên hệ trước (-1 = chưa từng liên hệ)
- **previous**: Số lần liên hệ trước chiến dịch này
- **poutcome**: Kết quả chiến dịch trước
- **deposit**: Khách hàng có đăng ký không (yes/no)

## 🏗️ Kiến trúc Pipeline

```
CSV File (Bank.csv)
    ↓
[RAW Layer] raw.bank_raw (landing zone - all text)
    ↓
[ETL Layer] etl.bank_staging_clean (cleaned & validated)
    ↓
[DW Layer] dw.bank_fact (final data warehouse)
```

## 🚀 Cách chạy

### Bước 1: Khởi động Docker

```bash
# Dọn dẹp (nếu chạy lại từ đầu)
docker-compose down -v

# Khởi động services
docker-compose up -d
```

Docker sẽ tự động:
1. Tạo database `bank_db`
2. Tạo schemas: `raw`, `etl`, `dw`
3. Tạo các bảng cần thiết
4. Khởi động Airflow

### Bước 2: Kiểm tra logs

```bash
# Xem logs khởi tạo Airflow
docker-compose logs -f airflow-init

# Xem logs webserver
docker-compose logs -f airflow-webserver
```

### Bước 3: Truy cập Airflow UI

- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

### Bước 4: Chạy DAG

1. Tìm DAG có tên: `bank_e2e_csv_to_dw`
2. Bật DAG (toggle switch)
3. Click nút "Trigger DAG" để chạy

## 📋 Các Task trong DAG

1. **start**: Task khởi đầu
2. **ingest_csv_to_raw**: Load CSV vào bảng `raw.bank_raw`
3. **transform_load_dw**: 
   - Parse và validate dữ liệu
   - Tạo staging table `etl.bank_staging_clean`
   - Upsert vào `dw.bank_fact`
4. **post_run_check**: Kiểm tra kết quả
5. **end**: Task kết thúc

## 🔍 Validation Rules

Pipeline áp dụng các rules sau khi transform:

- **age**: 18-100 tuổi
- **balance**: Phải là số nguyên
- **marital**: Chỉ chấp nhận married/single/divorced
- **default, housing, loan, deposit**: Chỉ chấp nhận yes/no
- **day**: 1-31
- **month**: jan-dec
- **duration**: ≥ 0
- **campaign**: ≥ 1
- **pdays**: ≥ -1
- **previous**: ≥ 0

## 📊 Kiểm tra kết quả

### Kết nối Postgres

```bash
docker exec -it <postgres_container_name> psql -U airflow -d bank_db
```

### Truy vấn mẫu

```sql
-- Xem số lượng records
SELECT 
    (SELECT COUNT(*) FROM raw.bank_raw) as raw_count,
    (SELECT COUNT(*) FROM dw.bank_fact) as dw_count;

-- Xem phân bố deposit
SELECT deposit, COUNT(*) as count
FROM dw.bank_fact
GROUP BY deposit;

-- Xem top 5 nghề nghiệp
SELECT job, COUNT(*) as count
FROM dw.bank_fact
GROUP BY job
ORDER BY count DESC
LIMIT 5;

-- Xem phân bố theo tình trạng hôn nhân
SELECT marital, deposit, COUNT(*) as count
FROM dw.bank_fact
GROUP BY marital, deposit
ORDER BY marital, deposit;
```

## 📁 Cấu trúc thư mục

```
.
├── dags/
│   ├── bank_e2e_csv_to_dw_dag.py      # DAG definition
│   ├── data/
│   │   └── Bank.csv                    # Dữ liệu nguồn
│   └── scripts/
│       ├── load_csv_to_postgres_raw.py # Ingest script
│       ├── etl_bank_transform_load.py  # ETL script
│       └── post_run_check.py           # Validation script
├── sql/
│   ├── 01_create_database.sql          # Tạo database
│   └── 02_init_schema.sql              # Tạo schemas & tables
├── logs/                                # Airflow logs
├── plugins/                             # Airflow plugins
├── docker-compose.yaml                  # Docker config
├── .env                                 # Environment variables
└── README.md                            # File này
```

## 🔧 Troubleshooting

### Lỗi kết nối database
```bash
# Kiểm tra Postgres đang chạy
docker ps | grep postgres

# Xem logs Postgres
docker-compose logs postgres
```

### Lỗi import CSV
- Kiểm tra file `data/Bank.csv` có tồn tại
- Kiểm tra encoding của file (phải là UTF-8)
- Kiểm tra số cột trong CSV (phải có 17 cột)

### Xem chi tiết lỗi
- Vào Airflow UI → Click vào DAG run → Xem logs từng task
- Logs cũng được lưu trong folder `logs/`

## 📝 Environment Variables

File `.env` và `docker-compose.yaml` đã cấu hình:
- `DB_HOST=postgres`
- `DB_PORT=5432`
- `DB_NAME=bank_db`
- `DB_USER=airflow`
- `DB_PASS=airflow`
- `CSV_PATH=/opt/airflow/dags/data/Bank.csv`

## 🛑 Dừng Pipeline

```bash
# Dừng tất cả services
docker-compose down

# Dừng và xóa volumes (xóa data)
docker-compose down -v
```
