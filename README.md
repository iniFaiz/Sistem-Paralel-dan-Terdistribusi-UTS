# Pub-Sub Log Aggregator

Tugas UTS Sistem Terdistribusi dan Parallel. Layanan agregator log yang menerima event dari publisher, memprosesnya melalui antrean internal, dan melakukan deduplikasi menggunakan SQLite.

## Fitur
- **Idempotent Consumer:** Memastikan event yang sama hanya diproses sekali.
- **Deduplication:** Menggunakan `(topic, event_id)` sebagai kunci unik.
- **Asynchronous Processing:** Menggunakan `asyncio.Queue` untuk memisahkan penerimaan dan pemrosesan.
- **Persistence:** Data tetap aman meskipun container di-restart menggunakan SQLite.
- **Dockerized:** Mudah dijalankan dengan Docker dan Docker Compose.

## Persyaratan
- Docker & Docker Compose
- Python 3.11+ (jika dijalankan lokal tanpa Docker)

## Cara Menjalankan

### Menggunakan Docker Compose (Nilai plus)
1. Build dan jalankan layanan:
   ```bash
   docker compose up --build
   ```
2. Aggregator akan tersedia di `http://localhost:8000`.
3. Publisher akan mulai mengirimkan event secara otomatis (termasuk simulasi duplikasi).

### Menjalankan Secara Lokal
1. Install dependensi:
   ```bash
   pip install -r requirements.txt
   ```
2. Jalankan Aggregator:
   ```bash
   python -m uvicorn src.main:app --reload
   ```
3. Jalankan Publisher (di terminal terpisah):
   ```bash
   python src/publisher.py
   ```

## API Endpoints
- `POST /publish`: Mengirim satu atau kumpulan event.
- `GET /events?topic=name`: Mengambil daftar event unik berdasarkan topik.
- `GET /stats`: Melihat statistik sistem (received, unique, duplicates, uptime).

## Menjalankan Unit Tests & Performance
```bash
# Unit tests
pytest

# Performance test (Pastikan aggregator sedang berjalan)
python tests/performance_test.py
```

## Struktur Proyek
- `src/`: Kode sumber aplikasi.
- `tests/`: Unit tests menggunakan pytest.
- `Dockerfile` & `docker-compose.yml`: Konfigurasi container.
- `report.md`: Jawaban teori dan penjelasan desain.
