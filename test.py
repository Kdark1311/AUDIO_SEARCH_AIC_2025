from elasticsearch import Elasticsearch

class ElasticsearchConnection:
    def __init__(self, host="https://localhost:9200", username="elastic", password="kiet13112007"):
        self.host = host
        self.username = username
        self.password = password
        self.es = None

    def connect(self):
        try:
            print(f"🔌 Đang kết nối tới {self.host}...")
            # Kết nối với Elasticsearch, bỏ qua SSL
            self.es = Elasticsearch(
                self.host,
                basic_auth=(self.username, self.password),
                verify_certs=False  # Bỏ qua chứng chỉ SSL
            )

            # Kiểm tra kết nối
            if self.es.ping():
                print("✅ Đã kết nối tới Elasticsearch!")
            else:
                print("❌ Không thể kết nối tới Elasticsearch!")

        except Exception as e:
            print(f"❌ Lỗi khi kết nối: {e}")

# ========================== USAGE ==========================
if __name__ == "__main__":
    # Khởi tạo đối tượng kiểm tra kết nối
    es_connection = ElasticsearchConnection()

    # Kiểm tra kết nối
    es_connection.connect()
