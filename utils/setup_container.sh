#!/bin/bash

echo "=================================="
echo "🚀 ELASTICSEARCH SETUP SCRIPT"
echo "=================================="

# 1. Kiểm tra Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker chưa được cài đặt!"
    echo "Cài Docker: https://docs.docker.com/get-docker/"
    exit 1
fi

# 2. Xóa container cũ (nếu có)
echo ""
echo "🗑️  Xóa container cũ (nếu có)..."
docker stop elasticsearch 2>/dev/null
docker rm elasticsearch 2>/dev/null

# 3. Tạo container mới với volume
echo ""
echo "📦 Tạo Elasticsearch container với volume..."
docker run -d \
  --name elasticsearch \
  -p 9200:9200 \
  -p 9300:9300 \
  -v elasticsearch-data:/usr/share/elasticsearch/data \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  -e "xpack.security.http.ssl.enabled=false" \
  docker.elastic.co/elasticsearch/elasticsearch:8.7.0

# 4. Đợi Elasticsearch khởi động
echo ""
echo "⏳ Đang đợi Elasticsearch khởi động (30s)..."
sleep 30

# 5. Kiểm tra kết nối
echo ""
echo "🔍 Kiểm tra kết nối..."
if curl -s http://localhost:9200 > /dev/null; then
    echo "✅ Elasticsearch đã sẵn sàng!"
else
    echo "❌ Không thể kết nối tới Elasticsearch"
    echo "Kiểm tra logs: docker logs elasticsearch"
    exit 1
fi

# 6. Cài Vietnamese Plugin
echo ""
echo "📥 Cài Vietnamese Plugin..."
docker exec elasticsearch bin/elasticsearch-plugin install -b \
  https://github.com/duydo/elasticsearch-analysis-vietnamese/releases/download/v8.7.0/elasticsearch-analysis-vietnamese-8.7.0.zip

# 7. Restart để plugin có hiệu lực
echo ""
echo "🔄 Restart Elasticsearch..."
docker restart elasticsearch

echo ""
echo "⏳ Đang đợi restart (30s)..."
sleep 30

# 8. Kiểm tra plugin
echo ""
echo "✔️  Kiểm tra plugin đã cài..."
docker exec elasticsearch bin/elasticsearch-plugin list

# 9. Hoàn thành
echo ""
echo "=================================="
echo "✅ SETUP HOÀN TẤT!"
echo "=================================="
echo ""
echo "📋 Thông tin:"
echo "   - Elasticsearch URL: http://localhost:9200"
echo "   - Volume: elasticsearch-data"
echo "   - Plugin: elasticsearch-analysis-vietnamese"
echo ""
echo "🚀 Bước tiếp theo:"
echo "   1. Cài Python dependencies: pip install -r requirements.txt"
echo "   2. Import data: python import_elasticsearch.py"
echo "   3. Search: python speech_retrieval_interactive.py"
echo ""
echo "💡 Tips:"
echo "   - Xem logs: docker logs elasticsearch"
echo "   - Stop: docker stop elasticsearch"
echo "   - Start: docker start elasticsearch"
echo "   - Xóa tất cả: docker rm -f elasticsearch && docker volume rm elasticsearch-data"
echo ""