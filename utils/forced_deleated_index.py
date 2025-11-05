"""
Script XÓA NHANH - Không cần xác nhận (Cẩn thận!)
"""

from elasticsearch import Elasticsearch


def force_delete_index():
    """Xóa index ngay lập tức - KHÔNG hỏi xác nhận"""
    
    HOST = "http://localhost:9200"
    INDEX_NAME = "ocr_index"
    
    print("🗑️  FORCE DELETE - Đang xóa index...")
    
    try:
        # Kết nối
        es = Elasticsearch(
            hosts=[HOST],
            verify_certs=False,
            ssl_show_warn=False,
            request_timeout=30
        )
        
        if not es.ping():
            print("❌ Không thể kết nối Elasticsearch")
            return False
        
        # Kiểm tra index
        if not es.indices.exists(index=INDEX_NAME):
            print(f"ℹ️  Index '{INDEX_NAME}' không tồn tại")
            return False
        
        # Lấy thông tin
        count = es.count(index=INDEX_NAME)
        doc_count = count['count']
        
        # XÓA NGAY
        es.indices.delete(index=INDEX_NAME)
        
        print(f"✅ Đã xóa {doc_count:,} documents")
        return True
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return False


if __name__ == "__main__":
    print("="*80)
    print("⚠️  FORCE DELETE MODE - Xóa không hỏi!")
    print("="*80)
    
    if force_delete_index():
        print("\n✅ Hoàn thành!")
    else:
        print("\n❌ Thất bại!")