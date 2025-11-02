"""
ELASTICSEARCH EXPORT SCRIPT - FIXED VERSION
Export toàn bộ index thành JSON files để đẩy lên GitHub
"""

import os
import json
from elasticsearch import Elasticsearch
from tqdm import tqdm


def export_index_to_files(
    host="http://localhost:9200",
    index_name="speech_index",
    output_dir="elasticsearch_data",
    batch_size=1000
):
    """
    Export Elasticsearch index thành các file JSON nhỏ
    
    Args:
        host: Elasticsearch URL
        index_name: Tên index cần export
        output_dir: Thư mục lưu files (có thể đẩy lên GitHub)
        batch_size: Số documents mỗi file
    """
    
    print("="*80)
    print("📦 ELASTICSEARCH INDEX EXPORT")
    print("="*80)
    
    # Kết nối ES
    print(f"\n🔌 Đang kết nối tới {host}...")
    es = Elasticsearch(hosts=[host], verify_certs=False, ssl_show_warn=False)
    
    if not es.ping():
        print("❌ Không thể kết nối Elasticsearch!")
        return
    
    print("✅ Đã kết nối")
    
    # Kiểm tra index
    if not es.indices.exists(index=index_name):
        print(f"❌ Index '{index_name}' không tồn tại!")
        return
    
    # Lấy mapping
    print(f"\n📋 Đang lấy mapping của index '{index_name}'...")
    mapping_response = es.indices.get_mapping(index=index_name)
    
    # ✅ FIX: Convert ObjectApiResponse to dict
    mapping = dict(mapping_response)
    
    # Tạo thư mục output
    os.makedirs(output_dir, exist_ok=True)
    
    # Lưu mapping
    mapping_file = os.path.join(output_dir, "mapping.json")
    with open(mapping_file, 'w', encoding='utf-8') as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)
    print(f"✅ Đã lưu mapping: {mapping_file}")
    
    # Đếm documents
    count_result = es.count(index=index_name)
    total_docs = count_result['count']
    print(f"\n📊 Tổng số documents: {total_docs:,}")
    
    # Export documents theo batch
    print(f"\n📤 Đang export documents (batch size: {batch_size})...")
    
    batch_num = 0
    exported = 0
    
    # Scroll API để lấy tất cả documents
    query = {"query": {"match_all": {}}}
    
    # Initial search
    response = es.search(
        index=index_name,
        body=query,
        scroll='2m',
        size=batch_size
    )
    
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']
    
    # Progress bar
    pbar = tqdm(total=total_docs, desc="Exporting", unit="docs")
    
    while hits:
        # Lưu batch hiện tại
        batch_file = os.path.join(output_dir, f"batch_{batch_num:04d}.json")
        batch_data = []
        
        for hit in hits:
            # Chỉ lấy _source (bỏ _id, _index, _score)
            doc = hit['_source']
            
            # ⚠️ BỎ EMBEDDING để giảm kích thước file (sẽ tạo lại khi import)
            if 'embedding' in doc:
                doc['embedding'] = None  # Đánh dấu để tạo lại sau
            
            batch_data.append(doc)
        
        # Ghi file
        with open(batch_file, 'w', encoding='utf-8') as f:
            json.dump(batch_data, f, ensure_ascii=False, indent=2)
        
        exported += len(hits)
        pbar.update(len(hits))
        batch_num += 1
        
        # Lấy batch tiếp theo
        response = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
    
    pbar.close()
    
    # Clear scroll
    es.clear_scroll(scroll_id=scroll_id)
    
    # Lưu metadata
    metadata = {
        "index_name": index_name,
        "total_documents": total_docs,
        "total_batches": batch_num,
        "batch_size": batch_size,
        "note": "embedding=None will be regenerated on import"
    }
    
    metadata_file = os.path.join(output_dir, "metadata.json")
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    print("\n" + "="*80)
    print("✅ EXPORT HOÀN THÀNH!")
    print("="*80)
    print(f"📁 Thư mục output: {output_dir}")
    print(f"📝 Số files: {batch_num + 2} (mapping + metadata + {batch_num} batches)")
    print(f"📊 Tổng documents: {exported:,}")
    print("\n💡 Bước tiếp theo:")
    print(f"   1. Kiểm tra thư mục: {output_dir}/")
    print(f"   2. Đẩy lên GitHub: git add {output_dir} && git commit && git push")
    print(f"   3. Trên máy khác: git clone và chạy import_elasticsearch.py")
    print("="*80)


if __name__ == "__main__":
    # Cấu hình
    HOST = "http://localhost:9200"
    INDEX_NAME = "speech_index"
    OUTPUT_DIR = "elasticsearch_data"  # Thư mục này sẽ đẩy lên GitHub
    
    # Chạy export
    export_index_to_files(
        host=HOST,
        index_name=INDEX_NAME,
        output_dir=OUTPUT_DIR,
        batch_size=1000  # Mỗi file 1000 documents
    )