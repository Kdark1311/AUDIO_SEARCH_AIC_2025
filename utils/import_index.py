"""
ELASTICSEARCH IMPORT SCRIPT - IMPROVED VERSION
Import data từ exported files (clone từ GitHub)
Compatible with Elasticsearch 8.11.0
"""

import os
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from sentence_transformers import SentenceTransformer
from tqdm import tqdm


def import_index_from_files(
    host="http://localhost:9200",
    input_dir="elasticsearch_data",
    regenerate_embeddings=True,
    semantic_model="sentence-transformers/stsb-xlm-r-multilingual"
):
    """
    Import Elasticsearch index từ exported files
    
    Args:
        host: Elasticsearch URL
        input_dir: Thư mục chứa exported files (clone từ GitHub)
        regenerate_embeddings: Tạo lại embeddings (khuyên dùng=True)
        semantic_model: Model để tạo embeddings
    """
    
    print("="*80)
    print("📦 ELASTICSEARCH INDEX IMPORT")
    print("="*80)
    
    # Kiểm tra thư mục input
    if not os.path.exists(input_dir):
        print(f"❌ Thư mục '{input_dir}' không tồn tại!")
        print("   Hãy đảm bảo đã clone repo từ GitHub")
        return
    
    # Kết nối ES
    print(f"\n🔌 Đang kết nối tới {host}...")
    es = Elasticsearch(
        hosts=[host], 
        verify_certs=False, 
        ssl_show_warn=False,
        request_timeout=60,
        max_retries=3,
        retry_on_timeout=True
    )
    
    if not es.ping():
        print("❌ Không thể kết nối Elasticsearch!")
        print("   Hãy chạy: ./setup_elasticsearch.sh")
        return
    
    print("✅ Đã kết nối")
    
    # Load metadata
    metadata_file = os.path.join(input_dir, "metadata.json")
    if not os.path.exists(metadata_file):
        print(f"❌ Không tìm thấy metadata.json trong {input_dir}")
        return
    
    with open(metadata_file, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    
    index_name = metadata['index_name']
    total_docs = metadata['total_documents']
    total_batches = metadata['total_batches']
    
    print(f"\n📋 Metadata:")
    print(f"   - Index name: {index_name}")
    print(f"   - Total documents: {total_docs:,}")
    print(f"   - Total batches: {total_batches}")
    
    # Load mapping
    mapping_file = os.path.join(input_dir, "mapping.json")
    with open(mapping_file, 'r', encoding='utf-8') as f:
        mapping_data = json.load(f)
    
    # Lấy mapping của index (bỏ qua index name cũ)
    original_mapping = list(mapping_data.values())[0]['mappings']
    
    # Xóa index cũ nếu tồn tại
    if es.indices.exists(index=index_name):
        print(f"\n⚠️  Index '{index_name}' đã tồn tại")
        response = input("Xóa và tạo lại? (yes/no): ")
        if response.lower() != 'yes':
            print("❌ Đã hủy")
            return
        es.indices.delete(index=index_name)
        print(f"🗑️  Đã xóa index cũ")
    
    # Tạo index mới với mapping (ES 8.x style)
    print(f"\n🔨 Đang tạo index '{index_name}'...")
    es.indices.create(index=index_name, mappings=original_mapping)  # ✅ mappings= not body=
    print("✅ Đã tạo index")
    
    # Load model nếu cần regenerate embeddings
    model = None
    if regenerate_embeddings:
        print(f"\n📥 Đang tải model: {semantic_model}...")
        model = SentenceTransformer(semantic_model)
        print("✅ Model đã sẵn sàng")
    
    # Import documents với BULK API (nhanh hơn)
    print(f"\n📤 Đang import documents (bulk mode)...")
    
    imported = 0
    errors = 0
    pbar = tqdm(total=total_docs, desc="Importing", unit="docs")
    
    for batch_num in range(total_batches):
        batch_file = os.path.join(input_dir, f"batch_{batch_num:04d}.json")
        
        if not os.path.exists(batch_file):
            print(f"\n⚠️  Không tìm thấy {batch_file}")
            continue
        
        # Load batch
        with open(batch_file, 'r', encoding='utf-8') as f:
            batch_data = json.load(f)
        
        # Prepare bulk actions
        actions = []
        for doc in batch_data:
            # Regenerate embedding nếu cần
            if regenerate_embeddings and model and doc.get('embedding') is None:
                text = doc.get('text', '')
                if text:
                    doc['embedding'] = model.encode(text).tolist()
            
            # Bulk action
            actions.append({
                '_index': index_name,
                '_source': doc
            })
        
        # Bulk index (nhanh hơn nhiều)
        try:
            success, failed = bulk(es, actions, raise_on_error=False)
            imported += success
            errors += len(failed)
            pbar.update(success)
            
            if failed:
                print(f"\n⚠️  Batch {batch_num}: {len(failed)} documents failed")
        except Exception as e:
            print(f"\n❌ Lỗi batch {batch_num}: {e}")
            errors += len(actions)
    
    pbar.close()
    
    # Refresh index
    es.indices.refresh(index=index_name)
    
    # Verify
    final_count = es.count(index=index_name)['count']
    
    print("\n" + "="*80)
    print("✅ IMPORT HOÀN THÀNH!")
    print("="*80)
    print(f"📊 Documents imported: {imported:,}")
    print(f"📊 Documents in index: {final_count:,}")
    print(f"❌ Errors: {errors:,}")
    
    if final_count != total_docs:
        print(f"\n⚠️  CẢNH BÁO: Số lượng không khớp!")
        print(f"   Expected: {total_docs:,}")
        print(f"   Got: {final_count:,}")
        print(f"   Missing: {total_docs - final_count:,}")
    else:
        print("\n✅ Số lượng documents khớp hoàn toàn!")
    
    print("\n💡 Bước tiếp theo:")
    print(f"   python speech_retrieval_interactive.py")
    print("="*80)


if __name__ == "__main__":
    # Cấu hình
    HOST = "http://localhost:9200"
    INPUT_DIR = "elasticsearch_data"  # Thư mục clone từ GitHub
    
    print("\n🎯 CÁC TÙY CHỌN:")
    print("1. Import với regenerate embeddings (khuyên dùng)")
    print("2. Import không regenerate embeddings (nhanh hơn)")
    
    choice = input("\nNhập lựa chọn (1/2, Enter=1): ").strip()
    
    regenerate = True if choice != '2' else False
    
    if regenerate:
        print("\n✨ Sẽ tạo lại embeddings từ text")
    else:
        print("\n⚡ Sẽ bỏ qua embeddings (semantic search không hoạt động)")
    
    # Chạy import
    import_index_from_files(
        host=HOST,
        input_dir=INPUT_DIR,
        regenerate_embeddings=regenerate
    )