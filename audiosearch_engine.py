"""
ELASTICSEARCH SPEECH RETRIEVAL SYSTEM - FINAL VERSION
với Vietnamese Plugin + Fuzzy Search + Incremental Indexing
Compatible with Elasticsearch 8.7.0 + elasticsearch-analysis-vietnamese plugin

Tính năng:
✅ Vietnamese Plugin Analyzer (tách từ tiếng Việt chuyên nghiệp)
✅ Fuzzy Search (cho phép gõ sai 1-2 ký tự)
✅ Incremental Indexing (bỏ qua file đã index)
✅ BM25 + Vector Search
✅ 4 chế độ sử dụng linh hoạt
"""

import os
import json
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from typing import List, Dict
import warnings
import hashlib
warnings.filterwarnings('ignore')


# ========================== HELPER FUNCTIONS ==========================
def list_keyframes_in_range(entry: Dict, base_keyframe_dir: str) -> List[str]:
    """
    Lấy danh sách frame thực tế trong thư mục Keyframes
    mà có số frame nằm trong [start_frame, end_frame].    
    """
    json_file = os.path.basename(entry["file"])
    video_name = os.path.splitext(json_file)[0]
    k_folder = video_name.split('_')[0]  # K01
    keyframe_folder = os.path.join(base_keyframe_dir, k_folder, video_name)
    
    start_f, end_f = entry["start_frame"], entry["end_frame"]
    frame_paths = []

    if os.path.exists(keyframe_folder):
        for fname in sorted(os.listdir(keyframe_folder)):
            if fname.endswith((".webp", ".jpg", ".png")):
                try:
                    frame_num = int(os.path.splitext(fname)[0])
                    if start_f <= frame_num <= end_f:
                        frame_paths.append(os.path.join(keyframe_folder, fname))
                except ValueError:
                    continue
    
    return frame_paths


def get_file_hash(filepath: str) -> str:
    """
    Tính hash của file để phát hiện thay đổi
    """
    hash_md5 = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception:
        return ""


# ========================== MAIN CLASS ==========================
class SpeechRetrievalES:
    """
    Speech Retrieval System using Elasticsearch
    
    Features:
    - Vietnamese Plugin Analyzer (tách từ tiếng Việt chuyên nghiệp)
    - Keyword search (BM25) with Fuzzy matching
    - Semantic search (Dense Vector)
    - Keyframe extraction
    - Incremental indexing (skip already indexed files)
    """
    
    def __init__(
        self,
        context_json_dir: str,
        base_keyframe_dir: str,
        host: str = "http://localhost:9200",
        index_name: str = "speech_index",
        semantic_model: str = "sentence-transformers/stsb-xlm-r-multilingual",
        use_semantic: bool = True,
        load_data: bool = True,
        force_reindex: bool = False,  # 🆕 Cưỡng chế index lại tất cả
        index_tracker_file: str = ".indexed_files.json"  # 🆕 File lưu danh sách đã index
    ):
        """
        Khởi tạo Speech Retrieval System
        
        Args:
            context_json_dir: Thư mục chứa các file JSON transcript
            base_keyframe_dir: Thư mục chứa keyframes
            host: Elasticsearch host URL
            index_name: Tên index trong Elasticsearch
            semantic_model: Model cho semantic search
            use_semantic: Có sử dụng semantic search không
            load_data: Có load dữ liệu vào ES không (False nếu đã index rồi)
            force_reindex: True = index lại tất cả (bỏ qua tracking)
            index_tracker_file: File JSON lưu danh sách file đã index
        """
        self.context_json_dir = context_json_dir
        self.base_keyframe_dir = base_keyframe_dir
        self.index_name = index_name
        self.use_semantic = use_semantic
        self.force_reindex = force_reindex
        self.index_tracker_file = index_tracker_file

        print("="*80)
        print("🚀 KHỞI ĐỘNG SPEECH RETRIEVAL SYSTEM")
        print("="*80)
        print("✨ Vietnamese Plugin + Fuzzy Search + Incremental Indexing")
        
        # Load danh sách file đã index
        self.indexed_files = self._load_indexed_files()
        
        # Kết nối Elasticsearch
        self._connect_elasticsearch(host)
        
        # Load semantic model
        if use_semantic:
            self._load_semantic_model(semantic_model)
        
        # Tạo index
        self._setup_index()
        
        # Index dữ liệu
        if load_data:
            self._index_data()

    def _load_indexed_files(self) -> Dict[str, str]:
        """
        Load danh sách file đã index từ file JSON
        Returns: Dict {filepath: hash}
        """
        if self.force_reindex:
            print("\n⚠️  Force reindex enabled - sẽ index lại tất cả file")
            return {}
        
        if os.path.exists(self.index_tracker_file):
            try:
                with open(self.index_tracker_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                print(f"\n📋 Đã load thông tin {len(data)} file đã index từ {self.index_tracker_file}")
                return data
            except Exception as e:
                print(f"\n⚠️  Không thể đọc {self.index_tracker_file}: {e}")
                return {}
        else:
            print(f"\n📋 File tracker chưa tồn tại - sẽ tạo mới")
            return {}

    def _save_indexed_files(self):
        """
        Lưu danh sách file đã index vào file JSON
        """
        try:
            with open(self.index_tracker_file, 'w', encoding='utf-8') as f:
                json.dump(self.indexed_files, f, ensure_ascii=False, indent=2)
            print(f"\n💾 Đã lưu thông tin {len(self.indexed_files)} file vào {self.index_tracker_file}")
        except Exception as e:
            print(f"\n⚠️  Không thể lưu {self.index_tracker_file}: {e}")

    def _should_index_file(self, filepath: str) -> bool:
        """
        Kiểm tra xem file có cần index không
        Returns: True nếu cần index (file mới hoặc đã thay đổi)
        """
        if self.force_reindex:
            return True
        
        # Tính hash của file hiện tại
        current_hash = get_file_hash(filepath)
        
        # Kiểm tra file đã được index chưa
        if filepath in self.indexed_files:
            stored_hash = self.indexed_files[filepath]
            # Nếu hash giống nhau = file không đổi = không cần index lại
            if stored_hash == current_hash:
                return False
        
        return True

    def _connect_elasticsearch(self, host: str):
        """Kết nối tới Elasticsearch"""
        print(f"\n🔌 Đang kết nối tới {host}...")
        
        try:
            from elasticsearch import __version__ as es_version
            print(f"   Phiên bản client: {es_version}")
            
            # Tạo connection
            self.es = Elasticsearch(
                hosts=[host],
                verify_certs=False,
                ssl_show_warn=False,
                request_timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
            
            # Kiểm tra kết nối
            info = self.es.info()
            print(f"✅ Kết nối thành công!")
            print(f"   - Cluster: {info['cluster_name']}")
            print(f"   - Version: {info['version']['number']}")
            
        except Exception as e:
            print(f"\n❌ LỖI KẾT NỐI: {e}")
            print("\n🔧 Hướng dẫn khắc phục:")
            print("1. Kiểm tra Docker container: docker ps")
            print("2. Xem logs: docker logs elasticsearch")
            print("3. Test curl: curl http://localhost:9200")
            print("4. Kiểm tra version: pip show elasticsearch")
            raise ConnectionError("Không thể kết nối tới Elasticsearch!")

    def _load_semantic_model(self, model_name: str):
        """Load sentence transformer model"""
        print(f"\n📥 Đang tải model: {model_name}...")
        self.model = SentenceTransformer(model_name)
        print("✅ Model đã sẵn sàng!")

    def _setup_index(self):
        """
        ✨ Tạo index với Vietnamese Plugin Analyzer
        
        Plugin 'vietnamese' cung cấp:
        - Vietnamese tokenizer: Tách từ tiếng Việt chính xác
        - Stop words: Tự động loại bỏ từ vô nghĩa
        - Normalization: Chuẩn hóa text tiếng Việt
        """
        print(f"\n🔧 Kiểm tra index '{self.index_name}'...")
        
        try:
            if self.es.indices.exists(index=self.index_name):
                print(f"ℹ️  Index đã tồn tại")
                return
            
            print(f"🔨 Tạo index mới với Vietnamese Plugin...")
            
            # ✨ Sử dụng analyzer "vietnamese" từ plugin
            mapping = {
                "mappings": {
                    "properties": {
                        "text": {
                            "type": "text",
                            "analyzer": "vietnamese"  # 👈 Analyzer từ plugin
                        },
                        "start_frame": {"type": "integer"},
                        "end_frame": {"type": "integer"},
                        "start_sec": {"type": "float"},
                        "end_sec": {"type": "float"},
                        "file": {"type": "keyword"},
                        "video_name": {"type": "keyword"},
                        "L": {"type": "integer"},
                        "embedding": {
                            "type": "dense_vector",
                            "dims": 768,
                            "index": True,
                            "similarity": "cosine"
                        }
                    }
                }
            }
            
            self.es.indices.create(index=self.index_name, body=mapping)
            print("✅ Index đã được tạo với Vietnamese Plugin!")
            print("   - Tokenizer: Vietnamese word segmentation")
            print("   - Analyzer: Optimized for Vietnamese text")
            
        except Exception as e:
            print(f"⚠️  Lỗi khi tạo index: {e}")
            if "vietnamese" in str(e).lower():
                print("\n❌ PLUGIN CHƯA ĐƯỢC CÀI ĐẶT!")
                print("Plugin đã cài chưa? Kiểm tra:")
                print("  docker exec -it elasticsearch bin/elasticsearch-plugin list")
            raise

    def _index_data(self):
        """Đọc và index dữ liệu từ các file JSON (chỉ index file mới/thay đổi)"""
        print("\n" + "="*80)
        print("📂 BẮT ĐẦU INDEX DỮ LIỆU (INCREMENTAL)")
        print("="*80)
        print(f"Thư mục: {self.context_json_dir}\n")
        
        # Đếm số file
        all_files = []
        for root, _, files in os.walk(self.context_json_dir):
            for file in files:
                if file.endswith(".json"):
                    all_files.append(os.path.join(root, file))
        
        print(f"📊 Tổng số file JSON: {len(all_files)}")
        print(f"📋 Số file đã index trước đó: {len(self.indexed_files)}")
        
        # Lọc file cần index
        files_to_index = [f for f in all_files if self._should_index_file(f)]
        files_skipped = len(all_files) - len(files_to_index)
        
        print(f"🆕 File cần index: {len(files_to_index)}")
        print(f"⏭️  File bỏ qua (đã index): {files_skipped}")
        
        if len(files_to_index) == 0:
            print("\n✅ Không có file mới - Bỏ qua indexing")
            return
        
        print()
        
        count = 0
        indexed_count = 0
        
        for full_path in files_to_index:
            video_name = os.path.splitext(os.path.basename(full_path))[0]
            
            try:
                with open(full_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                file_display = os.path.basename(full_path)
                print(f"📄 {file_display} ({len(data)} entries)")
                
                # Index từng entry với progress bar
                for idx, item in enumerate(tqdm(data, desc=f"  Indexing", leave=False)):
                    text = item.get("text", "").strip()
                    if not text:
                        continue
                    
                    doc = {
                        "text": text,
                        "start_frame": item.get("start_frame"),
                        "end_frame": item.get("end_frame"),
                        "start_sec": item.get("start_sec"),
                        "end_sec": item.get("end_sec"),
                        "file": full_path,
                        "video_name": video_name,
                        "L": idx,
                    }

                    # Thêm embedding
                    if self.use_semantic:
                        doc["embedding"] = self.model.encode(text).tolist()

                    self.es.index(index=self.index_name, document=doc)
                    count += 1
                
                # Cập nhật hash của file vào tracker
                self.indexed_files[full_path] = get_file_hash(full_path)
                indexed_count += 1
                
                print(f"   ✅ Đã index xong\n")
                
            except Exception as e:
                print(f"   ⚠️  Lỗi: {e}\n")
        
        # Lưu danh sách file đã index
        self._save_indexed_files()
        
        print("="*80)
        print(f"🎉 HOÀN THÀNH!")
        print(f"   - Đã index: {indexed_count} file")
        print(f"   - Tổng documents: {count:,}")
        print(f"   - Bỏ qua: {files_skipped} file (đã index trước đó)")
        print("="*80)

    def search(self, query: str, k: int = 3, use_fuzzy: bool = False) -> Dict:
        """
        ✨ Tìm kiếm với Vietnamese Plugin + Fuzzy Search
        
        Args:
            query: Câu truy vấn
            k: Số kết quả trả về
            use_fuzzy: Bật fuzzy search (cho phép gõ sai)
            
        Returns:
            Dict chứa kết quả keyword và semantic
        """
        results = {}

        # 1. Keyword Search (BM25) với Fuzzy option
        if use_fuzzy:
            # ✨ FUZZY SEARCH - Cho phép gõ sai 1-2 ký tự
            keyword_query = {
                "size": k,
                "query": {
                    "match": {
                        "text": {
                            "query": query,
                            "fuzziness": "AUTO",  # Tự động điều chỉnh (1-2 ký tự sai)
                            "prefix_length": 1,    # Ít nhất 1 ký tự đầu đúng
                            "max_expansions": 50   # Tối đa 50 biến thể
                        }
                    }
                }
            }
        else:
            # Keyword search thông thường
            keyword_query = {
                "size": k,
                "query": {"match": {"text": query}}
            }
        
        resp = self.es.search(index=self.index_name, body=keyword_query)
        results["keyword"] = [hit["_source"] for hit in resp["hits"]["hits"]]

        # 2. Semantic Search (Dense Vector)
        if self.use_semantic:
            query_vec = self.model.encode(query).tolist()
            
            semantic_query = {
                "size": k,
                "knn": {
                    "field": "embedding",
                    "query_vector": query_vec,
                    "k": k,
                    "num_candidates": 100
                }
            }
            
            try:
                resp = self.es.search(index=self.index_name, **semantic_query)
                results["semantic"] = [hit["_source"] for hit in resp["hits"]["hits"]]
            except Exception as e:
                print(f"⚠️  Lỗi semantic search: {e}")
                results["semantic"] = []

        return results

    def search_with_frames(self, query: str, k: int = 3, use_fuzzy: bool = False) -> Dict:
        """
        Tìm kiếm và kèm theo keyframes
        
        Args:
            query: Câu truy vấn
            k: Số kết quả trả về
            use_fuzzy: Bật fuzzy search
            
        Returns:
            Dict chứa kết quả search + danh sách keyframes
        """
        results = self.search(query, k, use_fuzzy)
        output = {}

        for mode in ["semantic", "keyword"]:
            output[mode] = []
            for r in results.get(mode, []):
                frames = list_keyframes_in_range(r, self.base_keyframe_dir)
                r["frames"] = frames
                r["num_frames"] = len(frames)
                output[mode].append(r)
        
        return output

    def display_results(self, results: Dict):
        """
        Hiển thị kết quả search một cách đẹp mắt
        
        Args:
            results: Dict chứa kết quả từ search_with_frames()
        """
        for mode in ["semantic", "keyword"]:
            print(f"\n{'='*80}")
            print(f"🔍 {mode.upper()} SEARCH RESULTS")
            print('='*80)
            
            mode_results = results.get(mode, [])
            if not mode_results:
                print("❌ Không tìm thấy kết quả")
                continue
            
            for i, r in enumerate(mode_results, 1):
                print(f"\n[{i}] Video: {r.get('video_name', 'N/A')}")
                print(f"    📝 Text: {r['text'][:150]}...")
                print(f"    ⏱️  Time: {r['start_sec']:.2f}s → {r['end_sec']:.2f}s")
                print(f"    🎞️  Frames: {r['start_frame']} → {r['end_frame']}")
                print(f"    📸 Keyframes: {r.get('num_frames', 0)} frames")
                
                if r.get('frames'):
                    print(f"    📁 Danh sách keyframes:")
                    for frame in r['frames'][:5]:
                        print(f"       • {frame}")
                    if len(r['frames']) > 5:
                        print(f"       ... và {len(r['frames']) - 5} frames khác")

    def reset_index_tracker(self):
        """
        🗑️ XÓA file tracker - dùng khi muốn index lại từ đầu
        """
        if os.path.exists(self.index_tracker_file):
            os.remove(self.index_tracker_file)
            print(f"🗑️  Đã xóa {self.index_tracker_file}")
            self.indexed_files = {}
        else:
            print(f"ℹ️  File {self.index_tracker_file} không tồn tại")


# ========================== MAIN PROGRAM ==========================
if __name__ == "__main__":
    # Cấu hình đường dẫn
    AUDIO_DIR = r"E:\DATA\AUDIO_RECOGNIZATION"
    KEYFRAME_DIR = r"E:\DATA\AIC_2025"
    
    # 🎯 CÁC CHẾ ĐỘ SỬ DỤNG:
    
    # ============ CHẾ ĐỘ 1: LẦN ĐẦU TIÊN (Index ) ============
    retrieval = SpeechRetrievalES(
        context_json_dir=AUDIO_DIR,
        base_keyframe_dir=KEYFRAME_DIR,
        host="http://localhost:9200",
        index_name="speech_index_vn",
        use_semantic=True,
        load_data=True,
        force_reindex=False  # False = chỉ index file mới
    )
    

    
    # ============ CHẾ ĐỘ 3: CHỈ TÌM KIẾM (Không index) ============
    # retrieval = SpeechRetrievalES(
    #     context_json_dir=AUDIO_DIR,
    #     base_keyframe_dir=KEYFRAME_DIR,
    #     host="http://localhost:9200",
    #     index_name="speech_index_vn",
    #     use_semantic=True,
    #     load_data=False  # False = không index, chỉ search
    # )
    
    # # ========================== DEMO TÌM KIẾM ==========================
    # print("\n" + "="*80)
    # print("🔍 DEMO TÌM KIẾM")
    # print("="*80)
    
    # # Ví dụ 1: Search thông thường
    # query1 = "xe ô tô"
    # print(f"\nQuery: '{query1}' (không fuzzy)")
    # results1 = retrieval.search_with_frames(query1, k=5, use_fuzzy=False)
    # retrieval.display_results(results1)
    
    # Ví dụ 2: Search với fuzzy
    # query2 = "oto"
    # print(f"\nQuery: '{query2}' (với fuzzy - gõ sai)")
    # results2 = retrieval.search_with_frames(query2, k=5, use_fuzzy=True)
    # retrieval.display_results(results2)
    
    # print("\n" + "="*80)
    # print("✅ HOÀN THÀNH!")
    # print("="*80)
    
    # 💡 TIP: Nếu muốn xóa tracker và index lại từ đầu:
    # retrieval.reset_index_tracker()