from audio_search_engine_list import audio_search
AUDIO_DIR = r"E:\DATA\AUDIO_RECOGNIZATION"
KEYFRAME_DIR = r"E:\Workplace\AIC_2025\Data\Keyframes"
print(audio_search(
        query="xe ô tô",
        top_k=500,
        audio_dir=AUDIO_DIR,
        keyframe_dir=KEYFRAME_DIR,
        index_name="speech_index",
        use_fuzzy=True,
        mode="keyword",
        use_semantic=False
    ))
