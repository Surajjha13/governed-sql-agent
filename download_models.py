import os
from sentence_transformers import SentenceTransformer

def download():
    model_name = "all-MiniLM-L6-v2"
    # Ensure the models directory exists in the container
    cache_dir = os.path.join(os.getcwd(), "models")
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    
    print(f"Downloading model '{model_name}' to {cache_dir}...")
    SentenceTransformer(model_name, cache_folder=cache_dir)
    print("Download complete.")

if __name__ == "__main__":
    download()
