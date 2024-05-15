from Constants import TEMP_DIR_PATH
from Secrets import (
        EMBEDDINGS_DEPLOYMENT,
        OPENAI_API_KEY,
        RAW_DATA_BLOB_NAME, 
        RAW_DATA_CONTAINER, 
        ABSTRACT_EMBEDDINGS_BLOB, 
        CHUNK_EMBEDDINGS_BLOB,
        AZURE_OPENAI_ENDPOINT
)
from langchain_text_splitters import RecursiveCharacterTextSplitter
from openai import AzureOpenAI
from cleanup import cleanup
from azure.storage.blob import BlobClient, StorageStreamDownloader
from blob_operations import get_blob_client, get_blob_service
import os
from zipfile import ZipFile
from tiktoken import get_encoding, Encoding
import json

def unzip(zip_path: str, extract_path: str) -> None:
        with ZipFile(zip_path) as zip_file:
                zip_file.extractall(extract_path)
        return

def open_file(path: str) -> str:
        with open(path, "r", encoding = "latin1") as f:
                text: str = f.read()
        return text

def get_embeddings(chunks: list[str], client: AzureOpenAI) -> list[list[float]]:
        embed_response = client.embeddings.create(
                input = chunks,
                model = "text-embedding-3-small"
        )
        embeddings: list[list[float]] = [entry.embedding for entry in embed_response.data]
        return embeddings


def main() -> None:
        client: AzureOpenAI = AzureOpenAI(
                azure_endpoint = AZURE_OPENAI_ENDPOINT,
                api_key = OPENAI_API_KEY,
                api_version = "2024-02-01",
                azure_deployment = EMBEDDINGS_DEPLOYMENT
        )
        blob_client: BlobClient = get_blob_client(RAW_DATA_BLOB_NAME + ".zip", RAW_DATA_CONTAINER, get_blob_service())
        blob_download_path = os.path.join(TEMP_DIR_PATH, RAW_DATA_BLOB_NAME + ".zip")
        raw_data_path = os.path.join(TEMP_DIR_PATH, RAW_DATA_BLOB_NAME)
        with open(blob_download_path, "wb") as f:
                download_stream: StorageStreamDownloader[bytes] = blob_client.download_blob()
                f.write(download_stream.readall())
        unzip(blob_download_path, raw_data_path)
        tokenizer: Encoding = get_encoding("cl100k_base")
        def count_tokens(text: str) -> int:
                return len(tokenizer.encode(text))
        chunker: RecursiveCharacterTextSplitter = RecursiveCharacterTextSplitter(
                separators = ["\n\n", ".", "\n"],
                chunk_size = 1024,
                chunk_overlap = 50,
                length_function = count_tokens
        )
        file_paths: list[str] = [path for path in os.listdir(raw_data_path) if path != "metadata.json"]
        metadata_path: str = os.path.join(raw_data_path, "metadata.json")
        chunks_metadata_path: str = os.path.join(TEMP_DIR_PATH, "chunks.json")
        print("Loading metadata...")
        with open(metadata_path, "r", encoding = "latin1") as f:
                entries_metadata: dict[str, dict[str, list[str]]] = json.load(f)
        chunks_metadata: list[dict[str, str | list[str] | list[float]]] = []
        print(f"Embedding {len(file_paths)} files' chunks. This may take several minutes")
        print(" ---+--- 1 ---+--- 2 ---+--- 3 ---+--- 4 ---+--- 5")
        for index, file in enumerate(file_paths[:20]):
                print(".", end = "")
                if (index + 1) % 50 == 0: print(f" {index + 1}")
                text_uuid: str = file.split(".")[0]
                temp_file_metadata: dict[str, list[str]] = entries_metadata[text_uuid]
                file_metadata: dict[str, str | list[str] | list[float]]= {
                        "doc_id": text_uuid,
                        "title": temp_file_metadata["dc.title"][0],
                        "abstract": temp_file_metadata["dc.description.abstract"][0],
                        "author": f"{temp_file_metadata['dc.contributor.author']}",
                        "url": temp_file_metadata["dc.identifier.uri"][0]
                }
                text: str = open_file(os.path.join(raw_data_path, file))
                chunks: list[str] = chunker.split_text(text)
                chunk_embeddings: list[list[float]] = get_embeddings(chunks, client)
                entry_chunks_metadata: list[dict[str, str | list[str] | list[float]]] = []
                for chunk, embed in zip(chunks, chunk_embeddings):
                        temp_metadata: dict[str, str | list[str] | list[float]] = dict(file_metadata)
                        temp_metadata.update({
                                "text": chunk,
                                "vector": embed
                        })
                        entry_chunks_metadata.append(temp_metadata)
                chunks_metadata += entry_chunks_metadata
        print("\nSaving embeddings and metadata JSON")
        with open(chunks_metadata_path, "w", encoding="latin1") as f:
                json.dump(chunks_metadata, f)
        print("Uploading blob")
        chunks_blob_client: BlobClient = get_blob_client(CHUNK_EMBEDDINGS_BLOB, RAW_DATA_CONTAINER, get_blob_service())
        with open(chunks_metadata_path, "rb") as f:
                chunks_blob_client.upload_blob(f, overwrite = True)

        
                
                

        



if __name__ == "__main__":
        try:
                main()
        except Exception as e:
                raise e
        # finally:
        #         cleanup()