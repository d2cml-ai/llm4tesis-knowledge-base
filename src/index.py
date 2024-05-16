from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
        CorsOptions,
        SearchIndex,
        ScoringProfile,
        SearchFieldDataType,
        SimpleField,
        SearchableField,
        SearchField,
        VectorSearch,
        VectorSearchProfile,
        HnswAlgorithmConfiguration
)
from azure.storage.blob import BlobClient
from Constants import TEMP_DIR_PATH
from Secrets import (
        SEARCH_ENDPOINT,
        AZURE_SEARCH_API_KEY,
        CHUNK_EMBEDDINGS_BLOB,
        RAW_DATA_CONTAINER,
        CHUNK_INDEX_NAME

)
from cleanup import cleanup
from blob_operations import get_blob_client, get_blob_service
import json
import os

def get_index() -> SearchIndex:
        index_name: str = CHUNK_INDEX_NAME
        fields: list[SearchField] = [
                SimpleField(name = "doc_id", type = SearchFieldDataType.String, key = True),
                SimpleField(name = "title", type = SearchFieldDataType.String),
                SimpleField(name = "abstract", type = SearchFieldDataType.String),
                SimpleField(name = "author", type = SearchFieldDataType.String),
                SimpleField(name = "url", type = SearchFieldDataType.String),
                SearchableField(name = "text", type = SearchFieldDataType.String),
                SearchField(
                        name = "vector",
                        type = SearchFieldDataType.Collection(SearchFieldDataType.Single),
                        searchable = True,
                        vector_search_dimensions = 1536,
                        vector_search_profile_name = "chunks-search"
                )
        ]
        vector_search: VectorSearch = VectorSearch(
                profiles = [VectorSearchProfile(name = "chunks-search", algorithm_configuration_name = "chunks-search-algo")],
                algorithms = [HnswAlgorithmConfiguration(name = "chunks-search-algo")]
        )
        return SearchIndex(name = index_name, fields = fields, vector_search = vector_search)

def get_chunks(path: str) -> list[dict[str, str | list[str] | list[float]]]:
        with open(path, "r", encoding = "utf-8-sig") as f:
                chunks: list[dict[str, str | list[str] | list[float]]] = json.load(f)
        return chunks



def main() -> None:
        print("Downloading chunk embeddings JSON")
        chunks_blob: BlobClient = get_blob_client(CHUNK_EMBEDDINGS_BLOB, RAW_DATA_CONTAINER, get_blob_service())
        chunks_path: str = os.path.join(TEMP_DIR_PATH, CHUNK_EMBEDDINGS_BLOB)
        with open(chunks_path, "wb") as f:
                f.write(chunks_blob.download_blob().readall())
        credential: AzureKeyCredential = AzureKeyCredential(AZURE_SEARCH_API_KEY)
        index_client = SearchIndexClient(SEARCH_ENDPOINT, credential)
        index_config: SearchIndex = get_index()
        index_client.create_index(index_config)
        print("Loading chunk embeddings JSON")
        chunks: list[dict[str, str | list[str] | list[float]]] = get_chunks(chunks_path)
        print("Uploading documents")
        search_client: SearchClient = SearchClient(SEARCH_ENDPOINT, CHUNK_INDEX_NAME, credential)
        search_client.upload_documents(chunks)


if __name__ == "__main__":
        try:
                main()
        except Exception as e:
                raise e
        finally:
                cleanup()

