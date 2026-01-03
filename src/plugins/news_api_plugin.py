"""News API plugin for financial news."""
from datetime import datetime
from typing import Any, AsyncIterator

import aiohttp

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class NewsAPIPlugin(DataSourcePlugin):
    """Plugin for NewsAPI.org news data."""

    plugin_id = "news_api"
    plugin_name = "News API"
    plugin_description = "Get news articles from NewsAPI.org"
    plugin_icon = "newspaper"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="api_key",
                label="API Key",
                field_type=FieldType.PASSWORD,
                required=True,
                help_text="Get from newsapi.org",
            ),
            CredentialField(
                name="endpoint",
                label="Endpoint",
                field_type=FieldType.SELECT,
                required=True,
                default="everything",
                options=[
                    {"value": "everything", "label": "Everything (Search)"},
                    {"value": "top-headlines", "label": "Top Headlines"},
                ],
            ),
            CredentialField(
                name="query",
                label="Search Query",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="Apple OR Tesla OR Bitcoin",
                help_text="Keywords to search for",
            ),
            CredentialField(
                name="sources",
                label="News Sources",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="bloomberg, reuters, cnbc",
                help_text="Comma-separated source IDs",
            ),
            CredentialField(
                name="category",
                label="Category",
                field_type=FieldType.SELECT,
                required=False,
                default="business",
                options=[
                    {"value": "business", "label": "Business"},
                    {"value": "technology", "label": "Technology"},
                    {"value": "general", "label": "General"},
                    {"value": "science", "label": "Science"},
                    {"value": "health", "label": "Health"},
                ],
            ),
            CredentialField(
                name="language",
                label="Language",
                field_type=FieldType.SELECT,
                required=False,
                default="en",
                options=[
                    {"value": "en", "label": "English"},
                    {"value": "es", "label": "Spanish"},
                    {"value": "de", "label": "German"},
                    {"value": "fr", "label": "French"},
                    {"value": "zh", "label": "Chinese"},
                ],
            ),
            CredentialField(
                name="sort_by",
                label="Sort By",
                field_type=FieldType.SELECT,
                required=False,
                default="publishedAt",
                options=[
                    {"value": "publishedAt", "label": "Published Date"},
                    {"value": "relevancy", "label": "Relevancy"},
                    {"value": "popularity", "label": "Popularity"},
                ],
            ),
            CredentialField(
                name="page_size",
                label="Articles per Request",
                field_type=FieldType.NUMBER,
                required=False,
                default="20",
                placeholder="20",
            ),
        ]

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._session = None

    async def connect(self) -> bool:
        self._session = aiohttp.ClientSession()
        self._connected = True
        return True

    async def disconnect(self):
        if self._session:
            await self._session.close()
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        api_key = self.credentials.get("api_key", "")
        if not api_key:
            return False, "API key is required"

        try:
            async with aiohttp.ClientSession() as session:
                url = "https://newsapi.org/v2/top-headlines"
                params = {"apiKey": api_key, "country": "us", "pageSize": 1}
                async with session.get(url, params=params) as response:
                    data = await response.json()
                    if data.get("status") == "ok":
                        return True, f"Connected! {data.get('totalResults', 0)} articles available"
                    return False, data.get("message", "Unknown error")
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._session:
            return

        api_key = self.credentials.get("api_key", "")
        endpoint = self.credentials.get("endpoint", "everything")
        query = self.credentials.get("query", "")
        sources = self.credentials.get("sources", "")
        category = self.credentials.get("category", "")
        language = self.credentials.get("language", "en")
        sort_by = self.credentials.get("sort_by", "publishedAt")
        page_size = int(self.credentials.get("page_size", 20) or 20)

        base_url = f"https://newsapi.org/v2/{endpoint}"
        params = {"apiKey": api_key, "pageSize": page_size, "language": language}

        if query:
            params["q"] = query
        if sources:
            params["sources"] = sources
        if endpoint == "top-headlines" and category:
            params["category"] = category
        if endpoint == "everything":
            params["sortBy"] = sort_by
        if endpoint == "top-headlines" and not sources:
            params["country"] = "us"

        try:
            async with self._session.get(base_url, params=params) as response:
                data = await response.json()

                if data.get("status") == "ok":
                    articles = data.get("articles", [])
                    for i, article in enumerate(articles):
                        yield DataRecord(
                            source_id=self.source_id,
                            source_type=self.plugin_id,
                            timestamp=datetime.utcnow().isoformat(),
                            data={
                                "title": article.get("title"),
                                "description": article.get("description"),
                                "content": article.get("content"),
                                "author": article.get("author"),
                                "source": article.get("source", {}).get("name"),
                                "url": article.get("url"),
                                "published_at": article.get("publishedAt"),
                                "image_url": article.get("urlToImage"),
                            },
                            metadata={"index": i, "query": query},
                        )

        except Exception as e:
            print(f"NewsAPI fetch error: {e}")
