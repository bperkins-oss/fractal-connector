"""SEC EDGAR filings plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

import aiohttp

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class SECEdgarPlugin(DataSourcePlugin):
    """Plugin for SEC EDGAR filings data."""

    plugin_id = "sec_edgar"
    plugin_name = "SEC EDGAR"
    plugin_description = "Access SEC filings (10-K, 10-Q, 8-K, etc.)"
    plugin_icon = "gavel"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="user_agent",
                label="User Agent Email",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="your@email.com",
                help_text="SEC requires a user agent with contact email",
            ),
            CredentialField(
                name="cik",
                label="CIK Number(s)",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="320193, 789019",
                help_text="Company CIK numbers (comma-separated)",
            ),
            CredentialField(
                name="ticker",
                label="Ticker Symbol(s)",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="AAPL, MSFT",
                help_text="Ticker symbols (will be converted to CIK)",
            ),
            CredentialField(
                name="filing_type",
                label="Filing Type",
                field_type=FieldType.SELECT,
                required=True,
                default="10-K",
                options=[
                    {"value": "10-K", "label": "10-K (Annual Report)"},
                    {"value": "10-Q", "label": "10-Q (Quarterly Report)"},
                    {"value": "8-K", "label": "8-K (Current Report)"},
                    {"value": "4", "label": "Form 4 (Insider Trading)"},
                    {"value": "13F", "label": "13F (Institutional Holdings)"},
                    {"value": "DEF 14A", "label": "DEF 14A (Proxy Statement)"},
                    {"value": "S-1", "label": "S-1 (IPO Registration)"},
                ],
            ),
            CredentialField(
                name="limit",
                label="Number of Filings",
                field_type=FieldType.NUMBER,
                required=False,
                default="10",
                placeholder="10",
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

    async def _ticker_to_cik(self, ticker: str) -> str | None:
        """Convert ticker symbol to CIK."""
        url = "https://www.sec.gov/cgi-bin/browse-edgar"
        params = {"action": "getcompany", "CIK": ticker, "type": "", "dateb": "", "owner": "include", "count": "1", "output": "atom"}
        headers = {"User-Agent": self.credentials.get("user_agent", "FractalConnector contact@example.com")}

        try:
            async with self._session.get(url, params=params, headers=headers) as response:
                if response.status == 200:
                    text = await response.text()
                    # Parse CIK from response
                    import re
                    match = re.search(r'CIK=(\d+)', text)
                    if match:
                        return match.group(1).zfill(10)
        except:
            pass
        return None

    async def test_connection(self) -> tuple[bool, str]:
        user_agent = self.credentials.get("user_agent", "")
        if not user_agent or "@" not in user_agent:
            return False, "Valid email required for User Agent"

        cik = self.credentials.get("cik", "")
        ticker = self.credentials.get("ticker", "")
        if not cik and not ticker:
            return False, "Either CIK or ticker is required"

        try:
            async with aiohttp.ClientSession() as session:
                headers = {"User-Agent": user_agent}
                async with session.get("https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=AAPL&type=10-K&count=1&output=atom", headers=headers) as response:
                    if response.status == 200:
                        return True, "Connected to SEC EDGAR"
                    return False, f"SEC API returned {response.status}"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._session:
            return

        user_agent = self.credentials.get("user_agent", "")
        ciks = [c.strip().zfill(10) for c in self.credentials.get("cik", "").split(",") if c.strip()]
        tickers = [t.strip().upper() for t in self.credentials.get("ticker", "").split(",") if t.strip()]
        filing_type = self.credentials.get("filing_type", "10-K")
        limit = int(self.credentials.get("limit", 10) or 10)

        headers = {"User-Agent": user_agent}

        # Convert tickers to CIKs
        for ticker in tickers:
            cik = await self._ticker_to_cik(ticker)
            if cik and cik not in ciks:
                ciks.append(cik)

        for cik in ciks:
            try:
                # Use the submissions API
                url = f"https://data.sec.gov/submissions/CIK{cik}.json"

                async with self._session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        company_name = data.get("name", "Unknown")
                        filings = data.get("filings", {}).get("recent", {})

                        forms = filings.get("form", [])
                        dates = filings.get("filingDate", [])
                        accessions = filings.get("accessionNumber", [])
                        descriptions = filings.get("primaryDocument", [])

                        count = 0
                        for i, form in enumerate(forms):
                            if form == filing_type or filing_type in form:
                                if count >= limit:
                                    break

                                yield DataRecord(
                                    source_id=self.source_id,
                                    source_type=self.plugin_id,
                                    timestamp=datetime.utcnow().isoformat(),
                                    data={
                                        "cik": cik,
                                        "company": company_name,
                                        "form_type": form,
                                        "filing_date": dates[i] if i < len(dates) else None,
                                        "accession_number": accessions[i] if i < len(accessions) else None,
                                        "document": descriptions[i] if i < len(descriptions) else None,
                                        "url": f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{accessions[i].replace('-', '')}/{descriptions[i]}" if i < len(accessions) and i < len(descriptions) else None,
                                    },
                                    metadata={"filing_type": filing_type},
                                )
                                count += 1

            except Exception as e:
                print(f"SEC EDGAR error for CIK {cik}: {e}")
