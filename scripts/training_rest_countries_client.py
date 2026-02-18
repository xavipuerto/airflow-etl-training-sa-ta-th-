#!/usr/bin/env python3
"""
HTTP Client for REST Countries API - ETL Training
==================================================

Public API: https://restcountries.com/
No authentication, ideal for learning

This client demonstrates:
- HTTP requests handling
- Retry policy
- Structured logging
- Error handling
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests


def pretty(obj: Any) -> str:
    """Pretty print JSON"""
    return json.dumps(obj, ensure_ascii=False, indent=2)


def now_ms() -> int:
    """Current time in milliseconds"""
    return int(time.time() * 1000)


@dataclass
class CallResult:
    """Result of an HTTP call"""
    ok: bool
    method: str
    url: str
    status: int
    elapsed_ms: int
    json_obj: Optional[Any]
    text: str
    
    def __str__(self):
        emoji = "✅" if self.ok else "❌"
        return f"{emoji} {self.method} {self.url} → {self.status} ({self.elapsed_ms}ms)"


class RestCountriesClient:
    """
    HTTP Client for REST Countries API
    
    Retry policy:
    - Retry on: 429 (rate limit), 500, 502, 503, 504
    - Maximum 3 attempts
    - Backoff: 2s, 4s, 8s
    """
    
    def __init__(
        self,
        base_url: str = "https://restcountries.com/v3.1",
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: int = 2,
    ):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Training-ETL-Client/1.0',
            'Accept': 'application/json',
        })
    
    def _should_retry(self, status: int) -> bool:
        """Determines if we should retry based on status code"""
        return status in (429, 500, 502, 503, 504)
    
    def _call(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> CallResult:
        """
        Execute an HTTP call with retry policy
        
        Args:
            method: GET, POST, PUT, DELETE
            endpoint: /all, /name/{name}, etc.
            params: Query parameters
            json_data: JSON body (for POST/PUT)
            
        Returns:
            CallResult with the result
        """
        url = f"{self.base_url}{endpoint}"
        attempt = 0
        
        while attempt < self.max_retries:
            attempt += 1
            start_ms = now_ms()
            
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_data,
                    timeout=self.timeout,
                )
                
                elapsed_ms = now_ms() - start_ms
                json_obj, text = self._safe_json(response)
                
                # If successful, return
                if 200 <= response.status_code < 300:
                    return CallResult(
                        ok=True,
                        method=method,
                        url=url,
                        status=response.status_code,
                        elapsed_ms=elapsed_ms,
                        json_obj=json_obj,
                        text=text,
                    )
                
                # If we should retry
                if self._should_retry(response.status_code) and attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** (attempt - 1))
                    print(f"⚠️  Attempt {attempt}/{self.max_retries} failed: {response.status_code}. Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                
                # Final error
                return CallResult(
                    ok=False,
                    method=method,
                    url=url,
                    status=response.status_code,
                    elapsed_ms=elapsed_ms,
                    json_obj=json_obj,
                    text=text,
                )
                
            except Exception as e:
                elapsed_ms = now_ms() - start_ms
                
                # Retry on network exceptions
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** (attempt - 1))
                    print(f"⚠️  Exception on attempt {attempt}/{self.max_retries}: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                
                # Final exception
                return CallResult(
                    ok=False,
                    method=method,
                    url=url,
                    status=0,
                    elapsed_ms=elapsed_ms,
                    json_obj=None,
                    text=str(e),
                )
        
        # Should not reach here, but for safety
        return CallResult(
            ok=False,
            method=method,
            url=url,
            status=0,
            elapsed_ms=0,
            json_obj=None,
            text="Max retries exceeded",
        )
    
    def _safe_json(self, resp: requests.Response) -> Tuple[Optional[Any], str]:
        """Try to parse JSON, if it fails return None and text"""
        try:
            return resp.json(), resp.text
        except Exception:
            return None, resp.text
    
    # =========================================================================
    # API METHODS - Multiple calls to get all fields
    # =========================================================================
    # The REST Countries API limits to 10 fields per request.
    # To get all data, we make multiple calls with different fields.
    
    def get_all_countries_basic(self) -> CallResult:
        """
        GET /all - Get BASIC fields of countries (8 fields)
        
        Fields: cca2, cca3, name, capital, region, subregion, area, population
        
        Returns:
            CallResult with list of countries with basic fields
        """
        fields = 'cca2,cca3,name,capital,region,subregion,area,population'
        return self._call('GET', '/all', params={'fields': fields})
    
    def get_all_countries_geo(self) -> CallResult:
        """
        GET /all - Get GEOGRAPHIC fields of countries (5 fields)
        
        Fields: cca2, cca3, latlng, landlocked, borders
        
        Returns:
            CallResult with list of countries with geographic fields
        """
        fields = 'cca2,cca3,latlng,landlocked,borders'
        return self._call('GET', '/all', params={'fields': fields})
    
    def get_all_countries_culture(self) -> CallResult:
        """
        GET /all - Get CULTURAL/ECONOMIC fields of countries (6 fields)
        
        Fields: cca2, cca3, languages, currencies, timezones, flags
        
        Returns:
            CallResult with list of countries with cultural fields
        """
        fields = 'cca2,cca3,languages,currencies,timezones,flags'
        return self._call('GET', '/all', params={'fields': fields})
    
    def get_all_countries_political(self) -> CallResult:
        """
        GET /all - Get POLITICAL fields of countries (5 fields)
        
        Fields: cca2, cca3, independent, unMember, ccn3
        
        Returns:
            CallResult with list of countries with political fields
        """
        fields = 'cca2,cca3,independent,unMember,ccn3'
        return self._call('GET', '/all', params={'fields': fields})
    
    def get_all_countries(self) -> CallResult:
        """
        GET /all - Get basic fields of countries (DEPRECATED - use specific methods)
        
        NOTE: This method uses only 10 fields for compatibility.
        To get all data, use:
        - get_all_countries_basic()
        - get_all_countries_geo()
        - get_all_countries_culture()
        - get_all_countries_political()
        
        Returns:
            CallResult with list of countries with basic fields
        """
        fields = 'cca2,cca3,name,capital,region,subregion,area,population'
        return self._call('GET', '/all', params={'fields': fields})
    
    def get_country_by_name(self, name: str) -> CallResult:
        """
        GET /name/{name} - Search countries by name
        
        Args:
            name: Country name (can be partial)
            
        Returns:
            CallResult with matching countries
        """
        fields = 'cca2,cca3,name,capital,region,subregion,latlng,area,population,flags'
        return self._call('GET', f'/name/{name}', params={'fields': fields})
    
    def get_countries_by_region(self, region: str) -> CallResult:
        """
        GET /region/{region} - Get countries by region
        
        Args:
            region: africa, americas, asia, europe, oceania
            
        Returns:
            CallResult with countries from the region
        """
        fields = 'cca2,cca3,name,capital,region,subregion,latlng,area,population,flags'
        return self._call('GET', f'/region/{region}', params={'fields': fields})
    
    def get_countries_by_subregion(self, subregion: str) -> CallResult:
        """
        GET /subregion/{subregion} - Get countries by subregion
        
        Args:
            subregion: Southern Europe, South America, etc.
            
        Returns:
            CallResult with countries from the subregion
        """
        fields = 'cca2,cca3,name,capital,region,subregion,latlng,area,population,flags'
        return self._call('GET', f'/subregion/{subregion}', params={'fields': fields})
    
    def get_country_by_code(self, code: str) -> CallResult:
        """
        GET /alpha/{code} - Get country by ISO code
        
        Args:
            code: ISO code 2 or 3 letters (ES, ESP, USA, US, etc.)
            
        Returns:
            CallResult with country data
        """
        fields = 'cca2,cca3,name,capital,region,subregion,latlng,area,population,flags'
        return self._call('GET', f'/alpha/{code}', params={'fields': fields})


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def extract_country_data(country: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract and normalize relevant country data
    
    This function demonstrates the TRANSFORM phase in ETL:
    - Extract nested fields
    - Handle null values
    - Normalize formats
    
    NOTE: The REST Countries API limits to 10 fields maximum.
    Available fields: cca2, cca3, name, capital, region, subregion, latlng, area, population, flags
    NOT available fields (NULL): ccn3, landlocked, languages, currencies, timezones, borders, independent, unMember
    
    Args:
        country: JSON object from API
        
    Returns:
        Dict with normalized data
    """
    return {
        # Identifiers (available)
        'code_iso2': country.get('cca2'),
        'code_iso3': country.get('cca3'),
        'code_numeric': None,  # Not available with fields limit
        
        # Names (available)
        'name_common': country.get('name', {}).get('common'),
        'name_official': country.get('name', {}).get('official'),
        'name_native': json.dumps(country.get('name', {}).get('nativeName', {})),
        
        # Geography (available)
        'capital': json.dumps(country.get('capital', [])),
        'region': country.get('region'),
        'subregion': country.get('subregion'),
        'latitude': country.get('latlng', [None, None])[0],
        'longitude': country.get('latlng', [None, None])[1],
        'area': country.get('area'),
        'landlocked': None,  # Not available with fields limit
        
        # Population (available)
        'population': country.get('population'),
        
        # Languages and currencies (NOT available)
        'languages': None,
        'currencies': None,
        
        # Other (NOT available)
        'timezones': None,
        'borders': None,
        'flag_emoji': None,
        'flag_svg': country.get('flags', {}).get('svg'),  # Available
        
        # Metadata (NOT available)
        'independent': None,
        'un_member': None,
    }


if __name__ == '__main__':
    # Usage example
    print("=" * 80)
    print("🌍 REST Countries API Client - Demo")
    print("=" * 80)
    
    client = RestCountriesClient()
    
    # Example 1: Get information about Spain
    print("\n📍 Example 1: Get information about Spain")
    print("-" * 80)
    result = client.get_country_by_code('ESP')
    print(result)
    if result.ok:
        country = result.json_obj[0]
        data = extract_country_data(country)
        print(f"\n🏴 Country: {data['name_common']}")
        print(f"   Capital: {data['capital']}")
        print(f"   Region: {data['region']} - {data['subregion']}")
        print(f"   Population: {data['population']:,}")
        print(f"   Area: {data['area']:,.0f} km²")
    
    # Example 2: Countries in Europe
    print("\n\n📍 Example 2: Countries in Europe")
    print("-" * 80)
    result = client.get_countries_by_region('europe')
    print(result)
    if result.ok:
        countries = result.json_obj
        print(f"\n✅ Found {len(countries)} European countries")
        print("   First 5:")
        for country in countries[:5]:
            data = extract_country_data(country)
            print(f"   - {data['name_common']} ({data['code_iso2']})")
    
    print("\n" + "=" * 80)
    print("✅ Demo completed")
    print("=" * 80)
