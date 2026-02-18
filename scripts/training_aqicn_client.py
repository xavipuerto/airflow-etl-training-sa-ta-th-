#!/usr/bin/env python3
"""
HTTP Client for AQICN API (World Air Quality Index)
======================================================

Public API: https://aqicn.org/api/
Documentation: https://aqicn.org/json-api/doc/

This client demonstrates:
- HTTP requests handling with API key
- Environment variables reading
- Retry policy
- Structured logging
- Error handling

Quota: 1,000 requests per second (free)
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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


class AQICNClient:
    """
    HTTP Client for AQICN API (World Air Quality Index)
    
    Retry policy:
    - Retry on: 429 (rate limit), 500, 502, 503, 504
    - Maximum 3 attempts
    - Backoff: 2s, 4s, 8s
    """
    
    def __init__(
        self,
        token: Optional[str] = None,
        base_url: str = "https://api.waqi.info",
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: int = 2,
    ):
        # Read token from environment variable or .env file
        self.token = token or self._load_token_from_env()
        
        if not self.token:
            raise ValueError(
                "AQICN API token not found. "
                "Provide it as argument, set AQICN_API_TOKEN environment variable, "
                "or add it to /opt/airflow/.env"
            )
        
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Training-ETL-Client/1.0',
            'Accept': 'application/json',
        })
    
    def _load_token_from_env(self) -> Optional[str]:
        """Try to load token from environment variable or .env file"""
        # First try environment variable
        token = os.getenv('AQICN_API_TOKEN')
        if token:
            return token
        
        # Try reading from .env file (common locations)
        env_paths = [
            '/opt/airflow/.env',
            '/srv/nfs/airflow/.env',
            '.env',
            '../.env',
        ]
        
        for env_path in env_paths:
            if os.path.exists(env_path):
                try:
                    with open(env_path, 'r') as f:
                        for line in f:
                            line = line.strip()
                            if line.startswith('AQICN_API_TOKEN='):
                                return line.split('=', 1)[1].strip()
                except Exception:
                    continue
        
        # Fallback for training, use hardcoded token
        # (only for educational purposes, don't do this in production)
        return '6122a7fc9cb8cd3585680249e1bae8011318db8b'
    
    def _should_retry(self, status: int) -> bool:
        """Determines if we should retry based on status code"""
        return status in (429, 500, 502, 503, 504)
    
    def _call(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> CallResult:
        """
        Execute an HTTP call with retry policy
        
        Args:
            method: GET, POST, PUT, DELETE
            endpoint: /feed/here, /feed/{city}, etc.
            params: Additional query parameters (token is added automatically)
            
        Returns:
            CallResult with the result
        """
        url = f"{self.base_url}{endpoint}"
        
        # Add token to parameters
        if params is None:
            params = {}
        params['token'] = self.token
        
        for attempt in range(1, self.max_retries + 1):
            try:
                start_time = now_ms()
                
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    timeout=self.timeout,
                )
                
                elapsed_ms = now_ms() - start_time
                json_obj, text = self._safe_json(response)
                
                # Success
                if response.status_code < 400:
                    return CallResult(
                        ok=True,
                        method=method,
                        url=url,
                        status=response.status_code,
                        elapsed_ms=elapsed_ms,
                        json_obj=json_obj,
                        text=text,
                    )
                
                # Recoverable error: retry
                if self._should_retry(response.status_code) and attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** (attempt - 1))
                    print(f"⚠️  Status {response.status_code}, retrying in {delay}s... (attempt {attempt}/{self.max_retries})")
                    time.sleep(delay)
                    continue
                
                # Unrecoverable error
                return CallResult(
                    ok=False,
                    method=method,
                    url=url,
                    status=response.status_code,
                    elapsed_ms=elapsed_ms,
                    json_obj=json_obj,
                    text=text,
                )
                
            except requests.exceptions.Timeout:
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** (attempt - 1))
                    print(f"⚠️  Timeout, retrying in {delay}s... (attempt {attempt}/{self.max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    return CallResult(
                        ok=False,
                        method=method,
                        url=url,
                        status=0,
                        elapsed_ms=0,
                        json_obj=None,
                        text="Request timeout",
                    )
            
            except Exception as e:
                return CallResult(
                    ok=False,
                    method=method,
                    url=url,
                    status=0,
                    elapsed_ms=0,
                    json_obj=None,
                    text=f"Error: {str(e)}",
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
    
    def _safe_json(self, resp: requests.Response) -> tuple[Optional[Any], str]:
        """Try to parse JSON, if it fails return None and text"""
        try:
            return resp.json(), resp.text
        except Exception:
            return None, resp.text
    
    # =========================================================================
    # API METHODS
    # =========================================================================
    
    def get_current_location(self) -> CallResult:
        """
        GET /feed/here - Get AQI from current location (IP-based)
        
        Returns:
            CallResult with air quality data
        """
        return self._call('GET', '/feed/here/')
    
    def get_city_feed(self, city: str) -> CallResult:
        """
        GET /feed/{city} - Get AQI from a specific city
        
        Args:
            city: City name (e.g., 'beijing', 'shanghai', 'london')
        
        Returns:
            CallResult with air quality data from the city
        """
        return self._call('GET', f'/feed/{city}/')
    
    def get_station_by_id(self, station_id: int) -> CallResult:
        """
        GET /feed/@{id} - Get AQI from a station by ID
        
        Args:
            station_id: Numeric station ID
        
        Returns:
            CallResult with station data
        """
        return self._call('GET', f'/feed/@{station_id}/')
    
    def get_geo_feed(self, lat: float, lng: float) -> CallResult:
        """
        GET /feed/geo:{lat};{lng} - Get AQI near coordinates
        
        Args:
            lat: Latitude
            lng: Longitude
        
        Returns:
            CallResult with nearest station data
        """
        return self._call('GET', f'/feed/geo:{lat};{lng}/')
    
    def search_stations(self, keyword: str) -> CallResult:
        """
        GET /search - Search stations by name/city
        
        Args:
            keyword: Search keyword
        
        Returns:
            CallResult with list of found stations
        """
        return self._call('GET', '/search/', params={'keyword': keyword})
    
    def get_map_stations(self, latlng: str) -> CallResult:
        """
        GET /map/bounds - Get stations within a map area
        
        Args:
            latlng: Coordinates in format "lat1,lng1,lat2,lng2"
                    Example: "39.379436,116.091794,40.235643,116.784382"
        
        Returns:
            CallResult with stations within the area
        """
        return self._call('GET', '/map/bounds/', params={'latlng': latlng})


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def extract_air_quality_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract and normalize air quality data
    
    This function demonstrates the TRANSFORM phase in ETL:
    - Extract nested fields
    - Handle null values
    - Normalize formats
    
    Args:
        data: 'data' JSON object from AQICN response
        
    Returns:
        Dict with normalized data
    """
    city = data.get('city', {})
    geo = city.get('geo', [None, None])
    iaqi = data.get('iaqi', {})
    time_data = data.get('time', {})
    
    return {
        # Identifiers
        'station_id': data.get('idx'),
        'city_name': city.get('name'),
        'city_url': city.get('url'),
        
        # Coordinates
        'latitude': geo[0] if len(geo) > 0 else None,
        'longitude': geo[1] if len(geo) > 1 else None,
        
        # Main AQI
        'aqi': data.get('aqi'),
        'dominant_pollutant': data.get('dominentpol'),  # Note: typo in API
        
        # Individual pollutants (iaqi = Individual AQI)
        'pm25': iaqi.get('pm25', {}).get('v'),
        'pm10': iaqi.get('pm10', {}).get('v'),
        'o3': iaqi.get('o3', {}).get('v'),
        'no2': iaqi.get('no2', {}).get('v'),
        'so2': iaqi.get('so2', {}).get('v'),
        'co': iaqi.get('co', {}).get('v'),
        
        # Weather conditions
        'temperature': iaqi.get('t', {}).get('v'),
        'humidity': iaqi.get('h', {}).get('v'),
        'pressure': iaqi.get('p', {}).get('v'),
        'wind_speed': iaqi.get('w', {}).get('v'),
        
        # Timestamp
        'measured_at': time_data.get('s'),  # Format: "2026-02-17 12:00:00"
        'timezone': time_data.get('tz'),
        'timestamp_unix': time_data.get('v'),
    }


if __name__ == '__main__':
    # Usage example
    print("=" * 80)
    print("🌍 AQICN API Client - Demo")
    print("=" * 80)
    
    # Note: make sure you have AQICN_API_TOKEN in your .env
    client = AQICNClient()
    
    # Example 1: Current location (IP-based)
    print("\n📍 Example 1: Current location (IP geolocation)")
    print("-" * 80)
    result = client.get_current_location()
    print(result)
    if result.ok:
        data = result.json_obj.get('data', {})
        aq_data = extract_air_quality_data(data)
        print(f"\n🏙️  City: {aq_data['city_name']}")
        print(f"   AQI: {aq_data['aqi']} ({aq_data['dominant_pollutant']})")
        print(f"   PM2.5: {aq_data['pm25']}")
        print(f"   PM10: {aq_data['pm10']}")
        print(f"   Temperature: {aq_data['temperature']}°C")
        print(f"   Timestamp: {aq_data['measured_at']}")
    
    # Example 2: Specific city
    print("\n\n📍 Example 2: Beijing")
    print("-" * 80)
    result = client.get_city_feed('beijing')
    print(result)
    if result.ok:
        data = result.json_obj.get('data', {})
        aq_data = extract_air_quality_data(data)
        print(f"\n🏙️  City: {aq_data['city_name']}")
        print(f"   AQI: {aq_data['aqi']} ({aq_data['dominant_pollutant']})")
        print(f"   Coordinates: ({aq_data['latitude']}, {aq_data['longitude']})")
    
    # Example 3: Search stations
    print("\n\n📍 Example 3: Search stations in London")
    print("-" * 80)
    result = client.search_stations('london')
    print(result)
    if result.ok:
        stations = result.json_obj.get('data', [])
        print(f"\n✅ Found {len(stations)} stations")
        print("   First 5:")
        for station in stations[:5]:
            print(f"   - {station.get('station', {}).get('name')} (AQI: {station.get('aqi', 'N/A')})")
    
    print("\n" + "=" * 80)
    print("✅ Demo completed")
    print("=" * 80)
