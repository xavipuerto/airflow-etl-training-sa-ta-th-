#!/usr/bin/env python3
"""
HTTP Client for Open-Meteo API - ETL Training
===============================================

Public API: https://open-meteo.com/
No authentication, ideal for learning

This client demonstrates:
- HTTP requests handling
- Retry policy
- Structured logging
- Time series (weather)
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

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


class OpenMeteoClient:
    """
    HTTP Client for Open-Meteo API
    
    Retry policy:
    - Retry on: 429 (rate limit), 500, 502, 503, 504
    - Maximum 3 attempts
    - Backoff: 2s, 4s, 8s
    """
    
    def __init__(
        self,
        base_url: str = "https://api.open-meteo.com/v1",
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
    ) -> CallResult:
        """
        Execute an HTTP call with retry policy
        
        Args:
            method: GET
            endpoint: /forecast, /historical, etc.
            params: Query parameters
            
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
    # API METHODS
    # =========================================================================
    
    def get_current_weather(self, latitude: float, longitude: float) -> CallResult:
        """
        GET /forecast - Get current weather for a location
        
        Args:
            latitude: Latitude
            longitude: Longitude
            
        Returns:
            CallResult with current weather data
        """
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'current': 'temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code',
            'timezone': 'UTC',
        }
        return self._call('GET', '/forecast', params=params)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def extract_weather_data(weather_response: Dict[str, Any], country_code: str, city: str) -> Dict[str, Any]:
    """
    Extract and normalize weather data
    
    Args:
        weather_response: JSON object from API
        country_code: ISO2 country code
        city: City name
        
    Returns:
        Dict with normalized data
    """
    current = weather_response.get('current', {})
    
    return {
        'measured_at': current.get('time'),  # ISO8601 timestamp
        'country': country_code,
        'city': city,
        'latitude': weather_response.get('latitude'),
        'longitude': weather_response.get('longitude'),
        'temperature': current.get('temperature_2m'),
        'humidity': current.get('relative_humidity_2m'),
        'precipitation': current.get('precipitation'),
        'wind_speed': current.get('wind_speed_10m'),
        'weather_code': current.get('weather_code'),
    }


if __name__ == '__main__':
    # Usage example
    print("=" * 80)
    print("🌤️  Open-Meteo API Client - Demo")
    print("=" * 80)
    
    client = OpenMeteoClient()
    
    # Example 1: Weather in Madrid
    print("\n📍 Example 1: Current weather in Madrid, Spain")
    print("-" * 80)
    result = client.get_current_weather(latitude=40.4168, longitude=-3.7038)
    print(result)
    if result.ok:
        data = extract_weather_data(result.json_obj, 'ES', 'Madrid')
        print(f"\n🌤️  Weather in {data['city']}:")
        print(f"   Time: {data['measured_at']}")
        print(f"   Temperature: {data['temperature']}°C")
        print(f"   Humidity: {data['humidity']}%")
        print(f"   Precipitation: {data['precipitation']} mm")
        print(f"   Wind: {data['wind_speed']} km/h")
    
    # Example 2: Weather in London
    print("\n\n📍 Example 2: Current weather in London, United Kingdom")
    print("-" * 80)
    result = client.get_current_weather(latitude=51.5074, longitude=-0.1278)
    print(result)
    if result.ok:
        data = extract_weather_data(result.json_obj, 'GB', 'London')
        print(f"\n🌤️  Weather in {data['city']}:")
        print(f"   Temperature: {data['temperature']}°C")
        print(f"   Humidity: {data['humidity']}%")
    
    print("\n" + "=" * 80)
    print("✅ Demo completed")
    print("=" * 80)
