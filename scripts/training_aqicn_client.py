#!/usr/bin/env python3
"""
Cliente HTTP para AQICN API (World Air Quality Index)
======================================================

API Pública: https://aqicn.org/api/
Documentación: https://aqicn.org/json-api/doc/

Este cliente demuestra:
- Manejo de requests HTTP con API key
- Lectura de variables de entorno
- Política de reintentos
- Logging estructurado
- Manejo de errores

Quota: 1,000 requests por segundo (gratuito)
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
    """Resultado de una llamada HTTP"""
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
    Cliente HTTP para AQICN API (World Air Quality Index)
    
    Política de reintentos:
    - Retry en: 429 (rate limit), 500, 502, 503, 504
    - Máximo 3 intentos
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
        # Leer token desde variable de entorno o archivo .env
        self.token = token or self._load_token_from_env()
        
        if not self.token:
            raise ValueError(
                "AQICN API token no encontrado. "
                "Provéelo como argumento, configura AQICN_API_TOKEN en variable de entorno, "
                "o añádelo en /opt/airflow/.env"
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
        """Intenta cargar el token desde variable de entorno o archivo .env"""
        # Primero intentar variable de entorno
        token = os.getenv('AQICN_API_TOKEN')
        if token:
            return token
        
        # Intentar leer desde .env file (ubicaciones comunes)
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
        
        # Como fallback para formación, usar token hardcodeado
        # (solo para propósitos educativos, no hacer esto en producción)
        return '6122a7fc9cb8cd3585680249e1bae8011318db8b'
    
    def _should_retry(self, status: int) -> bool:
        """Determina si debemos reintentar según el status code"""
        return status in (429, 500, 502, 503, 504)
    
    def _call(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> CallResult:
        """
        Ejecuta una llamada HTTP con política de reintentos
        
        Args:
            method: GET, POST, PUT, DELETE
            endpoint: /feed/here, /feed/{city}, etc.
            params: Query parameters adicionales (el token se añade automáticamente)
            
        Returns:
            CallResult con el resultado
        """
        url = f"{self.base_url}{endpoint}"
        
        # Añadir token a los parámetros
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
                
                # Error recuperable: reintentar
                if self._should_retry(response.status_code) and attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** (attempt - 1))
                    print(f"⚠️  Status {response.status_code}, reintentando en {delay}s... (intento {attempt}/{self.max_retries})")
                    time.sleep(delay)
                    continue
                
                # Error no recuperable
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
                    print(f"⚠️  Timeout, reintentando en {delay}s... (intento {attempt}/{self.max_retries})")
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
        
        # No debería llegar aquí, pero por seguridad
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
        """Intenta parsear JSON, si falla retorna None y el texto"""
        try:
            return resp.json(), resp.text
        except Exception:
            return None, resp.text
    
    # =========================================================================
    # MÉTODOS DE LA API
    # =========================================================================
    
    def get_current_location(self) -> CallResult:
        """
        GET /feed/here - Obtiene AQI de la ubicación actual (basado en IP)
        
        Returns:
            CallResult con datos de calidad del aire
        """
        return self._call('GET', '/feed/here/')
    
    def get_city_feed(self, city: str) -> CallResult:
        """
        GET /feed/{city} - Obtiene AQI de una ciudad específica
        
        Args:
            city: Nombre de la ciudad (e.g., 'beijing', 'shanghai', 'london')
        
        Returns:
            CallResult con datos de calidad del aire de la ciudad
        """
        return self._call('GET', f'/feed/{city}/')
    
    def get_station_by_id(self, station_id: int) -> CallResult:
        """
        GET /feed/@{id} - Obtiene AQI de una estación por ID
        
        Args:
            station_id: ID numérico de la estación
        
        Returns:
            CallResult con datos de la estación
        """
        return self._call('GET', f'/feed/@{station_id}/')
    
    def get_geo_feed(self, lat: float, lng: float) -> CallResult:
        """
        GET /feed/geo:{lat};{lng} - Obtiene AQI cercano a coordenadas
        
        Args:
            lat: Latitud
            lng: Longitud
        
        Returns:
            CallResult con datos de la estación más cercana
        """
        return self._call('GET', f'/feed/geo:{lat};{lng}/')
    
    def search_stations(self, keyword: str) -> CallResult:
        """
        GET /search - Busca estaciones por nombre/ciudad
        
        Args:
            keyword: Palabra clave de búsqueda
        
        Returns:
            CallResult con lista de estaciones encontradas
        """
        return self._call('GET', '/search/', params={'keyword': keyword})
    
    def get_map_stations(self, latlng: str) -> CallResult:
        """
        GET /map/bounds - Obtiene estaciones dentro de un mapa
        
        Args:
            latlng: Coordenadas en formato "lat1,lng1,lat2,lng2"
                    Ejemplo: "39.379436,116.091794,40.235643,116.784382"
        
        Returns:
            CallResult con estaciones dentro del área
        """
        return self._call('GET', '/map/bounds/', params={'latlng': latlng})


# =============================================================================
# FUNCIONES DE UTILIDAD
# =============================================================================

def extract_air_quality_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrae y normaliza datos de calidad del aire
    
    Esta función demuestra la fase de TRANSFORM en ETL:
    - Extraer campos anidados
    - Manejar valores nulos
    - Normalizar formatos
    
    Args:
        data: Objeto JSON 'data' de la respuesta AQICN
        
    Returns:
        Dict con datos normalizados
    """
    city = data.get('city', {})
    geo = city.get('geo', [None, None])
    iaqi = data.get('iaqi', {})
    time_data = data.get('time', {})
    
    return {
        # Identificadores
        'station_id': data.get('idx'),
        'city_name': city.get('name'),
        'city_url': city.get('url'),
        
        # Coordenadas
        'latitude': geo[0] if len(geo) > 0 else None,
        'longitude': geo[1] if len(geo) > 1 else None,
        
        # AQI principal
        'aqi': data.get('aqi'),
        'dominant_pollutant': data.get('dominentpol'),  # Nota: typo en la API
        
        # Contaminantes individuales (iaqi = Individual AQI)
        'pm25': iaqi.get('pm25', {}).get('v'),
        'pm10': iaqi.get('pm10', {}).get('v'),
        'o3': iaqi.get('o3', {}).get('v'),
        'no2': iaqi.get('no2', {}).get('v'),
        'so2': iaqi.get('so2', {}).get('v'),
        'co': iaqi.get('co', {}).get('v'),
        
        # Condiciones meteorológicas
        'temperature': iaqi.get('t', {}).get('v'),
        'humidity': iaqi.get('h', {}).get('v'),
        'pressure': iaqi.get('p', {}).get('v'),
        'wind_speed': iaqi.get('w', {}).get('v'),
        
        # Timestamp
        'measured_at': time_data.get('s'),  # Formato: "2026-02-17 12:00:00"
        'timezone': time_data.get('tz'),
        'timestamp_unix': time_data.get('v'),
    }


if __name__ == '__main__':
    # Ejemplo de uso
    print("=" * 80)
    print("🌍 AQICN API Client - Demo")
    print("=" * 80)
    
    # Nota: asegúrate de tener AQICN_API_TOKEN en tu .env
    client = AQICNClient()
    
    # Ejemplo 1: Ubicación actual (basado en IP)
    print("\n📍 Ejemplo 1: Ubicación actual (IP geolocalización)")
    print("-" * 80)
    result = client.get_current_location()
    print(result)
    if result.ok:
        data = result.json_obj.get('data', {})
        aq_data = extract_air_quality_data(data)
        print(f"\n🏙️  Ciudad: {aq_data['city_name']}")
        print(f"   AQI: {aq_data['aqi']} ({aq_data['dominant_pollutant']})")
        print(f"   PM2.5: {aq_data['pm25']}")
        print(f"   PM10: {aq_data['pm10']}")
        print(f"   Temperatura: {aq_data['temperature']}°C")
        print(f"   Timestamp: {aq_data['measured_at']}")
    
    # Ejemplo 2: Ciudad específica
    print("\n\n📍 Ejemplo 2: Beijing")
    print("-" * 80)
    result = client.get_city_feed('beijing')
    print(result)
    if result.ok:
        data = result.json_obj.get('data', {})
        aq_data = extract_air_quality_data(data)
        print(f"\n🏙️  Ciudad: {aq_data['city_name']}")
        print(f"   AQI: {aq_data['aqi']} ({aq_data['dominant_pollutant']})")
        print(f"   Coordenadas: ({aq_data['latitude']}, {aq_data['longitude']})")
    
    # Ejemplo 3: Buscar estaciones
    print("\n\n📍 Ejemplo 3: Buscar estaciones en London")
    print("-" * 80)
    result = client.search_stations('london')
    print(result)
    if result.ok:
        stations = result.json_obj.get('data', [])
        print(f"\n✅ Se encontraron {len(stations)} estaciones")
        print("   Primeras 5:")
        for station in stations[:5]:
            print(f"   - {station.get('station', {}).get('name')} (AQI: {station.get('aqi', 'N/A')})")
    
    print("\n" + "=" * 80)
    print("✅ Demo completada")
    print("=" * 80)
