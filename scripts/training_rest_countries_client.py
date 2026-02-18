#!/usr/bin/env python3
"""
Cliente HTTP para REST Countries API - Formación ETL
==================================================

API Pública: https://restcountries.com/
Sin autenticación, ideal para aprendizaje

Este cliente demuestra:
- Manejo de requests HTTP
- Política de reintentos
- Logging estructurado
- Manejo de errores
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


class RestCountriesClient:
    """
    Cliente HTTP para REST Countries API
    
    Política de reintentos:
    - Retry en: 429 (rate limit), 500, 502, 503, 504
    - Máximo 3 intentos
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
        """Determina si debemos reintentar según el status code"""
        return status in (429, 500, 502, 503, 504)
    
    def _call(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> CallResult:
        """
        Ejecuta una llamada HTTP con política de reintentos
        
        Args:
            method: GET, POST, PUT, DELETE
            endpoint: /all, /name/{name}, etc.
            params: Query parameters
            json_data: Body JSON (para POST/PUT)
            
        Returns:
            CallResult con el resultado
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
                
                # Si es exitoso, retornar
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
                
                # Si debemos reintentar
                if self._should_retry(response.status_code) and attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** (attempt - 1))
                    print(f"⚠️  Intento {attempt}/{self.max_retries} falló: {response.status_code}. Reintentando en {delay}s...")
                    time.sleep(delay)
                    continue
                
                # Error definitivo
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
                
                # Reintentar en caso de excepciones de red
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** (attempt - 1))
                    print(f"⚠️  Excepción en intento {attempt}/{self.max_retries}: {e}. Reintentando en {delay}s...")
                    time.sleep(delay)
                    continue
                
                # Excepción definitiva
                return CallResult(
                    ok=False,
                    method=method,
                    url=url,
                    status=0,
                    elapsed_ms=elapsed_ms,
                    json_obj=None,
                    text=str(e),
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
    
    def _safe_json(self, resp: requests.Response) -> Tuple[Optional[Any], str]:
        """Intenta parsear JSON, si falla retorna None y el texto"""
        try:
            return resp.json(), resp.text
        except Exception:
            return None, resp.text
    
    # =========================================================================
    # MÉTODOS DE LA API - Múltiples llamadas para obtener todos los campos
    # =========================================================================
    # La API REST Countries limita a 10 campos por request.
    # Para obtener todos los datos, hacemos múltiples llamadas con diferentes fields.
    
    def get_all_countries_basic(self) -> CallResult:
        """
        GET /all - Obtiene campos BÁSICOS de países (8 campos)
        
        Campos: cca2, cca3, name, capital, region, subregion, area, population
        
        Returns:
            CallResult con lista de países con campos básicos
        """
        fields = 'cca2,cca3,name,capital,region,subregion,area,population'
        return self._call('GET', '/all', params={'fields': fields})
    
    def get_all_countries_geo(self) -> CallResult:
        """
        GET /all - Obtiene campos GEOGRÁFICOS de países (5 campos)
        
        Campos: cca2, cca3, latlng, landlocked, borders
        
        Returns:
            CallResult con lista de países con campos geográficos
        """
        fields = 'cca2,cca3,latlng,landlocked,borders'
        return self._call('GET', '/all', params={'fields': fields})
    
    def get_all_countries_culture(self) -> CallResult:
        """
        GET /all - Obtiene campos CULTURALES/ECONÓMICOS de países (6 campos)
        
        Campos: cca2, cca3, languages, currencies, timezones, flags
        
        Returns:
            CallResult con lista de países con campos culturales
        """
        fields = 'cca2,cca3,languages,currencies,timezones,flags'
        return self._call('GET', '/all', params={'fields': fields})
    
    def get_all_countries_political(self) -> CallResult:
        """
        GET /all - Obtiene campos POLÍTICOS de países (5 campos)
        
        Campos: cca2, cca3, independent, unMember, ccn3
        
        Returns:
            CallResult con lista de países con campos políticos
        """
        fields = 'cca2,cca3,independent,unMember,ccn3'
        return self._call('GET', '/all', params={'fields': fields})
    
    def get_all_countries(self) -> CallResult:
        """
        GET /all - Obtiene campos básicos de países (DEPRECATED - usar métodos específicos)
        
        NOTA: Este método usa solo 10 campos por compatibilidad.
        Para obtener todos los datos, usar:
        - get_all_countries_basic()
        - get_all_countries_geo()
        - get_all_countries_culture()
        - get_all_countries_political()
        
        Returns:
            CallResult con lista de países con campos básicos
        """
        fields = 'cca2,cca3,name,capital,region,subregion,area,population'
        return self._call('GET', '/all', params={'fields': fields})
    
    def get_country_by_name(self, name: str) -> CallResult:
        """
        GET /name/{name} - Busca países por nombre
        
        Args:
            name: Nombre del país (puede ser parcial)
            
        Returns:
            CallResult con países que coinciden
        """
        fields = 'cca2,cca3,name,capital,region,subregion,latlng,area,population,flags'
        return self._call('GET', f'/name/{name}', params={'fields': fields})
    
    def get_countries_by_region(self, region: str) -> CallResult:
        """
        GET /region/{region} - Obtiene países por región
        
        Args:
            region: africa, americas, asia, europe, oceania
            
        Returns:
            CallResult con países de la región
        """
        fields = 'cca2,cca3,name,capital,region,subregion,latlng,area,population,flags'
        return self._call('GET', f'/region/{region}', params={'fields': fields})
    
    def get_countries_by_subregion(self, subregion: str) -> CallResult:
        """
        GET /subregion/{subregion} - Obtiene países por subregión
        
        Args:
            subregion: Southern Europe, South America, etc.
            
        Returns:
            CallResult con países de la subregión
        """
        fields = 'cca2,cca3,name,capital,region,subregion,latlng,area,population,flags'
        return self._call('GET', f'/subregion/{subregion}', params={'fields': fields})
    
    def get_country_by_code(self, code: str) -> CallResult:
        """
        GET /alpha/{code} - Obtiene país por código ISO
        
        Args:
            code: Código ISO 2 o 3 letras (ES, ESP, USA, US, etc.)
            
        Returns:
            CallResult con datos del país
        """
        fields = 'cca2,cca3,name,capital,region,subregion,latlng,area,population,flags'
        return self._call('GET', f'/alpha/{code}', params={'fields': fields})


# =============================================================================
# FUNCIONES DE UTILIDAD
# =============================================================================

def extract_country_data(country: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrae y normaliza datos relevantes de un país
    
    Esta función demuestra la fase de TRANSFORM en ETL:
    - Extraer campos anidados
    - Manejar valores nulos
    - Normalizar formatos
    
    NOTA: La API REST Countries limita a 10 campos máximo.
    Campos disponibles: cca2, cca3, name, capital, region, subregion, latlng, area, population, flags
    Campos NO disponibles (NULL): ccn3, landlocked, languages, currencies, timezones, borders, independent, unMember
    
    Args:
        country: Objeto JSON del país de la API
        
    Returns:
        Dict con datos normalizados
    """
    return {
        # Identificadores (disponibles)
        'code_iso2': country.get('cca2'),
        'code_iso3': country.get('cca3'),
        'code_numeric': None,  # No disponible con fields limit
        
        # Nombres (disponibles)
        'name_common': country.get('name', {}).get('common'),
        'name_official': country.get('name', {}).get('official'),
        'name_native': json.dumps(country.get('name', {}).get('nativeName', {})),
        
        # Geografía (disponibles)
        'capital': json.dumps(country.get('capital', [])),
        'region': country.get('region'),
        'subregion': country.get('subregion'),
        'latitude': country.get('latlng', [None, None])[0],
        'longitude': country.get('latlng', [None, None])[1],
        'area': country.get('area'),
        'landlocked': None,  # No disponible con fields limit
        
        # Población (disponible)
        'population': country.get('population'),
        
        # Idiomas y monedas (NO disponibles)
        'languages': None,
        'currencies': None,
        
        # Otros (NO disponibles)
        'timezones': None,
        'borders': None,
        'flag_emoji': None,
        'flag_svg': country.get('flags', {}).get('svg'),  # Disponible
        
        # Metadata (NO disponibles)
        'independent': None,
        'un_member': None,
    }


if __name__ == '__main__':
    # Ejemplo de uso
    print("=" * 80)
    print("🌍 REST Countries API Client - Demo")
    print("=" * 80)
    
    client = RestCountriesClient()
    
    # Ejemplo 1: Obtener información de España
    print("\n📍 Ejemplo 1: Obtener información de España")
    print("-" * 80)
    result = client.get_country_by_code('ESP')
    print(result)
    if result.ok:
        country = result.json_obj[0]
        data = extract_country_data(country)
        print(f"\n🏴 País: {data['name_common']}")
        print(f"   Capital: {data['capital']}")
        print(f"   Región: {data['region']} - {data['subregion']}")
        print(f"   Población: {data['population']:,}")
        print(f"   Área: {data['area']:,.0f} km²")
    
    # Ejemplo 2: Países de Europa
    print("\n\n📍 Ejemplo 2: Países de Europa")
    print("-" * 80)
    result = client.get_countries_by_region('europe')
    print(result)
    if result.ok:
        countries = result.json_obj
        print(f"\n✅ Se encontraron {len(countries)} países europeos")
        print("   Primeros 5:")
        for country in countries[:5]:
            data = extract_country_data(country)
            print(f"   - {data['name_common']} ({data['code_iso2']})")
    
    print("\n" + "=" * 80)
    print("✅ Demo completada")
    print("=" * 80)
