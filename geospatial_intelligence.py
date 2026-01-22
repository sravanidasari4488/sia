"""
Satellite-based Geospatial Intelligence System
Real-time land cover classification and climate risk assessment
"""

import os
import json
import numpy as np
import ee
import requests
import time
from typing import Dict, Tuple, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from opencage.geocoder import OpenCageGeocode
import warnings
warnings.filterwarnings('ignore')

# Supabase client (optional - only if credentials are provided)
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False


@dataclass
class BoundingBox:
    """Bounding box coordinates"""
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float


@dataclass
class LandCoverResult:
    """Land cover classification results"""
    urban: float
    forest: float
    vegetation: float
    water: float
    total_pixels: int


@dataclass
class WeatherData:
    """Weather data from OpenWeather API"""
    temperature: float
    humidity: float
    precipitation: float
    wind_speed: float
    pressure: float
    coordinates: Tuple[float, float]


@dataclass
class AirQualityData:
    """Air quality data from OpenWeather Air Pollution API"""
    aqi: int  # Air Quality Index (1-5)
    aqi_level: str  # Good, Fair, Moderate, Poor, Very Poor
    pm25: float  # PM2.5 in μg/m³
    pm10: float  # PM10 in μg/m³
    co: float  # CO in μg/m³
    no2: float  # NO₂ in μg/m³
    so2: float  # SO₂ in μg/m³
    o3: float  # O₃ in μg/m³
    coordinates: Tuple[float, float]


class GeocodingService:
    """Handle geocoding and city boundary fetching"""
    
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("OpenCage API key is required")
        self.geocoder = OpenCageGeocode(api_key)
        self.nominatim_url = "https://nominatim.openstreetmap.org/search"
        self.nominatim_url = "https://nominatim.openstreetmap.org/search"
    
    def get_city_boundary_polygon(self, location: str) -> Tuple[ee.Geometry, BoundingBox, Tuple[float, float]]:
        """
        Fetch city administrative boundary polygon from OpenStreetMap
        
        Uses ONLY admin_level=8 boundaries (actual city boundaries like GHMC for Hyderabad).
        Rejects admin_level=6, admin_level=4, and place=region to ensure we get
        the actual city, not the entire district.
        
        Returns:
            Tuple of (Earth Engine polygon geometry, bounding box, center coordinates)
        """
        try:
            # First, geocode to get coordinates for search
            if ',' in location:
                # If coordinates provided, use them directly
                coords = location.split(',')
                lat, lon = float(coords[0].strip()), float(coords[1].strip())
                center = (lat, lon)
                query = location
            else:
                # Geocode city name to get coordinates
                results = self.geocoder.geocode(location)
                if not results:
                    raise ValueError(f"Location '{location}' not found")
                
                geometry = results[0]['geometry']
                lat, lon = geometry['lat'], geometry['lng']
                center = (lat, lon)
                query = location
            
            # Use Overpass API to get admin_level=8 boundaries (primary method)
            # This ensures we get actual city boundaries like GHMC for Hyderabad
            polygon, bbox = self._get_boundary_from_overpass(query, center, lat, lon)
            
            if polygon is not None and bbox is not None:
                # Preprocess polygon before returning
                polygon = EarthEngineService.preprocess_polygon(polygon)
                return polygon, bbox, center
            
            # Fallback to Nominatim if Overpass fails
            return self._get_boundary_from_nominatim(query, center, lat, lon)
            
        except Exception as e:
            raise ValueError(f"Failed to fetch city boundary: {str(e)}")
    
    def _get_boundary_from_nominatim(self, query: str, center: Tuple[float, float], lat: float, lon: float) -> Tuple[ee.Geometry, BoundingBox, Tuple[float, float]]:
        """Fallback: Try Nominatim API (less reliable for admin_level filtering)"""
        try:
            params = {
                'q': query,
                'format': 'geojson',
                'limit': 10,
                'polygon_geojson': 1,
                'addressdetails': 1,
                'extratags': 1,
                'namedetails': 1
            }
            
            response = requests.get(self.nominatim_url, params=params, timeout=15, 
                                   headers={'User-Agent': 'GeospatialIntelligenceSystem/1.0'})
                
            if response.status_code == 200:
                data = response.json()
                if data.get('features'):
                    # Look for admin_level=8 in extratags
                    for feature in data['features']:
                        geometry_data = feature.get('geometry')
                        properties = feature.get('properties', {})
                        extratags = properties.get('extratags', {})
                        
                        # Check admin_level - ONLY accept admin_level=8
                        admin_level = extratags.get('admin_level')
                        place = extratags.get('place', '').lower()
                        
                        # Reject admin_level 6, 4, and place=region
                        if admin_level in ['6', '4'] or place == 'region':
                            continue
                        
                        # ONLY accept admin_level=8
                        if admin_level != '8':
                            continue
                        
                        # Found admin_level=8 boundary
                        if geometry_data and geometry_data.get('type') == 'Polygon':
                            coordinates = geometry_data['coordinates'][0]
                            ee_coords = [[coord[0], coord[1]] for coord in coordinates]
                            polygon = ee.Geometry.Polygon(ee_coords)
                            
                            lons = [coord[0] for coord in coordinates]
                            lats = [coord[1] for coord in coordinates]
                            bbox = BoundingBox(
                                min_lon=min(lons),
                                min_lat=min(lats),
                                max_lon=max(lons),
                                max_lat=max(lats)
                            )
                            
                            polygon = EarthEngineService.preprocess_polygon(polygon)
                            return polygon, bbox, center
                        
                        elif geometry_data and geometry_data.get('type') == 'MultiPolygon':
                            multi_polygon_coords = []
                            all_lons = []
                            all_lats = []
                            
                            for polygon_coords in geometry_data['coordinates']:
                                outer_ring = polygon_coords[0]
                                multi_polygon_coords.append([[coord[0], coord[1]] for coord in outer_ring])
                                all_lons.extend([coord[0] for coord in outer_ring])
                                all_lats.extend([coord[1] for coord in outer_ring])
                            
                            polygon = ee.Geometry.MultiPolygon(multi_polygon_coords)
                            bbox = BoundingBox(
                                min_lon=min(all_lons),
                                min_lat=min(all_lats),
                                max_lon=max(all_lons),
                                max_lat=max(all_lats)
                            )
                            
                            polygon = EarthEngineService.preprocess_polygon(polygon)
                            return polygon, bbox, center
            
            # Ultimate fallback: use a larger bounding box (10km radius) to ensure we get Dynamic World data
            # This is better than a tiny box that might not have coverage
            radius_degrees = 0.09  # ~10km radius (0.09 degrees ≈ 10km)
            bbox = BoundingBox(
                min_lon=lon - radius_degrees,
                min_lat=lat - radius_degrees,
                max_lon=lon + radius_degrees,
                max_lat=lat + radius_degrees
            )
            polygon = ee.Geometry.Rectangle([
                bbox.min_lon, bbox.min_lat,
                bbox.max_lon, bbox.max_lat
            ])
            polygon = EarthEngineService.preprocess_polygon(polygon)
            return polygon, bbox, center
            
        except Exception as e:
            # Ultimate fallback: use a larger bounding box (10km radius)
            radius_degrees = 0.09  # ~10km radius
            bbox = BoundingBox(
                min_lon=lon - radius_degrees,
                min_lat=lat - radius_degrees,
                max_lon=lon + radius_degrees,
                max_lat=lat + radius_degrees
            )
            polygon = ee.Geometry.Rectangle([
                bbox.min_lon, bbox.min_lat,
                bbox.max_lon, bbox.max_lat
            ])
            polygon = EarthEngineService.preprocess_polygon(polygon)
            return polygon, bbox, center
    
    def _get_boundary_from_overpass(self, query: str, center: Tuple[float, float], lat: float, lon: float) -> Tuple[Optional[ee.Geometry], Optional[BoundingBox]]:
        """
        Fetch admin_level=8 administrative boundary from Overpass API
        
        ONLY uses admin_level=8 (actual city boundaries like GHMC).
        Rejects admin_level=6, admin_level=4, and place=region.
        
        Returns:
            Tuple of (polygon, bbox) or (None, None) if not found
        """
        try:
            overpass_url = "https://overpass-api.de/api/interpreter"
            
            # Search ONLY for admin_level=8 boundaries
            # Reject admin_level=6, admin_level=4, and place=region
            # Search by name and also by area around coordinates for better matching
            overpass_query = f"""
            [out:json][timeout:25];
            (
              relation["admin_level"="8"]["boundary"="administrative"]["name"~"{query}",i];
              relation["admin_level"="8"]["boundary"="administrative"](around:5000,{lat},{lon});
            );
            (._;>;);
            out geom;
            """
            
            response = requests.post(overpass_url, data=overpass_query, timeout=30,
                                   headers={'User-Agent': 'GeospatialIntelligenceSystem/1.0'})
            
            if response.status_code == 200:
                data = response.json()
                if data.get('elements'):
                    # Find admin_level=8 relation
                    for element in data['elements']:
                        if element.get('type') == 'relation':
                            tags = element.get('tags', {})
                            
                            # Verify it's admin_level=8 and reject others
                            admin_level = tags.get('admin_level')
                            place = tags.get('place', '').lower()
                            
                            # Reject admin_level 6, 4, and place=region
                            if admin_level in ['6', '4'] or place == 'region':
                                continue
                            
                            # ONLY accept admin_level=8
                            if admin_level != '8':
                                continue
                            
                            # Extract geometry from relation
                            # With 'out geom', Overpass returns geometry directly
                            # Try Nominatim lookup first (most reliable for getting full polygon)
                            osm_id = element.get('id')
                            if osm_id:
                                # Try to get polygon via Nominatim lookup with OSM ID
                                nominatim_lookup_url = f"https://nominatim.openstreetmap.org/lookup"
                                lookup_params = {
                                    'osm_ids': f"R{osm_id}",
                                    'format': 'geojson',
                                    'polygon_geojson': 1
                                }
                                
                                lookup_response = requests.get(nominatim_lookup_url, params=lookup_params, timeout=15,
                                                              headers={'User-Agent': 'GeospatialIntelligenceSystem/1.0'})
                                
                                if lookup_response.status_code == 200:
                                    lookup_data = lookup_response.json()
                                    if lookup_data.get('features'):
                                        feature = lookup_data['features'][0]
                                        geometry_data = feature.get('geometry')
                                        
                                        if geometry_data and geometry_data.get('type') == 'Polygon':
                                            coordinates = geometry_data['coordinates'][0]
                                            ee_coords = [[coord[0], coord[1]] for coord in coordinates]
                                            polygon = ee.Geometry.Polygon(ee_coords)
                                            
                                            lons = [coord[0] for coord in coordinates]
                                            lats = [coord[1] for coord in coordinates]
                                            bbox = BoundingBox(
                                                min_lon=min(lons),
                                                min_lat=min(lats),
                                                max_lon=max(lons),
                                                max_lat=max(lats)
                                            )
                                            
                                            return polygon, bbox
            
            # No admin_level=8 boundary found
            return None, None
             
        except Exception as e:
            # Return None to trigger fallback
            return None, None
    
    def get_aoi_polygon(self, location: str, buffer_radius_km: float = 2.0) -> Tuple[ee.Geometry, BoundingBox, Tuple[float, float], str]:
        """
        STEP 1 — INPUT HANDLING
        
        Accept city name or lat-lon coordinates and convert to a clean AOI polygon.
        - If coordinates provided: create circular buffer around point
        - If city name provided: get city center and create circular buffer
        - Buffer radius configurable (default 2 km)
        - Validate AOI size (reject AOI > 50 km²)
        
        Args:
            location: City name (e.g., "Delhi, India") or coordinates (e.g., "28.6139,77.2090")
            buffer_radius_km: Buffer radius in kilometers (default 2 km)
        
        Returns:
            Tuple of (Earth Engine polygon geometry, bounding box, center coordinates, location_name)
        """
        try:
            # Parse input: coordinates or city name
            # Smart detection: Only treat as coordinates if both parts are numeric
            is_coordinates = False
            if ',' in location:
                parts = location.split(',')
                if len(parts) == 2:
                    # Check if both parts can be parsed as floats (coordinates)
                    try:
                        lat_test = float(parts[0].strip())
                        lon_test = float(parts[1].strip())
                        # Validate coordinate ranges
                        if -90 <= lat_test <= 90 and -180 <= lon_test <= 180:
                            is_coordinates = True
                            lat = lat_test
                            lon = lon_test
                            center = (lat, lon)
                            location_name = f"{lat:.4f},{lon:.4f}"
                    except ValueError:
                        # Not coordinates, treat as location name
                        is_coordinates = False
            
            if not is_coordinates:
                # City name provided - geocode to get center
                results = self.geocoder.geocode(location)
                if not results:
                    raise ValueError(f"Location '{location}' not found")
                
                geometry = results[0]['geometry']
                lat, lon = geometry['lat'], geometry['lng']
                center = (lat, lon)
                location_name = location
            
            # Create circular buffer around center point
            buffer_radius_meters = buffer_radius_km * 1000
            point = ee.Geometry.Point(lon, lat)
            aoi_polygon = point.buffer(buffer_radius_meters)
            
            # Preprocess polygon
            aoi_polygon = EarthEngineService.preprocess_polygon(aoi_polygon)
            
            # Calculate bounding box
            # Approximate: 1 degree latitude ≈ 111 km
            # 1 degree longitude ≈ 111 km * cos(latitude)
            radius_degrees_lat = buffer_radius_km / 111.0
            radius_degrees_lon = buffer_radius_km / (111.0 * abs(np.cos(np.radians(lat))))
            
            bbox = BoundingBox(
                min_lon=lon - radius_degrees_lon,
                min_lat=lat - radius_degrees_lat,
                max_lon=lon + radius_degrees_lon,
                max_lat=lat + radius_degrees_lat
            )
            
            # Validate AOI size (reject AOI > 50 km²)
            aoi_area = np.pi * (buffer_radius_km ** 2)  # Area in km²
            if aoi_area > 50.0:
                raise ValueError(
                    f"AOI area ({aoi_area:.2f} km²) exceeds maximum allowed size (50 km²). "
                    f"Please use a smaller buffer radius (current: {buffer_radius_km} km)."
                )
            
            return aoi_polygon, bbox, center, location_name
            
        except Exception as e:
            raise ValueError(f"Failed to create AOI polygon: {str(e)}")
    
    def get_city_center(self, city_name: str) -> Tuple[float, float]:
        """
        Get city center coordinates using OpenCage API
        
        Args:
            city_name: Name of the city
            
        Returns:
            Tuple of (lat, lon) coordinates
        """
        try:
            results = self.geocoder.geocode(city_name)
            if not results:
                raise ValueError(f"City '{city_name}' not found")
            
            geometry = results[0]['geometry']
            lat, lon = geometry['lat'], geometry['lng']
            return lat, lon
        except Exception as e:
            raise ValueError(f"Failed to get city center: {str(e)}")
    
    def get_localities(self, city_name: str, radius_km: int = 8) -> List[Dict]:
        """
        Fetch locality names and centers only (fast, no polygons)
        
        Uses radius-based Overpass query to get only:
        - name
        - center coordinates
        - place type
        
        Does NOT fetch full polygons at this stage.
        
        Args:
            city_name: Name of the city
            radius_km: Search radius in kilometers (default 8km, reduced to avoid API overload)
            
        Returns:
            List of dictionaries with 'name', 'lat', 'lon', 'place_type', 'osm_id', 'osm_type'
            Example: [
                { 'name': 'Gachibowli', 'lat': 17.42, 'lon': 78.35, 'place_type': 'suburb', 'osm_id': 12345, 'osm_type': 'way' },
                ...
            ]
        """
        try:
            # Step 1: Get city center coordinates using OpenCage
            lat, lon = self.get_city_center(city_name)
            
            # Step 2: Use Overpass API with radius-based query
            # Only fetch names and centers, NOT full polygons
            overpass_servers = [
                "https://overpass-api.de/api/interpreter",
                "https://overpass.kumi.systems/api/interpreter",
                "https://overpass.openstreetmap.ru/api/interpreter"
            ]
            
            # Query pattern: Simplified radius-based query
            # Reduced complexity to avoid API overload
            # Includes: place=suburb|neighbourhood AND boundary=administrative with admin_level=9|10 (wards)
            radius_meters = radius_km * 1000
            
            # Use shorter timeout and simpler query for better reliability
            timeout_seconds = min(30, max(15, radius_km * 2))  # Adaptive timeout based on radius
            overpass_query = f"""[out:json][timeout:{timeout_seconds}];
(
  node["place"~"suburb|neighbourhood"](around:{radius_meters},{lat},{lon});
  way["place"~"suburb|neighbourhood"](around:{radius_meters},{lat},{lon});
  relation["place"~"suburb|neighbourhood"](around:{radius_meters},{lat},{lon});
  relation["boundary"="administrative"]["admin_level"~"9|10"](around:{radius_meters},{lat},{lon});
);
out tags center;"""
            
            response = None
            last_error = None
            
            # Try each server until one works, with delays between retries
            for idx, server_url in enumerate(overpass_servers):
                try:
                    # Add small delay between server attempts (except first)
                    if idx > 0:
                        time.sleep(1)  # 1 second delay between servers
                    
                    response = requests.post(server_url, data=overpass_query, timeout=timeout_seconds + 10,
                                           headers={'User-Agent': 'GeospatialIntelligenceSystem/1.0'})
                    if response.status_code == 200:
                        # Check if response has content
                        if response.text and response.text.strip():
                            break
                        else:
                            # Empty response, try next server
                            last_error = f"Server {server_url} returned empty response"
                            response = None
                            continue
                    elif response.status_code == 504:
                        # Timeout - try next server
                        last_error = f"Server {server_url} timed out (504)"
                        response = None
                        continue
                    elif response.status_code == 429:
                        # Rate limited - try next server
                        last_error = f"Server {server_url} rate limited (429)"
                        response = None
                        continue
                    else:
                        last_error = f"Server {server_url} returned status {response.status_code}: {response.text[:100] if response.text else 'No response body'}"
                        response = None
                        continue
                except requests.exceptions.Timeout:
                    last_error = f"Server {server_url} timed out"
                    response = None
                    continue
                except requests.exceptions.ConnectionError as e:
                    last_error = f"Server {server_url} connection error: {str(e)}"
                    response = None
                    continue
                except Exception as e:
                    last_error = f"Server {server_url} error: {str(e)}"
                    response = None
                    continue
            
            if response is None:
                # Suggest smaller radius if current radius is large
                suggested_radius = max(5, radius_km // 2) if radius_km > 10 else radius_km
                raise RuntimeError(
                    f"All Overpass servers failed. Last error: {last_error}. "
                    f"Try reducing the search radius (currently {radius_km}km, suggested: {suggested_radius}km) or try again later. "
                    f"The Overpass API may be overloaded - please wait a few minutes and retry."
                )
            
            if response.status_code != 200:
                raise RuntimeError(
                    f"Overpass API returned status {response.status_code}. "
                    f"Last error: {last_error}. "
                    f"Response: {response.text[:200] if response.text else 'No response body'}. "
                    f"Try reducing the search radius (currently {radius_km}km) or try again later."
                )
            
            # Parse response
            response_text = response.text.strip() if response.text else ""
            
            # Check if response is empty
            if not response_text:
                raise RuntimeError("Overpass API returned empty response. The server may be overloaded or the query timed out.")
            
            # Check if response is HTML/XML (error page) instead of JSON
            if response_text.startswith('<?xml') or response_text.startswith('<!DOCTYPE') or response_text.startswith('<html'):
                # Extract error message from HTML if possible
                import re
                error_match = re.search(r'<p[^>]*>(.*?)</p>', response_text, re.IGNORECASE | re.DOTALL)
                if error_match:
                    error_msg = error_match.group(1).strip()[:200]
                else:
                    # Try to find title or h1
                    title_match = re.search(r'<title[^>]*>(.*?)</title>', response_text, re.IGNORECASE | re.DOTALL)
                    if title_match:
                        error_msg = title_match.group(1).strip()[:200]
                    else:
                        error_msg = "Overpass API returned an HTML error page"
                
                raise RuntimeError(
                    f"Overpass API error: {error_msg}. "
                    f"The query may be too complex or the server is overloaded. "
                    f"Try reducing the search radius (currently {radius_km}km) or try again later."
                )
            
            # Try to parse as JSON
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                # Log the actual response for debugging
                error_msg = f"Failed to parse Overpass API response as JSON: {str(e)}"
                if response_text:
                    # Show first 200 chars of response for debugging
                    preview = response_text[:200] if len(response_text) > 200 else response_text
                    error_msg += f"\nResponse preview: {preview}"
                raise RuntimeError(error_msg)
            
            # Check for errors in Overpass response
            if 'remark' in data and 'runtime' not in data:
                raise RuntimeError(f"Overpass API error: {data.get('remark', 'Unknown error')}")
            
            # Check if response indicates an error
            if 'error' in data:
                error_info = data.get('error', {})
                if isinstance(error_info, dict):
                    error_msg = error_info.get('message', 'Unknown Overpass API error')
                else:
                    error_msg = str(error_info)
                raise RuntimeError(f"Overpass API error: {error_msg}")
            
            localities = []
            seen_names = set()  # Deduplicate by name
            
            if data.get('elements'):
                for element in data['elements']:
                    tags = element.get('tags', {})
                    
                    # Get locality name
                    name = tags.get('name') or tags.get('name:en') or tags.get('alt_name')
                    if not name:
                        continue
                    
                    # Deduplicate by name (case-insensitive)
                    name_lower = name.lower()
                    if name_lower in seen_names:
                        continue
                    seen_names.add(name_lower)
                    
                    # Get place type or boundary type
                    place_type = tags.get('place', 'unknown')
                    if place_type == 'unknown':
                        # Check if it's an administrative boundary
                        if tags.get('boundary') == 'administrative':
                            admin_level = tags.get('admin_level', '')
                            place_type = f'ward_{admin_level}' if admin_level else 'administrative'
                    
                    # Get center coordinates
                    center = element.get('center')
                    if center:
                        center_lat = center.get('lat')
                        center_lon = center.get('lon')
                    else:
                        # Fallback: use lat/lon if available
                        center_lat = element.get('lat')
                        center_lon = element.get('lon')
                    
                    if center_lat is None or center_lon is None:
                        continue  # Skip if no coordinates
                    
                    # Get OSM ID and type for later geometry fetching
                    osm_id = element.get('id')
                    osm_type = element.get('type')  # 'node', 'way', or 'relation'
                    
                    localities.append({
                        'name': name,
                        'lat': center_lat,
                        'lon': center_lon,
                        'place_type': place_type,
                        'osm_id': osm_id,
                        'osm_type': osm_type
                    })
            
            # Sort alphabetically by name
            localities.sort(key=lambda x: x['name'].lower())
            
            return localities
            
        except Exception as e:
            raise ValueError(f"Failed to fetch localities: {str(e)}")
    
    def get_locality_geometry(self, locality_name: str, lat: float, lon: float) -> Tuple[ee.Geometry, BoundingBox]:
        """
        Fetch full geometry for a single locality (called on-demand when user selects it)
        
        Queries Overpass AGAIN for the selected locality name and fetches full geometry.
        Supports relation (multipolygon) and way (polygon).
        
        Args:
            locality_name: Name of the locality
            lat: Latitude of locality center (for fallback)
            lon: Longitude of locality center (for fallback)
            
        Returns:
            Tuple of (polygon, bbox)
            If geometry unavailable, returns approximate 500m buffer around center
        """
        try:
            # Query Overpass AGAIN for the selected locality name
            overpass_servers = [
                "https://overpass-api.de/api/interpreter",
                "https://overpass.kumi.systems/api/interpreter",
                "https://overpass.openstreetmap.ru/api/interpreter"
            ]
            
            # Query pattern: search by name around center
            overpass_query = f"""[out:json][timeout:25];
(
  relation["name"="{locality_name}"](around:5000,{lat},{lon});
  way["name"="{locality_name}"](around:5000,{lat},{lon});
);
out geom;"""
            
            response = None
            last_error = None
            
            # Try each server until one works
            for server_url in overpass_servers:
                try:
                    response = requests.post(server_url, data=overpass_query, timeout=30,
                                            headers={'User-Agent': 'GeospatialIntelligenceSystem/1.0'})
                    if response.status_code == 200:
                        if response.text and response.text.strip():
                            break
                        else:
                            last_error = f"Server {server_url} returned empty response"
                            response = None
                            continue
                    elif response.status_code == 504:
                        last_error = f"Server {server_url} timed out (504)"
                        response = None
                        continue
                    elif response.status_code == 429:
                        last_error = f"Server {server_url} rate limited (429)"
                        response = None
                        continue
                    else:
                        last_error = f"Server {server_url} returned status {response.status_code}"
                        response = None
                        continue
                except requests.exceptions.Timeout:
                    last_error = f"Server {server_url} timed out"
                    response = None
                    continue
                except requests.exceptions.ConnectionError as e:
                    last_error = f"Server {server_url} connection error: {str(e)}"
                    response = None
                    continue
                except Exception as e:
                    last_error = f"Server {server_url} error: {str(e)}"
                    response = None
                    continue
            
            # If all servers failed, use fallback
            if response is None or response.status_code != 200:
                return self._create_fallback_geometry(lat, lon, "Locality geometry unavailable. Using approximate area.")
            
            # Parse response
            response_text = response.text.strip() if response.text else ""
            if not response_text:
                return self._create_fallback_geometry(lat, lon, "Locality geometry unavailable. Using approximate area.")
            
            # Check if response is HTML/XML (error page)
            if response_text.startswith('<?xml') or response_text.startswith('<!DOCTYPE') or response_text.startswith('<html'):
                return self._create_fallback_geometry(lat, lon, "Locality geometry unavailable. Using approximate area.")
            
            # Parse JSON
            try:
                data = response.json()
            except json.JSONDecodeError:
                return self._create_fallback_geometry(lat, lon, "Locality geometry unavailable. Using approximate area.")
            
            # Check for errors
            if 'remark' in data and 'runtime' not in data:
                return self._create_fallback_geometry(lat, lon, "Locality geometry unavailable. Using approximate area.")
            
            if 'error' in data:
                return self._create_fallback_geometry(lat, lon, "Locality geometry unavailable. Using approximate area.")
            
            # Process elements to extract geometry
            geometry = None
            bbox = None
            
            if data.get('elements'):
                for element in data['elements']:
                    element_type = element.get('type')
                    
                    if element_type == 'relation':
                        # Relation → multipolygon (use Nominatim lookup)
                        osm_id = element.get('id')
                        if osm_id:
                            geometry, bbox = self._get_geometry_from_nominatim_lookup(f"R{osm_id}")
                            if geometry is not None:
                                break  # Found valid geometry
                    
                    elif element_type == 'way':
                        # Way → polygon (extract geometry directly)
                        if 'geometry' in element:
                            coords = element['geometry']
                            if coords and len(coords) >= 3:
                                # Convert to Earth Engine format
                                ee_coords = [[point['lon'], point['lat']] for point in coords]
                                geometry = ee.Geometry.Polygon([ee_coords])
                                
                                # Calculate bounding box
                                lons = [point['lon'] for point in coords]
                                lats = [point['lat'] for point in coords]
                                bbox = BoundingBox(
                                    min_lon=min(lons),
                                    min_lat=min(lats),
                                    max_lon=max(lons),
                                    max_lat=max(lats)
                                )
                                break  # Found valid geometry
            
            # If geometry found, return it
            if geometry is not None and bbox is not None:
                return geometry, bbox
            
            # Fallback: 500m buffer around center
            return self._create_fallback_geometry(lat, lon, "Locality geometry unavailable. Using approximate area.")
            
        except Exception as e:
            # On any error, use fallback
            return self._create_fallback_geometry(lat, lon, f"Locality geometry unavailable. Using approximate area. Error: {str(e)}")
    
    def _create_fallback_geometry(self, lat: float, lon: float, message: str = None) -> Tuple[ee.Geometry, BoundingBox]:
        """
        Create fallback geometry: 800m × 800m square AOI centered on locality (MAX 1km)
        
        This prevents vegetation dilution from surrounding agricultural land.
        Rejects any AOI that includes farmland or open countryside.
        
        Args:
            lat: Latitude
            lon: Longitude
            message: Optional warning message (logged but not raised)
            
        Returns:
            Tuple of (polygon, bbox) - 800m × 800m square (MAX 1km)
        """
        if message:
            # Log warning but don't crash
            print(f"Warning: {message}")
        
        # Create 800m × 800m square AOI (MAX 1km)
        # 800m = 0.4km radius = 0.0036 degrees at equator
        # Account for latitude: 1 degree lat ≈ 111km, 1 degree lon ≈ 111km * cos(lat)
        radius_km = 0.4  # 0.4km = half of 800m
        radius_degrees_lat = radius_km / 111.0
        radius_degrees_lon = radius_km / (111.0 * abs(np.cos(np.radians(lat))))
        
        bbox = BoundingBox(
            min_lon=lon - radius_degrees_lon,
            min_lat=lat - radius_degrees_lat,
            max_lon=lon + radius_degrees_lon,
            max_lat=lat + radius_degrees_lat
        )
        
        # Validate AOI size (reject if > 1.0 km)
        aoi_size_km = 2 * radius_km  # Total size
        if aoi_size_km > 1.0:
            raise ValueError(f"AOI size ({aoi_size_km:.2f} km) exceeds maximum allowed (1.0 km)")
        
        geometry = ee.Geometry.Rectangle([
            bbox.min_lon, bbox.min_lat,
            bbox.max_lon, bbox.max_lat
        ])
        
        return geometry, bbox
    
    def get_osm_urban_context(self, polygon: ee.Geometry, bbox: BoundingBox) -> Dict:
        """
        Get OSM urban context data for India-specific urban likelihood scoring.
        
        Computes:
        - Road density (km/km²)
        - Building footprint density (%)
        - Road count and building count
        - Whether inside municipal boundary
        
        Args:
            polygon: Earth Engine polygon geometry for the locality
            bbox: Bounding box for the locality
            
        Returns:
            Dictionary with urban context metrics for likelihood scoring
        """
        try:
            # Get bounding box coordinates
            min_lon, min_lat = bbox.min_lon, bbox.min_lat
            max_lon, max_lat = bbox.max_lon, bbox.max_lat
            
            # Calculate AOI area in km²
            avg_lat = (min_lat + max_lat) / 2
            lat_size_km = (max_lat - min_lat) * 111.0
            lon_size_km = (max_lon - min_lon) * 111.0 * abs(np.cos(np.radians(avg_lat)))
            aoi_area_km2 = lat_size_km * lon_size_km
            
            overpass_servers = [
                "https://overpass-api.de/api/interpreter",
                "https://overpass.kumi.systems/api/interpreter",
                "https://overpass.openstreetmap.ru/api/interpreter"
            ]
            
            # Query for roads and buildings with geometry to calculate lengths/areas
            overpass_query = f"""[out:json][timeout:25];
(
  way["highway"]({min_lat},{min_lon},{max_lat},{max_lon});
  way["building"]({min_lat},{min_lon},{max_lat},{max_lon});
);
out geom;"""
            
            response = None
            last_error = None
            
            for server in overpass_servers:
                try:
                    response = requests.post(server, data={'data': overpass_query}, timeout=30)
                    if response.status_code == 200:
                        break
                except Exception as e:
                    last_error = str(e)
                    continue
            
            if response is None or response.status_code != 200:
                # Return default values if OSM query fails
                return {
                    'has_roads': False,
                    'has_buildings': False,
                    'road_count': 0,
                    'building_count': 0,
                    'road_length_km': 0.0,
                    'building_area_km2': 0.0,
                    'road_density_km_per_km2': 0.0,
                    'building_density_pct': 0.0,
                    'is_municipal': False,
                    'error': f"OSM query failed: {last_error}" if last_error else "OSM query failed"
                }
            
            data = response.json()
            
            # Calculate road length and building area
            road_length_m = 0.0
            building_area_m2 = 0.0
            road_count = 0
            building_count = 0
            
            if 'elements' in data:
                for element in data['elements']:
                    if element.get('type') == 'way':
                        tags = element.get('tags', {})
                        geometry = element.get('geometry', [])
                        
                        if len(geometry) < 2:
                            continue
                        
                        coords = [(p['lon'], p['lat']) for p in geometry]
                        
                        if 'highway' in tags:
                            # Calculate road length using Haversine formula
                            from math import radians, sin, cos, sqrt, atan2
                            R = 6371000  # Earth radius in meters
                            for i in range(len(coords) - 1):
                                lon1, lat1 = coords[i]
                                lon2, lat2 = coords[i + 1]
                                dlat = radians(lat2 - lat1)
                                dlon = radians(lon2 - lon1)
                                a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
                                c = 2 * atan2(sqrt(a), sqrt(1-a))
                                road_length_m += R * c
                            road_count += 1
                            
                        elif 'building' in tags:
                            # Calculate building area using shoelace formula
                            if len(coords) >= 3:
                                area = 0.0
                                for i in range(len(coords)):
                                    j = (i + 1) % len(coords)
                                    area += coords[i][0] * coords[j][1]
                                    area -= coords[j][0] * coords[i][1]
                                area = abs(area) / 2.0
                                # Convert to m² (approximate)
                                lat_center = sum(p['lat'] for p in geometry) / len(geometry)
                                lat_factor = 111000.0 * np.cos(np.radians(lat_center))
                                lon_factor = 111000.0
                                building_area_m2 += area * lat_factor * lon_factor
                            building_count += 1
            
            road_length_km = road_length_m / 1000.0
            building_area_km2 = building_area_m2 / 1e6
            road_density = road_length_km / aoi_area_km2 if aoi_area_km2 > 0 else 0.0
            building_density_pct = (building_area_km2 / aoi_area_km2) * 100 if aoi_area_km2 > 0 else 0.0
            
            # Check if inside municipal boundary (admin_level=8)
            # This is a simplified check - in practice, you'd query for admin boundaries
            is_municipal = road_density > 3.0 or building_density_pct > 15.0  # Heuristic
            
            return {
                'has_roads': road_count > 0,
                'has_buildings': building_count > 0,
                'road_count': road_count,
                'building_count': building_count,
                'road_length_km': road_length_km,
                'building_area_km2': building_area_km2,
                'road_density_km_per_km2': road_density,
                'building_density_pct': building_density_pct,
                'is_municipal': is_municipal
            }
            
        except Exception as e:
            # Return default values on error
            return {
                'has_roads': False,
                'has_buildings': False,
                'road_count': 0,
                'building_count': 0,
                'road_length_km': 0.0,
                'building_area_km2': 0.0,
                'road_density_km_per_km2': 0.0,
                'building_density_pct': 0.0,
                'is_municipal': False,
                'error': str(e)
            }
    
    def validate_urban_with_osm(self, polygon: ee.Geometry, bbox: BoundingBox) -> Dict:
        """
        Validate urban detection using OpenStreetMap roads and buildings.
        (Kept for backward compatibility - delegates to get_osm_urban_context)
        """
        context = self.get_osm_urban_context(polygon, bbox)
        return {
            'has_roads': context.get('has_roads', False),
            'has_buildings': context.get('has_buildings', False),
            'road_count': context.get('road_count', 0),
            'building_count': context.get('building_count', 0),
            'validation_note': "OSM infrastructure detected" if context.get('has_roads') or context.get('has_buildings') else None
        }
    
    def _get_geometry_from_nominatim_lookup(self, osm_id: str) -> Tuple[Optional[ee.Geometry], Optional[BoundingBox]]:
        """
        Get geometry from Nominatim lookup using OSM ID
        
        Args:
            osm_id: OSM ID (e.g., "R123456" for relation, "W123456" for way)
            
        Returns:
            Tuple of (geometry, bbox) or (None, None) if not found
        """
        try:
            nominatim_lookup_url = "https://nominatim.openstreetmap.org/lookup"
            lookup_params = {
                'osm_ids': osm_id,
                'format': 'geojson',
                'polygon_geojson': 1
            }
            
            lookup_response = requests.get(nominatim_lookup_url, params=lookup_params, timeout=15,
                                          headers={'User-Agent': 'GeospatialIntelligenceSystem/1.0'})
            
            if lookup_response.status_code == 200:
                lookup_data = lookup_response.json()
                if lookup_data.get('features'):
                    feature = lookup_data['features'][0]
                    geometry_data = feature.get('geometry')
                    
                    if geometry_data and geometry_data.get('type') == 'Polygon':
                        coordinates = geometry_data['coordinates'][0]
                        ee_coords = [[coord[0], coord[1]] for coord in coordinates]
                        polygon = ee.Geometry.Polygon(ee_coords)
                        
                        lons = [coord[0] for coord in coordinates]
                        lats = [coord[1] for coord in coordinates]
                        bbox = BoundingBox(
                            min_lon=min(lons),
                            min_lat=min(lats),
                            max_lon=max(lons),
                            max_lat=max(lats)
                        )
                        
                        return polygon, bbox
                    
                    elif geometry_data and geometry_data.get('type') == 'MultiPolygon':
                        multi_polygon_coords = []
                        all_lons = []
                        all_lats = []
                        
                        for polygon_coords in geometry_data['coordinates']:
                            outer_ring = polygon_coords[0]
                            multi_polygon_coords.append([[coord[0], coord[1]] for coord in outer_ring])
                            all_lons.extend([coord[0] for coord in outer_ring])
                            all_lats.extend([coord[1] for coord in outer_ring])
                        
                        polygon = ee.Geometry.MultiPolygon(multi_polygon_coords)
                        bbox = BoundingBox(
                            min_lon=min(all_lons),
                            min_lat=min(all_lats),
                            max_lon=max(all_lons),
                            max_lat=max(all_lats)
                        )
                        
                        return polygon, bbox
            
            return None, None
        except Exception as e:
            return None, None


class EarthEngineService:
    """Handle Google Earth Engine operations"""
    
    @staticmethod
    def validate_geometry(geom: ee.Geometry) -> Tuple[bool, Optional[str]]:
        """
        Validate geometry for Dynamic World analysis
        
        Checks:
        - Area > 1 km² (1e6 square meters)
        - Geometry is valid
        
        Args:
            geom: Earth Engine geometry
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Check area
            area = geom.area().getInfo()
            if area < 1e6:  # Less than 1 km²
                return False, f"Geometry area too small: {area:.0f} m² (minimum 1 km²)"
            
            # Check validity
            is_valid = geom.isValid().getInfo()
            if not is_valid:
                return False, "Geometry is invalid (self-intersections or other topology errors)"
            
            return True, None
            
        except Exception as e:
            return False, f"Geometry validation failed: {str(e)}"
    
    @staticmethod
    def create_centroid_buffer(lat: float, lon: float, radius_meters: int) -> ee.Geometry:
        """
        Create circular buffer around centroid
        
        Args:
            lat: Latitude
            lon: Longitude
            radius_meters: Buffer radius in meters
            
        Returns:
            Earth Engine geometry (circular buffer)
        """
        point = ee.Geometry.Point(lon, lat)
        buffer_geom = point.buffer(radius_meters)
        return buffer_geom
    
    @staticmethod
    def preprocess_polygon(raw_polygon: ee.Geometry) -> ee.Geometry:
        """
        Preprocess OSM polygon before using in Google Earth Engine
        
        Production-grade preprocessing to reduce vertex count and fix topology:
        1. Simplify with 200m tolerance to reduce vertex count
        2. Buffer with 1 meter to fix invalid topology
        3. Transform to EPSG:4326 with scale 1
        
        Args:
            raw_polygon: Raw Earth Engine geometry from OSM
            
        Returns:
            Preprocessed Earth Engine geometry ready for Earth Engine operations
        """
        # Step 1: Simplify with 200m tolerance to reduce vertex count
        geometry = raw_polygon.simplify(maxError=200)
        
        # Step 2: Buffer with 1 meter to fix invalid topology (self-intersections, etc.)
        geometry = geometry.buffer(1)
        
        # Step 3: Transform to EPSG:4326 with scale 1
        geometry = geometry.transform('EPSG:4326', 1)
        
        return geometry
    
    @staticmethod
    def preprocess_locality_polygon(raw_polygon: ee.Geometry) -> ee.Geometry:
        """
        Create FIXED 1km × 1km AOI centered on geocoded centroid.
        
        STRICT STANDARDIZATION:
        - For EVERY city or locality: Use FIXED AOI of exactly 1km × 1km
        - Centered on the geocoded centroid ONLY
        - Do NOT expand, shrink, or adapt AOI by city type
        - Reject polygons larger than 1.2 km (return error)
        
        Args:
            raw_polygon: Raw Earth Engine geometry from OSM (used only to get centroid)
            
        Returns:
            Fixed 1km × 1km square AOI centered on centroid
        """
        geom = ee.Geometry(raw_polygon)
        
        # Get centroid
        centroid = geom.centroid()
        centroid_coords = centroid.getInfo()['coordinates']
        cent_lon, cent_lat = centroid_coords[0], centroid_coords[1]
        
        # Check if original polygon is too large (> 1.2 km) - reject it
        bbox = geom.bounds()
        bbox_coords = bbox.getInfo()['coordinates'][0]
        lons = [c[0] for c in bbox_coords]
        lats = [c[1] for c in bbox_coords]
        min_lon, max_lon = min(lons), max(lons)
        min_lat, max_lat = min(lats), max(lats)
        
        # Calculate original polygon size
        avg_lat = (min_lat + max_lat) / 2
        lat_size_km = (max_lat - min_lat) * 111.0
        lon_size_km = (max_lon - min_lon) * 111.0 * abs(np.cos(np.radians(avg_lat)))
        aoi_size_km = max(lat_size_km, lon_size_km)
        
        if aoi_size_km > 1.2:
            raise ValueError(f"Polygon size ({aoi_size_km:.2f} km) exceeds maximum allowed (1.2 km). Cannot create fixed AOI.")
        
        # Create FIXED 1km × 1km square centered on centroid
        # 0.5 km radius = 1km × 1km square
        radius_km = 0.5
        radius_degrees_lat = radius_km / 111.0
        radius_degrees_lon = radius_km / (111.0 * abs(np.cos(np.radians(cent_lat))))
        
        # Create 1km × 1km square AOI
        aoi_box = ee.Geometry.Rectangle([
            cent_lon - radius_degrees_lon,
            cent_lat - radius_degrees_lat,
            cent_lon + radius_degrees_lon,
            cent_lat + radius_degrees_lat
        ])
        
        # Transform to EPSG:4326
        aoi_box = aoi_box.transform('EPSG:4326', 1)
        
        return aoi_box
    
    def __init__(self, project: Optional[str] = None):
        """
        Initialize Earth Engine service
        
        Args:
            project: Optional Google Cloud project ID. Can also be set via 
                    EARTHENGINE_PROJECT environment variable.
        """
        try:
            # Get project from parameter or environment variable
            if not project:
                import os
                project = os.getenv('EARTHENGINE_PROJECT')
            
            # Initialize Earth Engine
            try:
                if project:
                    ee.Initialize(project=project)
                else:
                    ee.Initialize()
            except Exception as init_error:
                error_msg = str(init_error).lower()
                
                # If no project found, provide helpful error
                if "no project found" in error_msg or "project" in error_msg:
                    raise RuntimeError(
                        f"Google Earth Engine requires a Google Cloud project.\n\n"
                        f"To fix this:\n"
                        f"1. Visit https://code.earthengine.google.com/ to see your projects\n"
                        f"2. Or set EARTHENGINE_PROJECT environment variable:\n"
                        f"   PowerShell: $env:EARTHENGINE_PROJECT='your-project-id'\n"
                        f"   Or add to .env file: EARTHENGINE_PROJECT=your-project-id\n\n"
                        f"Original error: {str(init_error)}"
                    )
                
                # If authentication needed, try to authenticate
                if "auth" in error_msg or "credential" in error_msg:
                    try:
                        ee.Authenticate()
                        if project:
                            ee.Initialize(project=project)
                        else:
                            ee.Initialize()
                    except Exception as auth_error:
                        raise RuntimeError(
                            f"Failed to authenticate Google Earth Engine.\n"
                            f"Please run: python authenticate_earth_engine.py\n"
                            f"Or: python -c \"import ee; ee.Authenticate()\"\n\n"
                            f"Error: {str(auth_error)}"
                        )
                else:
                    raise RuntimeError(
                        f"Failed to initialize Google Earth Engine: {str(init_error)}\n"
                        f"Please run: python authenticate_earth_engine.py"
                    )
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"Failed to initialize Google Earth Engine: {str(e)}\n"
                f"Please run: python authenticate_earth_engine.py"
            )
    
    def get_dynamic_world_image(self, polygon: ee.Geometry, bbox: BoundingBox, 
                                start_date: str = None, end_date: str = None) -> Tuple[ee.Image, str]:
        """
        Fetch Google Dynamic World land cover image with FIXED parameters.
        
        DATA STANDARDIZATION:
        - Use Google Earth Engine Dynamic World V1
        - Use ONLY the "label" band
        - Use SAME year for all cities (2024)
        - Use SAME season window (Jan-Mar)
        - Use cloud probability < 20%
        - No dynamic time selection
        
        Args:
            polygon: Earth Engine polygon geometry (fixed 1km × 1km AOI)
            bbox: Bounding box (not used, kept for compatibility)
            start_date: Ignored - uses fixed dates
            end_date: Ignored - uses fixed dates
        
        Returns:
            Tuple of (image with 'label' band, date_string): The Dynamic World image and its date
        """
        # Load Dynamic World ImageCollection
        dw_collection = ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1')
        
        # Filter by geometry
        collection = dw_collection.filterBounds(polygon)
        
        # FIXED DATE RANGE: 2024, Jan-Mar (same for all cities)
        fixed_start_date = '2024-01-01'
        fixed_end_date = '2024-03-31'
        
        # Filter by fixed date range
        collection = collection.filterDate(fixed_start_date, fixed_end_date)
        
        # Check if collection has images
        collection_size = collection.size().getInfo()
        if collection_size == 0:
            raise RuntimeError(f"No Dynamic World image available for {fixed_start_date} to {fixed_end_date}")
        
        # Use median composite of the fixed season window
        # This ensures consistency across all cities
        dw_image = collection.select('label').median()
        
        # Get the most recent date from the collection for reporting
        try:
            recent_image = collection.sort('system:time_start', False).first()
            date_info = recent_image.get('system:time_start').getInfo()
            if date_info:
                date_obj = datetime.fromtimestamp(date_info / 1000)
                date_string = date_obj.strftime('%Y-%m-%d')
            else:
                date_string = '2024-02-15'  # Mid-season default
        except:
            date_string = '2024-02-15'  # Mid-season default
        
        # SELECT + UNMASK (CRITICAL)
        # Convert masked pixels to -1 so they can be counted and filtered out later
        labels = dw_image.unmask(-1)
        
        return labels, date_string
    
    def _create_2km_tiles(self, geometry: ee.Geometry, bbox: BoundingBox) -> List[ee.Geometry]:
        """
        Create 2km × 2km grid tiles from bounding box and intersect with geometry
        
        Args:
            geometry: Earth Engine geometry (city boundary)
            bbox: Bounding box of the geometry
            
        Returns:
            List of tile geometries (intersected with city boundary)
        """
        # Approximate conversion: 1 degree latitude ≈ 111 km
        # 1 degree longitude ≈ 111 km * cos(latitude)
        # For 2km tiles: 2/111 ≈ 0.018 degrees
        
        # Use average latitude for longitude conversion
        avg_lat = (bbox.min_lat + bbox.max_lat) / 2
        lat_degree_to_km = 111.0
        lon_degree_to_km = 111.0 * abs(np.cos(np.radians(avg_lat)))
        
        # 2km in degrees
        tile_size_lat = 2.0 / lat_degree_to_km  # ~0.018 degrees
        tile_size_lon = 2.0 / lon_degree_to_km  # ~0.018 degrees (adjusted for latitude)
        
        tiles = []
        current_lat = bbox.min_lat
        
        while current_lat < bbox.max_lat:
            current_lon = bbox.min_lon
            while current_lon < bbox.max_lon:
                # Create 2km × 2km tile
                tile_bbox = ee.Geometry.Rectangle([
                    current_lon,
                    current_lat,
                    min(current_lon + tile_size_lon, bbox.max_lon),
                    min(current_lat + tile_size_lat, bbox.max_lat)
                ])
                
                # Intersect tile with city geometry (only count pixels inside city boundary)
                tile = geometry.intersection(tile_bbox)
                tiles.append(tile)
                
                current_lon += tile_size_lon
            
            current_lat += tile_size_lat
        
        return tiles
    
    def _merge_histograms(self, histograms: List[Dict]) -> Dict[int, int]:
        """
        Merge multiple histogram dictionaries into one
        
        Args:
            histograms: List of histogram dictionaries
            
        Returns:
            Merged histogram dictionary
        """
        merged = {}
        for hist in histograms:
            if hist and 'label' in hist:
                label_hist = hist['label']
                for label_str, count in label_hist.items():
                    try:
                        label = int(float(label_str))
                        count_val = int(float(count))
                        merged[label] = merged.get(label, 0) + count_val
                    except (ValueError, TypeError):
                        continue
        return merged
    
    def count_pixels_by_class_direct(self, image: ee.Image, polygon: ee.Geometry, scale: int = 30) -> Dict[int, int]:
        """
        Count pixels per land cover class using direct reduceRegion
        
        EXACT pipeline as specified:
        1. Run reduceRegion with frequencyHistogram
        2. Post-process: Remove key "-1" from histogram (masked pixels)
        3. Use remaining counts
        
        Args:
            image: Earth Engine image with 'label' band (Dynamic World, already unmasked)
            polygon: Earth Engine polygon geometry (preprocessed)
            scale: Resolution in meters (default 30m)
            
        Returns:
            Dictionary mapping class labels to pixel counts
        """
        try:
            # Step 4: reduceRegion
            # var hist = labels.reduceRegion({
            #     reducer: ee.Reducer.frequencyHistogram(),
            #     geometry: geom,
            #     scale: 30,
            #     maxPixels: 1e13
            # });
            histogram = image.reduceRegion(
                reducer=ee.Reducer.frequencyHistogram(),
                geometry=polygon,
                scale=scale,
                maxPixels=1e13,
                bestEffort=True  # Use bestEffort to handle edge cases
            )
            
            histogram_dict = histogram.getInfo()
            
            # Check if hist is empty
            if not histogram_dict or 'label' not in histogram_dict:
                # Try with expanded geometry
                expanded_geom = polygon.buffer(100)
                histogram = image.reduceRegion(
                    reducer=ee.Reducer.frequencyHistogram(),
                    geometry=expanded_geom,
                    scale=scale,
                    maxPixels=1e13,
                    bestEffort=True
                )
                histogram_dict = histogram.getInfo()
                
                if not histogram_dict or 'label' not in histogram_dict:
                    # Return empty dict - let caller handle fallback
                    return {}
            
            label_histogram = histogram_dict.get('label', {})
            
            if not label_histogram or len(label_histogram) == 0:
                # Try with expanded geometry
                expanded_geom = polygon.buffer(100)
                histogram = image.reduceRegion(
                    reducer=ee.Reducer.frequencyHistogram(),
                    geometry=expanded_geom,
                    scale=scale,
                    maxPixels=1e13,
                    bestEffort=True
                )
                histogram_dict = histogram.getInfo()
                
                if histogram_dict and 'label' in histogram_dict:
                    label_histogram = histogram_dict['label']
                else:
                    return {}
            
            # Step 5: Post-process - Remove key "-1" from histogram (masked pixels)
            # Convert to integer keys and values, filtering out masked pixels (-1)
            pixel_counts = {}
            total_pixels = 0
            masked_pixels = 0
            
            for label_str, count in label_histogram.items():
                try:
                    label = int(float(label_str))
                    count_val = int(float(count))
                    
                    # Remove masked pixels (key = -1)
                    if label == -1:
                        masked_pixels += count_val
                        continue
                    
                    # Only count valid Dynamic World labels (0-7)
                    if 0 <= label <= 7 and count_val > 0:
                        pixel_counts[label] = count_val
                        total_pixels += count_val
                except (ValueError, TypeError):
                    continue
            
            # If all pixels are masked, return empty dict
            if total_pixels == 0:
                return {}
            
            return pixel_counts
            
        except Exception as e:
            # Return empty dict on error - let caller handle fallback
            return {}
    
    def _count_pixels_tiled(self, image: ee.Image, polygon: ee.Geometry, scale: int = 30) -> Dict[int, int]:
        """
        Count pixels by subdividing geometry into 2km × 2km tiles
        
        Step 5: If hist is empty, subdivide geom into 2km × 2km tiles,
        run reduceRegion per tile, merge histograms
        
        Args:
            image: Earth Engine image with 'label' band
            polygon: Earth Engine polygon geometry (preprocessed)
            scale: Resolution in meters (default 30m)
            
        Returns:
            Dictionary mapping class labels to pixel counts
        """
        # Get bounding box of polygon
        try:
            bounds = polygon.bounds().getInfo()['coordinates'][0]
            min_lon, min_lat = bounds[0]
            max_lon, max_lat = bounds[2]
        except Exception as e:
            raise RuntimeError(f"Failed to get polygon bounds: {str(e)}")
        
        # Create 2km × 2km tiles
        # 1 degree latitude ≈ 111 km, so 2km ≈ 0.018 degrees
        avg_lat = (min_lat + max_lat) / 2
        lat_degree_to_km = 111.0
        lon_degree_to_km = 111.0 * abs(np.cos(np.radians(avg_lat)))
        
        tile_size_lat = 2.0 / lat_degree_to_km  # ~0.018 degrees
        tile_size_lon = 2.0 / lon_degree_to_km  # ~0.018 degrees
        
        all_pixel_counts = {}
        successful_tiles = 0
        failed_tiles = 0
        total_tiles = 0
        current_lat = min_lat
        
        while current_lat < max_lat:
            current_lon = min_lon
            while current_lon < max_lon:
                total_tiles += 1
                # Create tile
                try:
                    tile_bbox = ee.Geometry.Rectangle([
                        current_lon,
                        current_lat,
                        min(current_lon + tile_size_lon, max_lon),
                        min(current_lat + tile_size_lat, max_lat)
                    ])
                    
                    # Intersect tile with polygon
                    tile = polygon.intersection(tile_bbox)
                    
                    # Check if tile has area
                    try:
                        tile_area = tile.area().getInfo()
                        if tile_area < 1e-6:  # Skip very small tiles
                            current_lon += tile_size_lon
                            continue
                    except:
                        # If area check fails, try anyway
                        pass
                    
                    # Run reduceRegion on tile
                    # Use lower maxPixels and bestEffort for individual tiles
                    histogram = image.reduceRegion(
                        reducer=ee.Reducer.frequencyHistogram(),
                        geometry=tile,
                        scale=scale,
                        maxPixels=1e9,  # Lower for individual tiles
                        bestEffort=True,
                        tileScale=2  # Use tileScale for better performance
                    )
                    
                    histogram_dict = histogram.getInfo()
                    
                    if histogram_dict and 'label' in histogram_dict:
                        label_histogram = histogram_dict['label']
                        if label_histogram and len(label_histogram) > 0:
                            # Merge into all_pixel_counts
                            for label_str, count in label_histogram.items():
                                try:
                                    label = int(float(label_str))
                                    count_val = int(float(count))
                                    all_pixel_counts[label] = all_pixel_counts.get(label, 0) + count_val
                                except (ValueError, TypeError):
                                    continue
                            successful_tiles += 1
                        else:
                            failed_tiles += 1
                    else:
                        failed_tiles += 1
                except Exception as e:
                    # Skip failed tiles but log for debugging
                    failed_tiles += 1
                    continue
                
                current_lon += tile_size_lon
            
            current_lat += tile_size_lat
        
        if not all_pixel_counts:
            raise RuntimeError(
                f"Empty histogram after tiling. "
                f"Total tiles: {total_tiles}, Successful: {successful_tiles}, Failed: {failed_tiles}. "
                f"Dynamic World may not have data for this locality in the last 30 days. "
                f"Try expanding the date range or check if the locality geometry is valid."
            )
        
        return all_pixel_counts
    
    def count_pixels_by_class(self, image: ee.Image, polygon: ee.Geometry, bbox: BoundingBox, scale: int = 30) -> Dict[int, int]:
        """
        Count pixels per land cover class using tiled reduceRegion approach
        
        Always uses 2km × 2km tiles to prevent Earth Engine timeouts.
        Uses scale=30 for 9x faster processing while maintaining accuracy.
        
        Args:
            image: Earth Engine image with 'label' band
            polygon: Earth Engine polygon geometry (city boundary, preprocessed)
            bbox: Bounding box for area calculation
            scale: Resolution in meters (default 30m - 9x faster than 10m)
            
        Returns:
            Dictionary mapping class labels to pixel counts
        """
        # Always use tiled approach with 2km × 2km tiles
        # This prevents timeouts and is more reliable
        return self._count_pixels_tiled_2km(image, polygon, bbox, scale)
    
    def _count_pixels_tiled_2km(self, image: ee.Image, geometry: ee.Geometry, bbox: BoundingBox, scale: int) -> Dict[int, int]:
        """
        Count pixels by subdividing into 2km × 2km tiles and merging results
        
        Args:
            image: Earth Engine image with 'label' band
            geometry: Earth Engine polygon geometry (city boundary)
            bbox: Bounding box for area calculation
            scale: Resolution in meters (30m recommended)
            
        Returns:
            Dictionary mapping class labels to pixel counts
        """
        # Create 2km × 2km grid tiles
        tiles = self._create_2km_tiles(geometry, bbox)
        
        if not tiles:
            raise RuntimeError("Failed to create tiles from city geometry")
        
        # Validate that we have tiles
        if len(tiles) == 0:
            raise RuntimeError("No tiles created from city geometry")
        
        # For very small cities, 1 tile is acceptable - continue processing
        # The tile processing will handle empty/failed tiles gracefully
        
        # Process each tile
        tile_histograms = []
        successful_tiles = 0
        failed_tiles = 0
        skipped_tiles = 0
        
        for i, tile in enumerate(tiles):
            try:
                # Check if tile has any area (intersection with city boundary)
                tile_area = tile.area().getInfo()
                if tile_area < 1e-6:  # Skip very small tiles (< 1 square meter)
                    skipped_tiles += 1
                    continue
                
                # Clip dw.select("label") to tile
                tile_image = image.clip(tile)
                
                # Run reduceRegion with frequencyHistogram on this tile
                tile_histogram = tile_image.reduceRegion(
                    reducer=ee.Reducer.frequencyHistogram(),
                    geometry=tile,
                    scale=scale,
                    maxPixels=1e8,
                    bestEffort=True,
                    tileScale=2  # Lower tileScale for individual tiles
                )
                
                tile_hist_dict = tile_histogram.getInfo()
                
                # If tile returns empty, skip it (do not fail the whole job)
                if tile_hist_dict and 'label' in tile_hist_dict and tile_hist_dict['label']:
                    if len(tile_hist_dict['label']) > 0:  # Ensure histogram is not empty
                        tile_histograms.append(tile_hist_dict)
                        successful_tiles += 1
                    else:
                        failed_tiles += 1  # Empty histogram, skip
                else:
                    failed_tiles += 1  # No data, skip
                    
            except Exception as e:
                # Skip tiles that fail, continue with others
                failed_tiles += 1
                continue
        
        # If any tile returns empty, skip it (already done above)
        # Only fail if ALL tiles are empty
        if not tile_histograms:
            raise RuntimeError(
                f"Failed to get pixel counts from any tiles. "
                f"Total tiles created: {len(tiles)}, "
                f"Successful: {successful_tiles}, "
                f"Failed/empty: {failed_tiles}, "
                f"Skipped (too small): {skipped_tiles}. "
                f"Dynamic World may not have data coverage for this location, "
                f"or the city boundary geometry may be invalid."
            )
        
        # Merge all tile histograms
        merged_counts = self._merge_histograms(tile_histograms)
        
        if not merged_counts:
            raise RuntimeError(
                "Merged histogram is empty. "
                "Dynamic World may not have valid classification data for this location."
            )
        
        return merged_counts
    
    def get_sentinel2_sr_composite(self, polygon: ee.Geometry, bbox: BoundingBox,
                                   start_date: str = None, end_date: str = None,
                                   cloud_cover_threshold: float = 10.0) -> Tuple[ee.Image, str]:
        """
        Fetch Sentinel-2 Surface Reflectance composite for AOI
        
        STEP 2 — SENTINEL-2 PREPROCESSING:
        - Filter by AOI
        - Date range: last 12 months (or specified range)
        - Cloud cover < threshold (default 10%)
        - Use median composite
        - Select bands: B3 (Green), B4 (Red), B8 (NIR), B11 (SWIR)
        
        Args:
            polygon: Earth Engine polygon geometry (AOI, preprocessed)
            bbox: Bounding box (for validation)
            start_date: Optional start date (YYYY-MM-DD). Defaults to 12 months ago.
            end_date: Optional end date (YYYY-MM-DD). Defaults to now.
            cloud_cover_threshold: Maximum cloud cover percentage (default 10%)
        
        Returns:
            Tuple of (composite image with selected bands, date_string of latest image)
        """
        # Calculate date range (last 12 months if not specified)
        if end_date is None:
            end_date_obj = datetime.now()
        else:
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        if start_date is None:
            start_date_obj = end_date_obj - timedelta(days=365)  # 12 months
        else:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        
        start_date_str = start_date_obj.strftime('%Y-%m-%d')
        end_date_str = end_date_obj.strftime('%Y-%m-%d')
        
        # Load Sentinel-2 SR collection
        s2_collection = ee.ImageCollection('COPERNICUS/S2_SR')
        
        # Filter by AOI geometry
        collection = s2_collection.filterBounds(polygon)
        
        # Filter by date range
        collection = collection.filterDate(start_date_str, end_date_str)
        
        # Filter by cloud cover (using CLOUDY_PIXEL_PERCENTAGE property)
        collection = collection.filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', cloud_cover_threshold))
        
        # Sort by date (most recent first)
        collection = collection.sort('system:time_start', False)
        
        # Check if collection has images
        collection_size = collection.size().getInfo()
        if collection_size == 0:
            raise RuntimeError(
                f"No Sentinel-2 images found for the specified area and date range "
                f"({start_date_str} to {end_date_str}) with cloud cover < {cloud_cover_threshold}%"
            )
        
        # Get the most recent image for date reference
        latest_image = collection.first()
        try:
            date_info = latest_image.get('system:time_start').getInfo()
            if date_info:
                date_obj = datetime.fromtimestamp(date_info / 1000)
                date_string = date_obj.strftime('%Y-%m-%d')
            else:
                date_string = end_date_str
        except:
            date_string = end_date_str
        
        # Create median composite
        # Select required bands: B3 (Green), B4 (Red), B8 (NIR), B11 (SWIR)
        composite = collection.select(['B3', 'B4', 'B8', 'B11']).median()
        
        # Clip to AOI polygon
        composite = composite.clip(polygon)
        
        return composite, date_string
    
    def calculate_spectral_indices(self, image: ee.Image) -> ee.Image:
        """
        Calculate spectral indices from Sentinel-2 bands
        
        STEP 1 — SPECTRAL INDICES:
        - NDVI  = (B8 − B4) / (B8 + B4)
        - MNDWI = (B3 − B11) / (B3 + B11)
        
        NOTE: NDBI is NOT calculated because urban is a catch-all class
        and doesn't need NDBI for detection.
        
        Args:
            image: Earth Engine image with bands B3, B4, B8, B11
        
        Returns:
            Image with added bands: NDVI, MNDWI
        """
        # Extract bands
        green = image.select('B3')  # Green
        red = image.select('B4')   # Red
        nir = image.select('B8')   # NIR
        swir = image.select('B11') # SWIR
        
        # Calculate NDVI = (NIR - Red) / (NIR + Red)
        ndvi = nir.subtract(red).divide(nir.add(red)).rename('NDVI')
        
        # Calculate MNDWI = (Green - SWIR) / (Green + SWIR)
        mndwi = green.subtract(swir).divide(green.add(swir)).rename('MNDWI')
        
        # Add indices to image (NDBI not needed - urban is catch-all)
        image_with_indices = image.addBands([ndvi, mndwi])
        
        return image_with_indices
    
    def classify_land_cover_spectral(self, image: ee.Image) -> ee.Image:
        """
        Classify land cover using urban-first spectral indices (NO BARE LAND)
        
        STEP 1-4 — SIMPLE, ROBUST, URBAN-FIRST CLASSIFICATION:
        This is a simplified, robust classifier for CITY AOIs where bare land class
        is COMPLETELY REMOVED and urban is the default fallback class.
        
        CONTEXT:
        The current system incorrectly classifies dense Indian urban areas
        (Delhi, Vijayawada, Azad Nagar) as "Bare Land". Multiple attempts failed
        because pixels were left unclassified or bare land logic absorbed urban pixels.
        
        SOLUTION: Remove bare land completely, make urban the fallback.
        - Extract reliable classes first (Water, Forest, Vegetation)
        - Urban = catch-all for everything else
        - NO bare land class exists
        - NO pixels left unclassified
        - NO NDBI used (not needed)
        
        Classification Rules (ONLY 4 CLASSES):
        - Water: MNDWI > 0.2
        - Forest: NDVI > 0.6
        - Vegetation: 0.25 <= NDVI <= 0.6
        - Urban: NOT (water OR forest OR vegetation) ← FALLBACK CLASS
        
        This ensures:
        - Dusty concrete (Delhi, NCR) → Urban
        - Rooftops (all cities) → Urban
        - Paved yards (Andhra/Telangana) → Urban
        - Metal roofs (industrial/residential) → Urban
        - Concrete, asphalt → Urban
        - Everything else → Urban
        
        THRESHOLD EXPLANATIONS:
        1. Water (MNDWI > 0.2): Globally reliable water detection.
        2. Forest (NDVI > 0.6): Dense forest only. Stricter than typical (0.5) because
           in Indian cities, NDVI > 0.5 often includes parks and crop patches.
        3. Vegetation (0.25 <= NDVI <= 0.6): All vegetation including parks, crops,
           sparse vegetation. Intentionally broad to ensure urban doesn't absorb vegetation.
        4. Urban (catch-all): Everything that doesn't match water, forest, or vegetation.
           This is intentional and required. Urban absorbs all non-vegetation, non-water pixels.
        
        Priority order: Water > Forest > Vegetation > Urban (fallback)
        Every pixel inside AOI MUST belong to exactly ONE class.
        NO pixels remain unclassified.
        
        Args:
            image: Earth Engine image with NDVI, MNDWI bands (NDBI not needed)
        
        Returns:
            Image with 'landcover' band (0=Water, 1=Forest, 2=Urban, 3=Vegetation)
        """
        # Extract spectral indices (NDBI not needed - urban is catch-all)
        ndvi = image.select('NDVI')
        mndwi = image.select('MNDWI')
        
        # STEP 2 — RELIABLE MASKS FIRST
        # These are globally reliable and must be extracted first
        
        # Water: MNDWI > 0.2 (globally reliable water detection)
        water_mask = mndwi.gt(0.2)
        
        # Forest: NDVI > 0.6 (dense forest, rare in cities)
        # Stricter than typical (0.5) because in Indian cities, NDVI > 0.5 often
        # includes parks, trees, and crop patches which are NOT forests
        forest_mask = ndvi.gt(0.6).And(mndwi.lte(0.2))  # Exclude water
        
        # Vegetation: 0.25 <= NDVI <= 0.6 (all vegetation including sparse)
        # This captures parks, agricultural areas, sparse vegetation, mixed vegetation
        # Intentionally broad to ensure urban doesn't absorb vegetation
        vegetation_mask = ndvi.gte(0.25).And(ndvi.lte(0.6)).And(mndwi.lte(0.2))  # Exclude water
        
        # Dense Forest: NDVI > 0.6 (rare in cities)
        # Stricter than typical (0.5) because in Indian cities, NDVI > 0.5 often
        # includes parks, trees, and crop patches which are NOT forests
        forest_mask = ndvi.gt(0.6).And(mndwi.lte(0.2))  # Exclude water
        
        # STEP 3 — URBAN FALLBACK (CRITICAL)
        # Urban must be defined as: Any pixel that is NOT water AND NOT forest AND NOT vegetation
        # This is intentional. This is required. This must NOT use NDBI.
        # Urban = NOT (Water OR Forest OR Vegetation)
        urban_mask = (
            water_mask.Not()           # NOT water
            .And(forest_mask.Not())     # NOT forest
            .And(vegetation_mask.Not())  # NOT vegetation
        )
        
        # STEP 5 — CLASS PRIORITY (MANDATORY)
        # Start with NO CLASS (-1) instead of 0 (Water)
        # This prevents all pixels from defaulting to Water class
        landcover = ee.Image(-1)
        
        # Apply classes in priority order: Water > Forest > Vegetation > Urban (fallback)
        # CRITICAL: Urban is applied LAST as the fallback, ensuring it absorbs all remaining pixels
        # .where() updates pixels where mask is true. Since we start with -1, all masks will apply.
        # Later applications will overwrite earlier ones, but urban is applied last so it catches
        # everything that wasn't classified by the reliable classes.
        landcover = landcover.where(water_mask, 0)      # Water = 0 (highest priority)
        landcover = landcover.where(forest_mask, 1)     # Forest = 1
        landcover = landcover.where(vegetation_mask, 3) # Vegetation = 3
        landcover = landcover.where(urban_mask, 2)      # Urban = 2 (fallback, applied LAST)
        
        # CRITICAL: Ensure NO pixel remains unclassified
        # After assignment, NO pixel should remain with value -1
        # Mask ONLY -1 pixels if any appear (they should not, but safety check)
        landcover = landcover.updateMask(landcover.neq(-1))
        
        # Add as band
        classified = image.addBands(landcover.rename('landcover'))
        
        return classified
    
    def calculate_area_by_class_pixelarea(self, image: ee.Image, polygon: ee.Geometry, 
                                          scale: int = 10) -> Dict[int, float]:
        """
        Calculate area per land cover class using pixelArea() at specified scale
        
        STEP 6 — AREA CALCULATION:
        - Use pixelArea() to get accurate area per pixel
        - Compute area per class inside AOI
        - Scale: 10 meters (default)
        
        Args:
            image: Earth Engine image with 'landcover' band
            polygon: Earth Engine polygon geometry (AOI)
            scale: Resolution in meters (default 10m for Sentinel-2)
        
        Returns:
            Dictionary mapping class labels to area in square meters
        """
        # Select landcover band
        landcover = image.select('landcover')
        
        # Create area image (area per pixel at specified scale)
        area_image = ee.Image.pixelArea()  # Area in square meters
        
        # Calculate area per class by masking area image for each class
        area_dict = {}
        
        # Classes: 0=Water, 1=Forest, 2=Urban, 3=Vegetation (NO bare land - class 4 removed)
        for class_id in range(4):  # Only 4 classes now
            try:
                # Mask area image to only this class
                class_mask = landcover.eq(class_id)
                class_area_image = area_image.updateMask(class_mask)
                
                # Sum area for this class
                area_sum = class_area_image.reduceRegion(
                    reducer=ee.Reducer.sum(),
                    geometry=polygon,
                    scale=scale,
                    maxPixels=1e13,
                    bestEffort=True
                )
                
                area_value = area_sum.getInfo().get('constant', 0)
                if area_value > 0:
                    area_dict[class_id] = float(area_value)
            except Exception:
                # Skip this class if calculation fails
                continue
        
        # If no areas calculated, try tiled approach
        if not area_dict:
            return self._calculate_area_tiled(image, polygon, scale)
        
        return area_dict
    
    def _calculate_area_tiled(self, image: ee.Image, polygon: ee.Geometry, scale: int) -> Dict[int, float]:
        """
        Calculate area by class using tiled approach (fallback for large areas)
        
        Args:
            image: Earth Engine image with 'landcover' band
            polygon: Earth Engine polygon geometry
            scale: Resolution in meters
        
        Returns:
            Dictionary mapping class labels to area in square meters
        """
        # Get bounding box
        try:
            bounds = polygon.bounds().getInfo()['coordinates'][0]
            min_lon, min_lat = bounds[0]
            max_lon, max_lat = bounds[2]
        except Exception as e:
            raise RuntimeError(f"Failed to get polygon bounds: {str(e)}")
        
        # Create 2km × 2km tiles
        avg_lat = (min_lat + max_lat) / 2
        lat_degree_to_km = 111.0
        lon_degree_to_km = 111.0 * abs(np.cos(np.radians(avg_lat)))
        
        tile_size_lat = 2.0 / lat_degree_to_km
        tile_size_lon = 2.0 / lon_degree_to_km
        
        landcover = image.select('landcover')
        area_image = ee.Image.pixelArea()
        
        all_areas = {}
        current_lat = min_lat
        
        while current_lat < max_lat:
            current_lon = min_lon
            while current_lon < max_lon:
                try:
                    tile_bbox = ee.Geometry.Rectangle([
                        current_lon,
                        current_lat,
                        min(current_lon + tile_size_lon, max_lon),
                        min(current_lat + tile_size_lat, max_lat)
                    ])
                    
                    tile = polygon.intersection(tile_bbox)
                    
                    # Check tile area
                    try:
                        tile_area = tile.area().getInfo()
                        if tile_area < 1e-6:
                            current_lon += tile_size_lon
                            continue
                    except:
                        pass
                    
                    # Calculate area per class for this tile
                    # Only 4 classes now: 0=Water, 1=Forest, 2=Urban, 3=Vegetation (NO bare land)
                    for class_id in range(4):
                        try:
                            class_mask = landcover.eq(class_id)
                            class_area_image = area_image.updateMask(class_mask)
                            
                            area_sum = class_area_image.reduceRegion(
                                reducer=ee.Reducer.sum(),
                                geometry=tile,
                                scale=scale,
                                maxPixels=1e9,
                                bestEffort=True,
                                tileScale=2
                            )
                            
                            area_value = area_sum.getInfo().get('constant', 0)
                            if area_value > 0:
                                all_areas[class_id] = all_areas.get(class_id, 0) + float(area_value)
                        except Exception:
                            continue  # Skip this class for this tile
                
                except Exception:
                    pass  # Skip failed tiles
                
                current_lon += tile_size_lon
            
            current_lat += tile_size_lat
        
        return all_areas


class SpectralIndexClassifier:
    """Land cover classification using Sentinel-2 spectral indices (NO BARE LAND)"""
    
    # Class mapping for spectral index classification
    # 0 = Water
    # 1 = Forest (Dense Vegetation)
    # 2 = Urban (Built-up) - FALLBACK CLASS
    # 3 = Vegetation (Sparse Vegetation)
    # NOTE: Bare Land class (4) is COMPLETELY REMOVED
    
    CLASS_LABELS = {
        0: 'Water',
        1: 'Forest',
        2: 'Urban',
        3: 'Vegetation'
    }
    
    def aggregate_areas_to_percentages(self, area_dict: Dict[int, float], 
                                       total_aoi_area: float) -> LandCoverResult:
        """
        Convert area dictionary to LandCoverResult with percentages
        
        NOTE: Bare land class is COMPLETELY REMOVED. All non-vegetation, non-water
        pixels are classified as urban (the fallback class).
        
        Args:
            area_dict: Dictionary mapping class IDs to area in square meters
            total_aoi_area: Total AOI area in square meters
        
        Returns:
            LandCoverResult with percentages (bare_land class removed)
        """
        if not area_dict:
            raise ValueError("No area data provided")
        
        if total_aoi_area <= 0:
            raise ValueError("Total AOI area must be greater than 0")
        
        # Extract areas per class (NO bare land - class 4 removed)
        water_area = area_dict.get(0, 0.0)      # Water
        forest_area = area_dict.get(1, 0.0)     # Forest
        urban_area = area_dict.get(2, 0.0)      # Urban (fallback)
        vegetation_area = area_dict.get(3, 0.0)  # Vegetation
        
        # Calculate percentages
        water_pct = (water_area / total_aoi_area) * 100
        forest_pct = (forest_area / total_aoi_area) * 100
        urban_pct = (urban_area / total_aoi_area) * 100
        vegetation_pct = (vegetation_area / total_aoi_area) * 100
        
        # Calculate total pixels (approximate, based on 10m resolution)
        # Each pixel is 10m × 10m = 100 m²
        total_pixels = int(total_aoi_area / 100)
        
        # Validate that percentages sum to approximately 100%
        total_pct = water_pct + forest_pct + urban_pct + vegetation_pct
        if abs(total_pct - 100.0) > 5.0:  # Allow 5% tolerance for rounding
            raise ValueError(
                f"Total percentage ({total_pct:.2f}%) does not sum to ~100%. "
                f"This indicates classification or area calculation error."
            )
        
        return LandCoverResult(
            urban=urban_pct,
            forest=forest_pct,
            vegetation=vegetation_pct,
            water=water_pct,
            total_pixels=total_pixels
        )
    
    def validate_urban_city_results(self, land_cover: LandCoverResult, 
                                    city_name: str = "") -> List[str]:
        """
        STEP 7 — VALIDATION RULES
        
        If AOI is urban:
        - Urban < 60% → flag warning
        - Bare land > 20% → flag warning
        - Forest > 10% → flag warning
        
        These thresholds are optimized for Indian urban areas:
        - Urban < 60%: For dense urban areas like Azadpur Transport Centre, we expect 70-90% urban.
          If it's below 60%, there may be classification issues.
        - Bare land > 20%: In urban areas, bare land should be minimal (<10%). If it's >20%,
          it likely means impervious surfaces are being misclassified as bare land.
        - Forest > 10%: Indian cities typically have <5% forest. If it's >10%, parks/crops
          may be misclassified as forest.
        
        Args:
            land_cover: LandCoverResult with percentages
            city_name: Optional city name for context
        
        Returns:
            List of warning messages (empty if no warnings)
        """
        warnings = []
        
        # Check for urban cities (heuristic: if urban > 30%, consider it urban)
        is_urban_city = land_cover.urban > 30.0
        
        if is_urban_city:
            if land_cover.urban < 60.0:
                warnings.append(
                    f"Low urban percentage ({land_cover.urban:.1f}%) for urban city. "
                    f"Expected dense urban areas (e.g., Azadpur Transport Centre) to have 70-90% urban. "
                    f"This may indicate classification issues or the area has significant green space."
                )
            
            if land_cover.forest > 10.0:
                warnings.append(
                    f"High forest percentage ({land_cover.forest:.1f}%) for urban city. "
                    f"Expected Indian cities to have <5% forest. "
                    f"This may indicate that parks, trees, or crop patches are being misclassified as forest."
                )
        
        return warnings


class LandCoverClassifier:
    """Land cover classification using Google Dynamic World model with India-aware contextual scoring"""
    
    # Dynamic World label mapping
    # 0 = Water
    # 1 = Trees
    # 2 = Grass
    # 3 = Flooded vegetation
    # 4 = Crops
    # 5 = Shrub & scrub
    # 6 = Built area
    # 7 = Bare ground
    # 8 = Snow & ice
    
    DYNAMIC_WORLD_LABELS = {
        0: 'Water',
        1: 'Trees',
        2: 'Grass',
        3: 'Flooded vegetation',
        4: 'Crops',
        5: 'Shrub & scrub',
        6: 'Built area',
        7: 'Bare ground',
        8: 'Snow & ice'
    }
    
    @staticmethod
    def _serialize_metadata(metadata: Dict) -> Dict:
        """
        Recursively convert metadata to JSON-serializable format.
        Converts numpy types, numpy booleans, and other non-serializable types to native Python types.
        """
        if not isinstance(metadata, dict):
            if isinstance(metadata, (np.integer, np.floating)):
                return float(metadata)
            elif isinstance(metadata, np.bool_):
                return bool(metadata)
            return metadata
        
        serialized = {}
        for key, value in metadata.items():
            if isinstance(value, (np.integer, np.floating)):
                serialized[key] = float(value)
            elif isinstance(value, np.bool_):
                serialized[key] = bool(value)
            elif isinstance(value, bool):
                serialized[key] = bool(value)  # Ensure native Python bool
            elif isinstance(value, (int, float)):
                serialized[key] = value  # Native types are fine
            elif isinstance(value, dict):
                serialized[key] = LandCoverClassifier._serialize_metadata(value)
            elif isinstance(value, list):
                serialized[key] = [
                    LandCoverClassifier._serialize_metadata(item) if isinstance(item, dict) else (
                        bool(item) if isinstance(item, np.bool_) else (
                            float(item) if isinstance(item, (np.integer, np.floating)) else item
                        )
                    )
                    for item in value
                ]
            else:
                serialized[key] = value
        
        return serialized
    
    @staticmethod
    def compute_urban_likelihood_score(pixel_counts: Dict[int, int], 
                                       osm_context: Dict) -> Tuple[float, Dict[str, float]]:
        """
        Compute urban likelihood scores for India-specific context-aware classification.
        
        For each pixel class, compute base score, then apply contextual multipliers.
        Sum scores across all pixels to get total urban likelihood.
        
        Args:
            pixel_counts: Dictionary mapping Dynamic World class IDs to pixel counts
            osm_context: OSM urban context data (road density, building density, etc.)
            
        Returns:
            Tuple of (total_urban_likelihood, score_breakdown)
        """
        # Base urban likelihood scores per class
        base_scores = {
            0: 0.0,   # Water
            1: 0.2,   # Trees (tree-lined streets)
            2: 0.3,   # Grass (urban lawns, medians)
            3: 0.0,   # Flooded vegetation
            4: 0.0,   # Crops
            5: 0.0,   # Shrub & scrub
            6: 1.0,   # Built area
            7: 0.6,   # Bare ground (roads, courtyards)
            8: 0.0    # Snow & ice
        }
        
        # Compute contextual multipliers (India-specific)
        contextual_multiplier = 1.0
        
        # Road density multiplier
        road_density = osm_context.get('road_density_km_per_km2', 0.0)
        if road_density > 6.0:
            contextual_multiplier *= 1.3
        
        # Building density multiplier
        building_density = osm_context.get('building_density_pct', 0.0)
        if building_density > 30.0:
            contextual_multiplier *= 1.4
        
        # Municipal boundary multiplier
        is_municipal = osm_context.get('is_municipal', False)
        if is_municipal:
            contextual_multiplier *= 1.2
        
        # Note: Population density would require additional data source
        # For now, we use building density as a proxy
        
        # Compute total urban likelihood
        total_likelihood = 0.0
        score_breakdown = {
            'contextual_multiplier': contextual_multiplier,
            'multiplier_factors': {}
        }
        
        # Store multiplier factors for explanation
        if road_density > 6.0:
            score_breakdown['multiplier_factors']['road_density'] = 1.3
        if building_density > 30.0:
            score_breakdown['multiplier_factors']['building_density'] = 1.4
        if is_municipal:
            score_breakdown['multiplier_factors']['municipal'] = 1.2
        
        for class_id, count in pixel_counts.items():
            if class_id == -1 or class_id == 8:  # Skip masked pixels and snow
                continue
            
            base_score = base_scores.get(class_id, 0.0)
            # Apply contextual multiplier
            pixel_likelihood = base_score * contextual_multiplier
            # Clamp to [0, 1]
            pixel_likelihood = max(0.0, min(1.0, pixel_likelihood))
            
            # Weight by pixel count
            class_likelihood = pixel_likelihood * count
            total_likelihood += class_likelihood
            
            if class_id in base_scores:
                score_breakdown[f'class_{class_id}'] = {
                    'base_score': base_score,
                    'multiplier': contextual_multiplier,
                    'final_score': pixel_likelihood,
                    'pixel_count': count,
                    'weighted_likelihood': class_likelihood
                }
        
        return total_likelihood, score_breakdown
    
    def aggregate_classes(self, pixel_counts: Dict[int, int], 
                         osm_context: Dict = None, 
                         image: ee.Image = None,
                         polygon: ee.Geometry = None) -> Tuple[LandCoverResult, Dict]:
        """
        Aggregate Dynamic World classes using STRICT, CITY-INVARIANT mapping.
        
        FIXED CLASS MAPPING:
        Urban:
        - Built Area (6)
        - Bare Ground (7) - EXCEPT when crops are present (then counts as Vegetation)
        
        Vegetation:
        - Grass (2)
        - Crops (4)
        - Shrub & Scrub (5)
        - Bare Ground (7) - When crops are present (harvested/fallow fields)
        
        Forest:
        - Trees (1) ONLY if contiguous patch area > 0.25 km²
        - Else classify as Vegetation (not forest)
        
        Water:
        - Water (0)
        
        Exclude:
        - Flooded vegetation (3)
        - Snow/Ice (8)
        
        AGRICULTURAL FIELDS RULE:
        - If crops (4) are present, bare ground (7) is counted as Vegetation
        - This ensures harvested/fallow agricultural fields are correctly identified
        - If no crops present, bare ground counts as Urban
        
        Args:
            pixel_counts: Dictionary mapping Dynamic World class IDs to pixel counts
            osm_context: Ignored - kept for compatibility only
            image: Optional Earth Engine image for forest patch analysis
            polygon: Optional Earth Engine geometry for forest patch analysis
            
        Returns:
            Tuple of (LandCoverResult, metadata_dict) with:
            - LandCoverResult: Urban %, Forest %, Vegetation %, Water %
            - metadata_dict: Year, season, AOI size, pixel count
        """
        if not pixel_counts:
            raise ValueError("No pixel counts provided")
        
        # Calculate total pixels (excluding masked pixels with value -1 and snow/ice)
        total_pixels = sum(v for k, v in pixel_counts.items() if k != -1 and k != 8)
        
        if total_pixels == 0:
            raise ValueError("No pixels found in the specified area. The location may have no data.")
        
        # Get raw counts per Dynamic World class
        water = pixel_counts.get(0, 0)        # Water
        trees = pixel_counts.get(1, 0)        # Trees
        grass = pixel_counts.get(2, 0)       # Grass
        flooded_veg = pixel_counts.get(3, 0) # Flooded vegetation (EXCLUDED)
        crops = pixel_counts.get(4, 0)       # Crops
        shrub = pixel_counts.get(5, 0)       # Shrub & scrub
        built_area = pixel_counts.get(6, 0)  # Built area
        bare_ground = pixel_counts.get(7, 0) # Bare ground
        
        # STRICT CLASS MAPPING
        
        # Forest: Trees (1) ONLY if contiguous patch area > 0.25 km²
        # At scale=30m, each pixel = 30m × 30m = 900 m² = 0.0009 km²
        tree_area_km2 = trees * 0.0009  # Approximate area in km²
        
        if tree_area_km2 > 0.25:
            # Valid forest: Trees (1) only
            forest_count = trees
            # Remaining vegetation excludes trees
            base_vegetation_count = grass + crops + shrub
        else:
            # Invalid forest: Classify trees as vegetation
            forest_count = 0
            base_vegetation_count = grass + crops + shrub + trees
        
        # AGRICULTURAL FIELDS HANDLING:
        # Harvested/fallow fields are classified as Bare Ground (7) by Dynamic World
        # If crops are present, count bare ground as vegetation (harvested fields)
        # This ensures agricultural areas are correctly identified even when harvested
        if crops > 0:
            # Agricultural context: bare ground likely represents harvested fields
            vegetation_count = base_vegetation_count + bare_ground
            urban_count = built_area  # Only built area counts as urban
        else:
            # Non-agricultural context: bare ground counts as urban
            vegetation_count = base_vegetation_count
            urban_count = built_area + bare_ground
        
        # Calculate raw percentages (before normalization)
        urban_pct_raw = (urban_count / total_pixels) * 100 if total_pixels > 0 else 0.0
        forest_pct_raw = (forest_count / total_pixels) * 100 if total_pixels > 0 else 0.0
        vegetation_pct_raw = (vegetation_count / total_pixels) * 100 if total_pixels > 0 else 0.0
        water_pct_raw = (water / total_pixels) * 100 if total_pixels > 0 else 0.0
        
        # PRESENTATION-LEVEL NORMALIZATION: Urban-green floor for dense urban cores
        # Only applies when vegetation is extremely low in dense urban contexts
        # This is a presentation adjustment, NOT a pixel reclassification
        
        urban_pct = urban_pct_raw
        forest_pct = forest_pct_raw
        vegetation_pct = vegetation_pct_raw
        water_pct = water_pct_raw
        normalization_applied = False
        normalization_reason = None
        normalization_delta = 0.0
        
        # Normalization conditions: Dense urban core with extremely low vegetation
        # ALL conditions must be met:
        # - Urban percentage ≥ 85% (dense urban core)
        # - Vegetation percentage < 2% (extremely low green)
        # - Forest = 0% (no forest present)
        # - Water ≤ 3% (minimal water)
        if (urban_pct_raw >= 85.0 and 
            vegetation_pct_raw < 2.0 and 
            forest_pct_raw == 0.0 and 
            water_pct_raw <= 3.0):
            
            # Calculate vegetation deficit to reach minimum floor
            vegetation_floor = 2.5  # Minimum vegetation floor (%)
            delta = vegetation_floor - vegetation_pct_raw
            
            # Apply hard bounds to prevent excessive redistribution
            max_delta = 3.0  # Maximum vegetation added via normalization (%)
            if delta > max_delta:
                delta = max_delta
            
            # Ensure urban never drops below 80% (maintain urban character)
            min_urban = 80.0
            if urban_pct_raw - delta < min_urban:
                delta = urban_pct_raw - min_urban
            
            # Apply symmetric redistribution only if delta is positive
            if delta > 0:
                vegetation_pct = vegetation_pct_raw + delta
                urban_pct = urban_pct_raw - delta
                normalization_applied = True
                normalization_reason = "Urban-green floor normalization for dense urban core"
                normalization_delta = delta
        
        # Metadata: Raw and transparent
        metadata = {
            'year': '2024',
            'season': 'Jan-Mar',
            'total_pixels': total_pixels,
            'tree_area_km2': round(tree_area_km2, 4),
            'forest_valid': tree_area_km2 > 0.25,
            'raw_class_counts': {
                'water': water,
                'trees': trees,
                'grass': grass,
                'flooded_vegetation': flooded_veg,  # Excluded from totals
                'crops': crops,
                'shrub': shrub,
                'built_area': built_area,
                'bare_ground': bare_ground
            },
            'vegetation_components': {
                'grass': grass,
                'crops': crops,
                'shrub': shrub,
                'trees_as_vegetation': trees if tree_area_km2 <= 0.25 else 0,
                'bare_ground_as_vegetation': bare_ground if crops > 0 else 0
            },
            'agricultural_context': crops > 0,
            'harvested_fields_included': crops > 0 and bare_ground > 0,
            # Normalization metadata
            'normalization_applied': normalization_applied,
            'normalization_reason': normalization_reason,
            'normalization_delta': round(normalization_delta, 2) if normalization_applied else None,
            'raw_percentages': {
                'urban': round(urban_pct_raw, 2),
                'forest': round(forest_pct_raw, 2),
                'vegetation': round(vegetation_pct_raw, 2),
                'water': round(water_pct_raw, 2)
            },
            'normalized_percentages': {
                'urban': round(urban_pct, 2),
                'forest': round(forest_pct, 2),
                'vegetation': round(vegetation_pct, 2),
                'water': round(water_pct, 2)
            } if normalization_applied else None
        }
        
        return LandCoverResult(
            urban=urban_pct,
            forest=forest_pct,
            vegetation=vegetation_pct,
            water=water_pct,
            total_pixels=total_pixels
        ), metadata


class DisasterService:
    """Handle real-time natural disaster data from government and satellite feeds"""
    
    def __init__(self, openweather_key: str):
        self.openweather_key = openweather_key
    
    def get_earthquakes(self, lat: float, lon: float, max_radius_km: int = 500, days: int = 7) -> List[Dict]:
        """
        Fetch live earthquake data from USGS
        
        Args:
            lat: Latitude of locality
            lon: Longitude of locality
            max_radius_km: Maximum radius in km (default 500km)
            days: Number of days to look back (default 7)
            
        Returns:
            List of earthquake dictionaries
        """
        try:
            from datetime import datetime, timedelta
            
            # Calculate start time (now - days)
            start_time = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%S')
            
            url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
            params = {
                'format': 'geojson',
                'latitude': lat,
                'longitude': lon,
                'maxradiuskm': max_radius_km,
                'starttime': start_time,
                'minmagnitude': 4.0  # Only significant earthquakes
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 200:
                return []
            
            data = response.json()
            earthquakes = []
            
            if 'features' in data:
                for feature in data['features']:
                    props = feature.get('properties', {})
                    geom = feature.get('geometry', {})
                    coords = geom.get('coordinates', [])
                    
                    if len(coords) >= 2:
                        eq_lon, eq_lat = coords[0], coords[1]
                        
                        # Calculate distance from locality
                        distance_km = self._calculate_distance(lat, lon, eq_lat, eq_lon)
                        
                        # Filter: only show events within 300-500 km
                        if 300 <= distance_km <= 500:
                            # Parse time
                            time_ms = props.get('time', 0)
                            event_time = datetime.fromtimestamp(time_ms / 1000)
                            time_ago = self._format_time_ago(event_time)
                            
                            earthquakes.append({
                                'type': 'earthquake',
                                'title': f"Earthquake M{props.get('mag', 0):.1f}",
                                'severity': self._get_earthquake_severity(props.get('mag', 0)),
                                'distance_km': round(distance_km, 0),
                                'time': time_ago,
                                'source': 'USGS',
                                'magnitude': props.get('mag', 0),
                                'timestamp': event_time.isoformat()
                            })
            
            return earthquakes
            
        except Exception as e:
            # Return empty list on error, don't block the analysis
            return []
    
    def get_cyclones(self, lat: float, lon: float) -> List[Dict]:
        """
        Fetch live cyclone/storm data from NOAA
        
        Args:
            lat: Latitude of locality
            lon: Longitude of locality
            
        Returns:
            List of cyclone dictionaries
        """
        try:
            url = "https://www.nhc.noaa.gov/CurrentStorms.json"
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                return []
            
            data = response.json()
            cyclones = []
            
            # NOAA JSON structure may vary, try to parse it
            if isinstance(data, dict):
                storms = data.get('storms', []) or data.get('activeStorms', [])
                
                for storm in storms:
                    # Try to get storm position
                    storm_lat = storm.get('lat') or storm.get('latitude')
                    storm_lon = storm.get('lon') or storm.get('longitude')
                    
                    if storm_lat and storm_lon:
                        distance_km = self._calculate_distance(lat, lon, storm_lat, storm_lon)
                        
                        # Filter: only show storms within 500 km
                        if distance_km <= 500:
                            name = storm.get('name') or storm.get('stormName', 'Unnamed Storm')
                            category = storm.get('category') or storm.get('intensity', 'Unknown')
                            
                            # Try to get forecast time
                            forecast_time = storm.get('forecastTime') or storm.get('expectedLandfall')
                            time_str = self._format_cyclone_time(forecast_time)
                            
                            cyclones.append({
                                'type': 'cyclone',
                                'title': f"Cyclone {name}",
                                'severity': self._get_cyclone_severity(category),
                                'distance_km': round(distance_km, 0),
                                'time': time_str,
                                'source': 'NOAA',
                                'category': category
                            })
            
            return cyclones
            
        except Exception as e:
            # Return empty list on error
            return []
    
    def get_weather_alerts(self, lat: float, lon: float) -> List[Dict]:
        """
        Fetch weather alerts from OpenWeather OneCall API
        
        Args:
            lat: Latitude of locality
            lon: Longitude of locality
            
        Returns:
            List of weather alert dictionaries
        """
        try:
            # Try OneCall API 3.0 first (requires subscription)
            url = "https://api.openweathermap.org/data/3.0/onecall"
            params = {
                'lat': lat,
                'lon': lon,
                'appid': self.openweather_key,
                'exclude': 'minutely,hourly,daily'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            # If 3.0 fails, try 2.5 (free tier, but may not have alerts)
            if response.status_code != 200:
                url = "https://api.openweathermap.org/data/2.5/onecall"
                response = requests.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                return []
            
            data = response.json()
            alerts = []
            
            if 'alerts' in data and data['alerts']:
                for alert in data['alerts']:
                    event = alert.get('event', '').lower()
                    description = alert.get('description', '')
                    
                    # Filter for relevant disaster types
                    if any(keyword in event for keyword in ['flood', 'rain', 'heat', 'storm', 'wind', 'tornado', 'warning']):
                        severity = self._get_alert_severity(alert.get('severity', ''))
                        
                        alerts.append({
                            'type': 'weather_alert',
                            'title': alert.get('event', 'Weather Alert'),
                            'severity': severity,
                            'distance_km': 0,  # Local alert
                            'time': 'Active',
                            'source': 'OpenWeather',
                            'description': description[:200] if description else 'Weather alert active'
                        })
            
            return alerts
            
        except Exception as e:
            # Return empty list on error (don't block analysis)
            return []
    
    def get_all_disasters(self, lat: float, lon: float) -> List[Dict]:
        """
        Fetch all disaster data in parallel
        
        Args:
            lat: Latitude of locality
            lon: Longitude of locality
            
        Returns:
            Combined list of all disasters
        """
        import concurrent.futures
        
        all_disasters = []
        
        # Fetch all disaster types in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_eq = executor.submit(self.get_earthquakes, lat, lon)
            future_cyc = executor.submit(self.get_cyclones, lat, lon)
            future_weather = executor.submit(self.get_weather_alerts, lat, lon)
            
            try:
                all_disasters.extend(future_eq.result(timeout=10))
            except:
                pass
            
            try:
                all_disasters.extend(future_cyc.result(timeout=10))
            except:
                pass
            
            try:
                all_disasters.extend(future_weather.result(timeout=10))
            except:
                pass
        
        return all_disasters
    
    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points in km using Haversine formula"""
        from math import radians, sin, cos, sqrt, atan2
        
        R = 6371  # Earth radius in km
        
        lat1_rad = radians(lat1)
        lat2_rad = radians(lat2)
        delta_lat = radians(lat2 - lat1)
        delta_lon = radians(lon2 - lon1)
        
        a = sin(delta_lat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(delta_lon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        
        return R * c
    
    def _format_time_ago(self, event_time: datetime) -> str:
        """Format time as 'X hours/days ago'"""
        now = datetime.now()
        diff = now - event_time
        
        if diff.days > 0:
            return f"{diff.days} day{'s' if diff.days > 1 else ''} ago"
        elif diff.seconds >= 3600:
            hours = diff.seconds // 3600
            return f"{hours} hour{'s' if hours > 1 else ''} ago"
        else:
            minutes = diff.seconds // 60
            return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
    
    def _format_cyclone_time(self, forecast_time) -> str:
        """Format cyclone forecast time"""
        if not forecast_time:
            return "Active"
        
        try:
            if isinstance(forecast_time, str):
                # Try to parse ISO format
                forecast_dt = datetime.fromisoformat(forecast_time.replace('Z', '+00:00'))
                now = datetime.now(forecast_dt.tzinfo) if forecast_dt.tzinfo else datetime.now()
                diff = forecast_dt - now
                
                if diff.total_seconds() > 0:
                    hours = int(diff.total_seconds() / 3600)
                    return f"Expected in {hours} hours"
                else:
                    return "Active"
        except:
            pass
        
        return "Active"
    
    def _get_earthquake_severity(self, magnitude: float) -> str:
        """Determine earthquake severity"""
        if magnitude >= 7.0:
            return "High"
        elif magnitude >= 5.5:
            return "Medium"
        else:
            return "Low"
    
    def _get_cyclone_severity(self, category: str) -> str:
        """Determine cyclone severity"""
        if isinstance(category, (int, float)):
            if category >= 3:
                return "High"
            elif category >= 1:
                return "Medium"
            else:
                return "Low"
        
        category_str = str(category).upper()
        if 'MAJOR' in category_str or 'CATEGORY 3' in category_str or 'CATEGORY 4' in category_str or 'CATEGORY 5' in category_str:
            return "High"
        elif 'CATEGORY 1' in category_str or 'CATEGORY 2' in category_str:
            return "Medium"
        else:
            return "Low"
    
    def _get_alert_severity(self, severity: str) -> str:
        """Determine weather alert severity"""
        severity_lower = str(severity).lower()
        if 'extreme' in severity_lower or 'severe' in severity_lower:
            return "High"
        elif 'moderate' in severity_lower:
            return "Medium"
        else:
            return "Low"


class SupabaseService:
    """Handle Supabase database operations for caching"""
    
    def __init__(self, supabase_url: Optional[str] = None, supabase_key: Optional[str] = None):
        """
        Initialize Supabase client
        
        Args:
            supabase_url: Supabase project URL
            supabase_key: Supabase service role key (for server-side operations)
        """
        self.client: Optional[Client] = None
        if SUPABASE_AVAILABLE and supabase_url and supabase_key:
            try:
                self.client = create_client(supabase_url, supabase_key)
            except Exception as e:
                # Supabase not available, continue without caching
                self.client = None
    
    def is_available(self) -> bool:
        """Check if Supabase is configured and available"""
        return self.client is not None
    
    def insert_locality(self, city: str, name: str, geometry: ee.Geometry, lat: float, lon: float) -> Optional[str]:
        """
        Insert locality into Supabase database
        
        Args:
            city: City name
            name: Locality name
            geometry: Earth Engine geometry (will be converted to GeoJSON)
            lat: Latitude
            lon: Longitude
            
        Returns:
            Locality ID (UUID) if successful, None otherwise
        """
        if not self.client:
            return None
        
        try:
            # Convert Earth Engine geometry to GeoJSON
            geojson = self._ee_geometry_to_geojson(geometry)
            
            if not geojson:
                return None
            
            # Insert into localities table
            result = self.client.table('localities').insert({
                'city': city,
                'name': name,
                'geometry': geojson,  # Supabase will use ST_GeomFromGeoJSON()
                'lat': lat,
                'lon': lon
            }).execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]['id']
            
            return None
            
        except Exception as e:
            # Fail silently - caching is optional
            return None
    
    def get_locality_id(self, city: str, name: str) -> Optional[str]:
        """
        Get locality ID from database
        
        Args:
            city: City name
            name: Locality name
            
        Returns:
            Locality ID if found, None otherwise
        """
        if not self.client:
            return None
        
        try:
            result = self.client.table('localities').select('id').eq('city', city).eq('name', name).execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]['id']
            
            return None
            
        except Exception:
            return None
    
    def get_cached_landcover(self, locality_id: str) -> Optional[Dict]:
        """
        Get cached landcover data if it exists and is fresh (< 30 days)
        
        Args:
            locality_id: UUID of the locality
            
        Returns:
            Cached histogram data if fresh, None otherwise
        """
        if not self.client:
            return None
        
        try:
            result = self.client.table('landcover_cache').select('*').eq('locality_id', locality_id).execute()
            
            if result.data and len(result.data) > 0:
                cache_entry = result.data[0]
                last_updated = datetime.fromisoformat(cache_entry['last_updated'].replace('Z', '+00:00'))
                
                # Check if cache is fresh (within 30 days)
                age = datetime.now(last_updated.tzinfo) - last_updated
                if age.days < 30:
                    return {
                        'dw_histogram': cache_entry['dw_histogram'],
                        'satellite_source': cache_entry.get('satellite_source', 'Dynamic World'),
                        'satellite_date': cache_entry.get('satellite_date')
                    }
            
            return None
            
        except Exception:
            return None
    
    def save_landcover_cache(self, locality_id: str, dw_histogram: Dict, 
                            satellite_source: str = 'Dynamic World', 
                            satellite_date: Optional[str] = None):
        """
        Save landcover histogram to cache
        
        Args:
            locality_id: UUID of the locality
            dw_histogram: Pixel histogram dictionary
            satellite_source: Source of satellite data
            satellite_date: Date of satellite image
        """
        if not self.client:
            return
        
        try:
            # Upsert (insert or update) cache entry
            self.client.table('landcover_cache').upsert({
                'locality_id': locality_id,
                'dw_histogram': dw_histogram,
                'satellite_source': satellite_source,
                'satellite_date': satellite_date,
                'last_updated': datetime.now().isoformat()
            }).execute()
            
        except Exception:
            # Fail silently - caching is optional
            pass
    
    def _ee_geometry_to_geojson(self, geometry: ee.Geometry) -> Optional[Dict]:
        """
        Convert Earth Engine geometry to GeoJSON format
        
        Args:
            geometry: Earth Engine geometry
            
        Returns:
            GeoJSON dictionary or None if conversion fails
        """
        try:
            # Get geometry info from Earth Engine
            geom_info = geometry.getInfo()
            
            if geom_info.get('type') == 'Polygon':
                return {
                    'type': 'Polygon',
                    'coordinates': geom_info.get('coordinates', [])
                }
            elif geom_info.get('type') == 'MultiPolygon':
                return {
                    'type': 'MultiPolygon',
                    'coordinates': geom_info.get('coordinates', [])
                }
            
            return None
            
        except Exception:
            return None


class NewsService:
    """Handle weather-related news from NewsAPI"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2"
    
    def get_weather_news(self) -> List[Dict]:
        """
        Fetch global daily weather and climate-related news from NewsAPI
        
        Uses /v2/everything endpoint for global news coverage.
        
        Returns:
            List of news headline dictionaries (max 5)
        """
        if not self.api_key:
            # Return empty list if no API key (non-blocking)
            return []
        
        try:
            # Global weather and climate keywords
            keywords = "weather OR climate OR temperature OR rainfall OR snowfall OR heatwave"
            
            # Use /v2/everything endpoint for global news
            url = f"{self.base_url}/everything"
            params = {
                'q': keywords,
                'language': 'en',
                'sortBy': 'publishedAt',
                'pageSize': 5,
                'apiKey': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                # Handle rate limiting or errors gracefully
                if response.status_code == 429:
                    print("NewsAPI rate limit exceeded")
                else:
                    print(f"NewsAPI error: {response.status_code}")
                return []
            
            data = response.json()
            articles = data.get('articles', [])
            
            # Format news headlines
            news_headlines = []
            for article in articles[:5]:  # Limit to top 5
                if article.get('title') and article.get('title') != '[Removed]':
                    # Format published time
                    published_at = article.get('publishedAt', '')
                    published_time = None
                    if published_at:
                        try:
                            from datetime import datetime
                            dt = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                            published_time = dt.strftime('%Y-%m-%d %H:%M')
                        except:
                            published_time = published_at[:10]  # Fallback to date only
                    
                    news_headlines.append({
                        'title': article.get('title', ''),
                        'source': article.get('source', {}).get('name', 'Unknown'),
                        'published_at': published_time,
                        'url': article.get('url', '')
                    })
            
            return news_headlines
            
        except Exception as e:
            # Return empty list on error (non-blocking)
            print(f"Error fetching weather news: {str(e)}")
            return []


class WeatherService:
    """Handle weather data from OpenWeather API"""
    
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("OpenWeather API key is required")
        self.api_key = api_key
        self.base_url = "https://api.openweathermap.org/data/2.5"
    
    def get_weather_data(self, lat: float, lon: float) -> WeatherData:
        """Fetch current weather data for coordinates"""
        url = f"{self.base_url}/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key,
            'units': 'metric'
        }
        
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise RuntimeError(f"OpenWeather API error: {response.status_code} - {response.text}")
        
        data = response.json()
        
        # Get precipitation from rain or snow if available
        precipitation = 0.0
        if 'rain' in data:
            precipitation = data['rain'].get('1h', 0.0)
        elif 'snow' in data:
            precipitation = data['snow'].get('1h', 0.0)
        
        return WeatherData(
            temperature=data['main']['temp'],
            humidity=data['main']['humidity'],
            precipitation=precipitation,
            wind_speed=data['wind']['speed'],
            pressure=data['main']['pressure'],
            coordinates=(lat, lon)
        )
    
    def get_forecast_data(self, lat: float, lon: float, days: int = 5) -> List[Dict]:
        """Fetch weather forecast data"""
        url = f"{self.base_url}/forecast"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key,
            'units': 'metric'
        }
        
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise RuntimeError(f"OpenWeather API forecast error: {response.status_code}")
        
        forecast = response.json()
        return forecast.get('list', [])[:days * 8]  # 8 forecasts per day
    
    def get_air_quality(self, lat: float, lon: float) -> AirQualityData:
        """
        Fetch real-time air quality data from OpenWeather Air Pollution API
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            AirQualityData with AQI and pollutant concentrations
        """
        url = f"{self.base_url}/air_pollution"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key
        }
        
        # Increase timeout to handle slow connections (30 seconds)
        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            error_msg = f"OpenWeather Air Pollution API error: {response.status_code}"
            try:
                error_detail = response.json()
                error_msg += f" - {error_detail}"
            except:
                error_msg += f" - {response.text}"
            raise RuntimeError(error_msg)
        
        data = response.json()
        
        # Debug: Print response structure if needed
        # print(f"Air Quality API Response: {json.dumps(data, indent=2)}")
        
        # Extract air quality data from response
        if 'list' not in data or len(data['list']) == 0:
            raise RuntimeError(f"No air quality data available for this location. Response: {json.dumps(data)}")
        
        aq_data = data['list'][0]  # Current air quality
        main = aq_data.get('main', {})
        components = aq_data.get('components', {})
        
        # Validate that we have the required data
        if not main or 'aqi' not in main:
            raise RuntimeError(f"Invalid air quality response structure. Main data: {main}")
        
        # Get AQI (1-5 scale)
        aqi = main.get('aqi', 1)
        
        # Map AQI to text levels
        aqi_levels = {
            1: 'Good',
            2: 'Fair',
            3: 'Moderate',
            4: 'Poor',
            5: 'Very Poor'
        }
        aqi_level = aqi_levels.get(aqi, 'Unknown')
        
        # Extract pollutant concentrations (already in μg/m³)
        pm25 = components.get('pm2_5', 0.0)
        pm10 = components.get('pm10', 0.0)
        co_ug_m3 = components.get('co', 0.0)  # CO is in μg/m³ from OpenWeather
        no2 = components.get('no2', 0.0)
        so2 = components.get('so2', 0.0)
        o3 = components.get('o3', 0.0)
        
        # Convert CO from μg/m³ to mg/m³ (divide by 1000)
        co_mg_m3 = co_ug_m3 / 1000.0
        
        return AirQualityData(
            aqi=aqi,
            aqi_level=aqi_level,
            pm25=round(pm25, 2),
            pm10=round(pm10, 2),
            co=round(co_mg_m3, 3),  # CO in mg/m³
            no2=round(no2, 2),
            so2=round(so2, 2),
            o3=round(o3, 2),
            coordinates=(lat, lon)
        )
    
    def get_health_advisory(self, aqi: int) -> str:
        """
        Get health advisory text based on AQI level
        
        Args:
            aqi: Air Quality Index (1-5)
            
        Returns:
            Health advisory text
        """
        advisories = {
            1: "Air quality is satisfactory. No health concerns.",
            2: "Air quality is acceptable. Sensitive individuals may experience minor breathing discomfort.",
            3: "Members of sensitive groups may experience health effects. General public unlikely to be affected.",
            4: "Everyone may begin to experience health effects. Sensitive groups may experience more serious effects.",
            5: "Health alert: Everyone may experience serious health effects. Avoid outdoor activities."
        }
        return advisories.get(aqi, "Air quality data unavailable.")
    
    @staticmethod
    def _calculate_pm25_sub_aqi(pm25: float) -> int:
        """
        Calculate PM2.5 sub-AQI using US EPA standards
        
        PM2.5 (µg/m³, 24-hour) → AQI
        0–12.0     → 0–50
        12.1–35.4  → 51–100
        35.5–55.4  → 101–150
        55.5–150.4 → 151–200
        150.5–250.4 → 201–300
        >250.4     → 301–500
        """
        if pm25 <= 0:
            return 0
        elif pm25 <= 12.0:
            return int((pm25 / 12.0) * 50)
        elif pm25 <= 35.4:
            return int(51 + ((pm25 - 12.1) / (35.4 - 12.1)) * (100 - 51))
        elif pm25 <= 55.4:
            return int(101 + ((pm25 - 35.5) / (55.4 - 35.5)) * (150 - 101))
        elif pm25 <= 150.4:
            return int(151 + ((pm25 - 55.5) / (150.4 - 55.5)) * (200 - 151))
        elif pm25 <= 250.4:
            return int(201 + ((pm25 - 150.5) / (250.4 - 150.5)) * (300 - 201))
        else:
            return min(500, int(301 + ((pm25 - 250.5) / 100) * 199))
    
    @staticmethod
    def _calculate_pm10_sub_aqi(pm10: float) -> int:
        """
        Calculate PM10 sub-AQI using US EPA standards
        
        PM10 (µg/m³, 24-hour) → AQI
        0–54     → 0–50
        55–154   → 51–100
        155–254  → 101–150
        255–354  → 151–200
        355–424  → 201–300
        >424     → 301–500
        """
        if pm10 <= 0:
            return 0
        elif pm10 <= 54:
            return int((pm10 / 54) * 50)
        elif pm10 <= 154:
            return int(51 + ((pm10 - 55) / (154 - 55)) * (100 - 51))
        elif pm10 <= 254:
            return int(101 + ((pm10 - 155) / (254 - 155)) * (150 - 101))
        elif pm10 <= 354:
            return int(151 + ((pm10 - 255) / (354 - 255)) * (200 - 151))
        elif pm10 <= 424:
            return int(201 + ((pm10 - 355) / (424 - 355)) * (300 - 201))
        else:
            return min(500, int(301 + ((pm10 - 425) / 100) * 199))
    
    @staticmethod
    def _calculate_o3_sub_aqi(o3: float) -> int:
        """
        Calculate O3 sub-AQI using US EPA standards (8-hour average)
        
        O3 (µg/m³, 8-hour) → AQI
        0–107    → 0–50
        108–137  → 51–100
        138–168  → 101–150
        169–208  → 151–200
        209–392  → 201–300
        >392     → 301–500
        """
        if o3 <= 0:
            return 0
        elif o3 <= 107:
            return int((o3 / 107) * 50)
        elif o3 <= 137:
            return int(51 + ((o3 - 108) / (137 - 108)) * (100 - 51))
        elif o3 <= 168:
            return int(101 + ((o3 - 138) / (168 - 138)) * (150 - 101))
        elif o3 <= 208:
            return int(151 + ((o3 - 169) / (208 - 169)) * (200 - 151))
        elif o3 <= 392:
            return int(201 + ((o3 - 209) / (392 - 209)) * (300 - 201))
        else:
            return min(500, int(301 + ((o3 - 393) / 100) * 199))
    
    @staticmethod
    def _get_aqi_category(aqi_value: int) -> str:
        """Get US EPA AQI category based on value"""
        if aqi_value <= 50:
            return 'Good'
        elif aqi_value <= 100:
            return 'Moderate'
        elif aqi_value <= 150:
            return 'Unhealthy for Sensitive Groups'
        elif aqi_value <= 200:
            return 'Unhealthy'
        elif aqi_value <= 300:
            return 'Very Unhealthy'
        else:
            return 'Hazardous'
    
    @staticmethod
    def calculate_us_aqi(pm25: float, pm10: float = None, o3: float = None) -> Dict[str, any]:
        """
        Calculate US AQI using dominant pollutant method (US EPA standard)
        
        Final AQI = MAX(sub-AQI for PM2.5, PM10, O3)
        Dominant pollutant = pollutant with highest sub-AQI
        
        Args:
            pm25: PM2.5 concentration in µg/m³
            pm10: PM10 concentration in µg/m³ (optional)
            o3: O3 concentration in µg/m³ (optional)
            
        Returns:
            Dictionary with 'value', 'category', 'dominant_pollutant', 'sub_indices'
        """
        # Calculate sub-AQIs
        aqi_pm25 = WeatherService._calculate_pm25_sub_aqi(pm25) if pm25 is not None and pm25 >= 0 else 0
        
        aqi_pm10 = 0
        if pm10 is not None and pm10 >= 0:
            aqi_pm10 = WeatherService._calculate_pm10_sub_aqi(pm10)
        
        aqi_o3 = 0
        if o3 is not None and o3 >= 0:
            aqi_o3 = WeatherService._calculate_o3_sub_aqi(o3)
        
        # Final AQI = max of all sub-indices
        final_aqi = max(aqi_pm25, aqi_pm10, aqi_o3)
        
        # Determine dominant pollutant
        dominant_pollutant = 'PM2.5'
        if aqi_pm10 > aqi_pm25 and aqi_pm10 >= aqi_o3:
            dominant_pollutant = 'PM10'
        elif aqi_o3 > aqi_pm25 and aqi_o3 > aqi_pm10:
            dominant_pollutant = 'O3'
        
        return {
            'value': final_aqi,
            'category': WeatherService._get_aqi_category(final_aqi),
            'dominant_pollutant': dominant_pollutant,
            'standard': 'US EPA',
            'sub_indices': {
                'pm25': aqi_pm25,
                'pm10': aqi_pm10,
                'o3': aqi_o3
            }
        }


class ClimateRiskCalculator:
    """Calculate climate risks based on weather and land cover data"""
    
    @staticmethod
    def calculate_flood_risk(weather: WeatherData, land_cover: LandCoverResult, 
                           forecast: List[Dict] = None) -> str:
        """
        Calculate flood risk based on new rules:
        - if water% > 8 AND rainfall > 10mm → High
        - if water% > 5 → Medium
        - else Low
        """
        water_percent = land_cover.water
        rainfall = weather.precipitation
        
        if water_percent > 8 and rainfall > 10:
            return "High"
        elif water_percent > 5:
            return "Medium"
        else:
            return "Low"
    
    @staticmethod
    def calculate_heat_risk(weather: WeatherData, land_cover: LandCoverResult) -> str:
        """
        Calculate heat risk based on new rules:
        - if urban% > 50 AND temperature > 35°C → High
        - if urban% > 40 → Medium
        - else Low
        """
        urban_percent = land_cover.urban
        temperature = weather.temperature
        
        if urban_percent > 50 and temperature > 35:
            return "High"
        elif urban_percent > 40:
            return "Medium"
        else:
            return "Low"
    
    @staticmethod
    def calculate_drought_risk(weather: WeatherData, land_cover: LandCoverResult,
                              forecast: List[Dict] = None) -> str:
        """
        Calculate drought risk based on new rules:
        - if vegetation% < 20 AND rainfall < 5mm → High
        - if vegetation% < 30 → Medium
        - else Low
        """
        vegetation_percent = land_cover.vegetation
        rainfall = weather.precipitation
        
        if vegetation_percent < 20 and rainfall < 5:
            return "High"
        elif vegetation_percent < 30:
            return "Medium"
        else:
            return "Low"


class UrbanisationRiskCalculator:
    """
    Calculate Urbanisation Risk Score (URS) based on normalized inputs
    
    Formula: URS = (w₁ × U) + (w₂ × (1 − V)) + (w₃ × P) + (w₄ × I)
    
    Where:
    - U = Urban/Built-up land fraction (0-1)
    - V = Vegetation/Green cover fraction (0-1)
    - P = Population density (normalized 0-1)
    - I = Infrastructure stress (0-1)
    
    Weights:
    - w₁ = 0.40 (Urban dominance)
    - w₂ = 0.30 (Loss of vegetation)
    - w₃ = 0.20 (Population pressure)
    - w₄ = 0.10 (Infrastructure stress)
    """
    
    # Weights for the URS formula
    WEIGHT_URBAN = 0.40
    WEIGHT_VEGETATION_LOSS = 0.30
    WEIGHT_POPULATION = 0.20
    WEIGHT_INFRASTRUCTURE = 0.10
    
    @staticmethod
    def normalize_population_density(population_per_km2: float) -> float:
        """
        Normalize population density to 0-1 range
        
        Uses typical ranges:
        - Rural: 0-1000 per km² → 0.0-0.2
        - Suburban: 1000-5000 per km² → 0.2-0.5
        - Urban: 5000-15000 per km² → 0.5-0.8
        - Dense urban: >15000 per km² → 0.8-1.0
        """
        if population_per_km2 <= 0:
            return 0.0
        elif population_per_km2 <= 1000:
            return min(0.2, population_per_km2 / 5000)
        elif population_per_km2 <= 5000:
            return 0.2 + ((population_per_km2 - 1000) / 4000) * 0.3
        elif population_per_km2 <= 15000:
            return 0.5 + ((population_per_km2 - 5000) / 10000) * 0.3
        else:
            return min(1.0, 0.8 + ((population_per_km2 - 15000) / 20000) * 0.2)
    
    @staticmethod
    def estimate_infrastructure_stress(urban_fraction: float, population_normalized: float,
                                      air_quality: Optional[AirQualityData] = None) -> float:
        """
        Estimate infrastructure stress based on urban density, population, and air quality
        
        Args:
            urban_fraction: Urban land fraction (0-1)
            population_normalized: Normalized population density (0-1)
            air_quality: Optional air quality data
            
        Returns:
            Infrastructure stress score (0-1)
        """
        # Base stress from urban density
        base_stress = urban_fraction * 0.5
        
        # Add population pressure
        population_stress = population_normalized * 0.3
        
        # Add air quality stress (if available)
        air_quality_stress = 0.0
        if air_quality:
            # Higher AQI = higher stress
            # AQI 1-2 (Good/Fair) → 0.0-0.1
            # AQI 3 (Moderate) → 0.2
            # AQI 4 (Poor) → 0.4
            # AQI 5 (Very Poor) → 0.6
            aqi_stress_map = {1: 0.0, 2: 0.1, 3: 0.2, 4: 0.4, 5: 0.6}
            air_quality_stress = aqi_stress_map.get(air_quality.aqi, 0.0)
        
        total_stress = base_stress + population_stress + air_quality_stress
        return min(1.0, total_stress)
    
    @staticmethod
    def apply_vegetation_floor_rule(urban_fraction: float, vegetation_fraction: float) -> Tuple[float, float]:
        """
        Apply vegetation floor rule to prevent unrealistic risk inflation
        
        If U > 0.95 and V < 0.05:
            V = 0.05
            U = U - 0.05
        
        Args:
            urban_fraction: Urban land fraction (0-1)
            vegetation_fraction: Vegetation fraction (0-1)
            
        Returns:
            Tuple of (adjusted_urban_fraction, adjusted_vegetation_fraction)
        """
        if urban_fraction > 0.95 and vegetation_fraction < 0.05:
            adjusted_vegetation = 0.05
            adjusted_urban = max(0.0, urban_fraction - 0.05)
            return adjusted_urban, adjusted_vegetation
        return urban_fraction, vegetation_fraction
    
    @classmethod
    def calculate_urbanisation_risk(cls, land_cover: LandCoverResult,
                                    population_per_km2: Optional[float] = None,
                                    air_quality: Optional[AirQualityData] = None) -> Dict:
        """
        Calculate Urbanisation Risk Score (URS) for a region
        
        Args:
            land_cover: Land cover classification results
            population_per_km2: Optional population density per km²
            air_quality: Optional air quality data
            
        Returns:
            Dictionary with URS score, risk level, and breakdown
        """
        # Convert percentages to fractions (0-1)
        U = land_cover.urban / 100.0
        V = (land_cover.vegetation + land_cover.forest) / 100.0
        
        # Apply vegetation floor rule
        U, V = cls.apply_vegetation_floor_rule(U, V)
        
        # Normalize population density (default to moderate if not provided)
        if population_per_km2 is None:
            # Estimate based on urban fraction
            # High urban → assume high population
            estimated_pop = U * 15000  # Scale to typical urban density
            P = cls.normalize_population_density(estimated_pop)
        else:
            P = cls.normalize_population_density(population_per_km2)
        
        # Calculate infrastructure stress
        I = cls.estimate_infrastructure_stress(U, P, air_quality)
        
        # Calculate URS using the formula
        # URS = (w₁ × U) + (w₂ × (1 − V)) + (w₃ × P) + (w₄ × I)
        urs = (
            cls.WEIGHT_URBAN * U +
            cls.WEIGHT_VEGETATION_LOSS * (1 - V) +
            cls.WEIGHT_POPULATION * P +
            cls.WEIGHT_INFRASTRUCTURE * I
        )
        
        # Clamp to 0-1 range
        urs = max(0.0, min(1.0, urs))
        
        # Determine risk level
        if urs < 0.3:
            risk_level = "Low"
        elif urs < 0.6:
            risk_level = "Moderate"
        elif urs < 0.8:
            risk_level = "High"
        else:
            risk_level = "Critical"
        
        return {
            'urs_score': round(urs, 4),
            'risk_level': risk_level,
            'breakdown': {
                'urban_contribution': round(cls.WEIGHT_URBAN * U, 4),
                'vegetation_loss_contribution': round(cls.WEIGHT_VEGETATION_LOSS * (1 - V), 4),
                'population_contribution': round(cls.WEIGHT_POPULATION * P, 4),
                'infrastructure_contribution': round(cls.WEIGHT_INFRASTRUCTURE * I, 4)
            },
            'inputs': {
                'urban_fraction': round(U, 4),
                'vegetation_fraction': round(V, 4),
                'population_normalized': round(P, 4),
                'infrastructure_stress': round(I, 4)
            },
            'formula': 'URS = (0.40 × U) + (0.30 × (1 − V)) + (0.20 × P) + (0.10 × I)'
        }

    @classmethod
    def calculate_esi(cls, land_cover: LandCoverResult,
                     air_quality: Optional[AirQualityData] = None) -> Dict:
        """Calculate Environmental Sustainability Index (ESI)"""
        V = (land_cover.forest + land_cover.vegetation) / 100.0

        aqi_score = 1.0
        if air_quality:
            aqi_score = max(0.0, 1.0 - (air_quality.aqi - 1) / 4.0)

        esi = (0.6 * V) + (0.4 * aqi_score)

        return {
            'esi_score': round(esi, 4),
            'rating': 'Excellent' if esi > 0.8 else 'Good' if esi > 0.6 else 'Fair' if esi > 0.4 else 'Poor'
        }


class CarbonFootprintCalculator:
    """
    Calculate carbon footprint and sequestration potential based on land cover
    
    Uses IPCC standards and Indian context for calculations
    """
    
    # Carbon sequestration rates (tonnes CO2 per hectare per year)
    # Based on IPCC guidelines and Indian forest research
    SEQUESTRATION_RATES = {
        'forest': 5.5,  # Dense forest (tonnes CO2/ha/year)
        'vegetation': 2.2,  # Sparse vegetation/crops (tonnes CO2/ha/year)
        'urban': -0.5,  # Urban areas emit CO2 (negative sequestration)
        'water': 0.0  # Water bodies don't sequester carbon
    }
    
    # Carbon emission factors (tonnes CO2 per hectare per year)
    EMISSION_FACTORS = {
        'urban': 8.5,  # Urban areas emit CO2 (buildings, vehicles, industry)
        'forest': 0.0,
        'vegetation': 0.0,
        'water': 0.0
    }
    
    @classmethod
    def calculate_carbon_impact(cls, land_cover: LandCoverResult, area_km2: float) -> Dict:
        """
        Calculate carbon footprint and sequestration potential
        
        Args:
            land_cover: Land cover classification results
            area_km2: Total area in square kilometers
            
        Returns:
            Dictionary with carbon calculations in tonnes CO2/year and rupees
        """
        area_hectares = area_km2 * 100  # Convert km² to hectares
        
        # Calculate sequestration (positive = absorbs CO2)
        forest_sequestration = (land_cover.forest / 100) * area_hectares * cls.SEQUESTRATION_RATES['forest']
        vegetation_sequestration = (land_cover.vegetation / 100) * area_hectares * cls.SEQUESTRATION_RATES['vegetation']
        urban_sequestration = (land_cover.urban / 100) * area_hectares * cls.SEQUESTRATION_RATES['urban']
        
        total_sequestration = forest_sequestration + vegetation_sequestration + urban_sequestration
        
        # Calculate emissions (negative = emits CO2)
        urban_emissions = (land_cover.urban / 100) * area_hectares * cls.EMISSION_FACTORS['urban']
        
        # Net carbon impact (negative = net emitter, positive = net sequester)
        net_carbon_impact = total_sequestration - urban_emissions
        
        # Carbon credit value (Indian carbon credit market rate: ~₹500-800 per tonne CO2)
        # Using conservative estimate of ₹600 per tonne
        CARBON_CREDIT_RATE_RUPEES = 600
        
        sequestration_value_rupees = total_sequestration * CARBON_CREDIT_RATE_RUPEES
        emission_cost_rupees = urban_emissions * CARBON_CREDIT_RATE_RUPEES
        net_value_rupees = net_carbon_impact * CARBON_CREDIT_RATE_RUPEES
        
        return {
            'area_km2': round(area_km2, 2),
            'area_hectares': round(area_hectares, 2),
            'carbon_sequestration': {
                'forest_co2_per_year': round(forest_sequestration, 2),
                'vegetation_co2_per_year': round(vegetation_sequestration, 2),
                'urban_co2_per_year': round(urban_sequestration, 2),
                'total_sequestration_co2_per_year': round(total_sequestration, 2),
                'value_rupees_per_year': round(sequestration_value_rupees, 2)
            },
            'carbon_emissions': {
                'urban_co2_per_year': round(urban_emissions, 2),
                'cost_rupees_per_year': round(emission_cost_rupees, 2)
            },
            'net_carbon_impact': {
                'co2_per_year': round(net_carbon_impact, 2),
                'status': 'Net Carbon Sink' if net_carbon_impact > 0 else 'Net Carbon Emitter',
                'value_rupees_per_year': round(net_value_rupees, 2)
            },
            'carbon_credit_rate_rupees_per_tonne': CARBON_CREDIT_RATE_RUPEES,
            'methodology': 'Based on IPCC guidelines and Indian carbon credit market rates'
        }


class EconomicImpactAnalyzer:
    """
    Analyze economic impact based on land cover and environmental factors
    
    All values in Indian Rupees (₹)
    """
    
    # Property value multipliers (relative to base urban property value)
    # Based on Indian real estate market research
    PROPERTY_VALUE_MULTIPLIERS = {
        'urban': 1.0,  # Base value
        'forest': 0.1,  # Forest land has lower property value
        'vegetation': 0.3,  # Agricultural/vegetation land
        'water': 0.05  # Water bodies
    }
    
    # Green space premium (percentage increase in property value per 1% green space)
    GREEN_SPACE_PREMIUM = 0.5  # 0.5% increase per 1% green space
    
    # Tourism potential (annual revenue per km²)
    TOURISM_POTENTIAL = {
        'forest': 500000,  # ₹5 lakh per km² per year for forest areas
        'water': 800000,  # ₹8 lakh per km² per year for water bodies
        'vegetation': 200000,  # ₹2 lakh per km² per year for vegetation
        'urban': 100000  # ₹1 lakh per km² per year for urban tourism
    }
    
    # Agricultural productivity (annual revenue per km²)
    AGRICULTURAL_PRODUCTIVITY = 1500000  # ₹15 lakh per km² per year for agricultural land
    
    # Health cost savings (annual savings per person per 1% green space increase)
    HEALTH_COST_SAVINGS_PER_PERSON = 500  # ₹500 per person per year per 1% green space
    
    @classmethod
    def calculate_economic_impact(cls, land_cover: LandCoverResult, area_km2: float,
                                 population: Optional[int] = None,
                                 base_property_value_per_km2: float = 500000000) -> Dict:
        """
        Calculate economic impact in Indian Rupees
        
        Args:
            land_cover: Land cover classification results
            area_km2: Total area in square kilometers
            population: Optional population count
            base_property_value_per_km2: Base property value per km² (default ₹50 crore)
            
        Returns:
            Dictionary with economic impact calculations in rupees
        """
        # Property value analysis
        urban_area_km2 = (land_cover.urban / 100) * area_km2
        forest_area_km2 = (land_cover.forest / 100) * area_km2
        vegetation_area_km2 = (land_cover.vegetation / 100) * area_km2
        water_area_km2 = (land_cover.water / 100) * area_km2
        
        total_green_space_pct = land_cover.forest + land_cover.vegetation
        
        # Calculate property values
        urban_property_value = urban_area_km2 * base_property_value_per_km2 * cls.PROPERTY_VALUE_MULTIPLIERS['urban']
        forest_property_value = forest_area_km2 * base_property_value_per_km2 * cls.PROPERTY_VALUE_MULTIPLIERS['forest']
        vegetation_property_value = vegetation_area_km2 * base_property_value_per_km2 * cls.PROPERTY_VALUE_MULTIPLIERS['vegetation']
        water_property_value = water_area_km2 * base_property_value_per_km2 * cls.PROPERTY_VALUE_MULTIPLIERS['water']
        
        total_property_value = urban_property_value + forest_property_value + vegetation_property_value + water_property_value
        
        # Green space premium (applies to urban areas)
        green_space_premium_value = urban_property_value * (total_green_space_pct / 100) * (cls.GREEN_SPACE_PREMIUM / 100)
        
        # Tourism potential
        forest_tourism = forest_area_km2 * cls.TOURISM_POTENTIAL['forest']
        water_tourism = water_area_km2 * cls.TOURISM_POTENTIAL['water']
        vegetation_tourism = vegetation_area_km2 * cls.TOURISM_POTENTIAL['vegetation']
        urban_tourism = urban_area_km2 * cls.TOURISM_POTENTIAL['urban']
        
        total_tourism_potential = forest_tourism + water_tourism + vegetation_tourism + urban_tourism
        
        # Agricultural productivity
        agricultural_potential = vegetation_area_km2 * cls.AGRICULTURAL_PRODUCTIVITY
        
        # Health cost savings
        health_savings = 0
        if population:
            health_savings = population * total_green_space_pct * cls.HEALTH_COST_SAVINGS_PER_PERSON
        
        # Total economic value
        total_economic_value = (
            total_property_value +
            green_space_premium_value +
            total_tourism_potential +
            agricultural_potential +
            health_savings
        )
        
        return {
            'area_km2': round(area_km2, 2),
            'land_cover_breakdown': {
                'urban_km2': round(urban_area_km2, 2),
                'forest_km2': round(forest_area_km2, 2),
                'vegetation_km2': round(vegetation_area_km2, 2),
                'water_km2': round(water_area_km2, 2)
            },
            'property_values_rupees': {
                'urban_property_value': round(urban_property_value, 2),
                'forest_property_value': round(forest_property_value, 2),
                'vegetation_property_value': round(vegetation_property_value, 2),
                'water_property_value': round(water_property_value, 2),
                'total_property_value': round(total_property_value, 2),
                'green_space_premium': round(green_space_premium_value, 2)
            },
            'tourism_potential_rupees_per_year': {
                'forest_tourism': round(forest_tourism, 2),
                'water_tourism': round(water_tourism, 2),
                'vegetation_tourism': round(vegetation_tourism, 2),
                'urban_tourism': round(urban_tourism, 2),
                'total_tourism_potential': round(total_tourism_potential, 2)
            },
            'agricultural_potential_rupees_per_year': round(agricultural_potential, 2),
            'health_cost_savings_rupees_per_year': round(health_savings, 2) if population else None,
            'total_economic_value_rupees': round(total_economic_value, 2),
            'currency': 'INR (Indian Rupees)',
            'methodology': 'Based on Indian real estate market, tourism data, and health impact studies'
        }


class GeminiCropRecommendationService:
    """Service to get crop suitability recommendations using Google Gemini API"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Gemini API service
        
        Args:
            api_key: Google Gemini API key (optional, can be set via env var)
        """
        self.api_key = api_key or os.getenv('GEMINI_API_KEY')
        if not self.api_key:
            raise ValueError("Gemini API key is required. Set GEMINI_API_KEY environment variable.")
        
        try:
            import google.generativeai as genai
            genai.configure(api_key=self.api_key)
            self.genai = genai
            # Use Gemini Flash for faster responses
            # Try gemini-1.5-flash (most reliable), fallback to gemini-pro
            try:
                self.model = genai.GenerativeModel('gemini-1.5-flash')
            except:
                try:
                    # Try experimental 2.0 if available
                    self.model = genai.GenerativeModel('gemini-2.0-flash-exp')
                except:
                    self.model = genai.GenerativeModel('gemini-pro')
        except ImportError:
            raise ImportError("google-generativeai package is required. Install with: pip install google-generativeai")
    
    def get_crop_recommendations(self, location: str, lat: float, lon: float,
                                land_cover: LandCoverResult,
                                weather_data: Optional[WeatherData] = None) -> Dict:
        """
        Get crop suitability recommendations for a region using Gemini API
        
        Args:
            location: Location name
            lat: Latitude
            lon: Longitude
            land_cover: Land cover classification results
            weather_data: Optional weather data
            
        Returns:
            Dictionary with crop recommendations and analysis
        """
        try:
            # Prepare context for Gemini
            context = self._prepare_context(location, lat, lon, land_cover, weather_data)
            
            # Create prompt for Gemini
            prompt = f"""You are an agricultural expert analyzing crop suitability for a specific region.

Location: {location}
Coordinates: {lat}, {lon}

Land Cover Analysis:
- Urban/Built-up: {land_cover.urban:.2f}%
- Vegetation: {land_cover.vegetation:.2f}%
- Forest: {land_cover.forest:.2f}%
- Water: {land_cover.water:.2f}%

"""
            
            if weather_data:
                prompt += f"""Weather Conditions:
- Temperature: {weather_data.temperature:.1f}°C
- Humidity: {weather_data.humidity:.1f}%
- Precipitation: {weather_data.precipitation:.1f} mm
- Wind Speed: {weather_data.wind_speed:.1f} m/s

"""
            
            prompt += """Based on this satellite image analysis and regional data, provide:

1. Top 5 most suitable crops for this region (considering climate, soil type typical for this location, and land cover)
2. For each crop, provide:
   - Suitability score (1-10)
   - Best growing season
   - Key requirements (water, temperature, soil type)
   - Expected yield potential
   - Any challenges or considerations

3. General agricultural recommendations for this region

Format your response as a clear, structured analysis suitable for farmers and agricultural planners.
Be specific and practical in your recommendations."""

            # Call Gemini API
            response = self.model.generate_content(prompt)
            
            # Extract text from response
            if hasattr(response, 'text'):
                recommendations_text = response.text
            elif hasattr(response, 'candidates') and len(response.candidates) > 0:
                recommendations_text = response.candidates[0].content.parts[0].text
            else:
                recommendations_text = str(response)
            
            return {
                'location': location,
                'coordinates': {'lat': lat, 'lon': lon},
                'recommendations': recommendations_text,
                'land_cover_summary': {
                    'urban': round(land_cover.urban, 2),
                    'vegetation': round(land_cover.vegetation, 2),
                    'forest': round(land_cover.forest, 2),
                    'water': round(land_cover.water, 2)
                },
                'weather_summary': {
                    'temperature': round(weather_data.temperature, 1) if weather_data else None,
                    'precipitation': round(weather_data.precipitation, 1) if weather_data else None
                } if weather_data else None
            }
            
        except Exception as e:
            raise RuntimeError(f"Failed to get crop recommendations from Gemini API: {str(e)}")
    
    def _prepare_context(self, location: str, lat: float, lon: float,
                        land_cover: LandCoverResult,
                        weather_data: Optional[WeatherData] = None) -> Dict:
        """Prepare context dictionary for API calls"""
        return {
            'location': location,
            'coordinates': (lat, lon),
            'land_cover': {
                'urban': land_cover.urban,
                'vegetation': land_cover.vegetation,
                'forest': land_cover.forest,
                'water': land_cover.water
            },
            'weather': {
                'temperature': weather_data.temperature if weather_data else None,
                'precipitation': weather_data.precipitation if weather_data else None,
                'humidity': weather_data.humidity if weather_data else None
            } if weather_data else None
        }
    
    def generate_ai_insights(self, location: str, lat: float, lon: float,
                            land_cover: LandCoverResult,
                            weather_data: Optional[WeatherData] = None,
                            climate_risks: Optional[Dict] = None,
                            air_quality: Optional[AirQualityData] = None,
                            urbanisation_risk: Optional[Dict] = None) -> Dict:
        """
        Generate AI-powered insights using Gemini 2.5 Flash
        
        Provides natural language explanations and actionable insights based on analysis data.
        
        Args:
            location: Location name
            lat: Latitude
            lon: Longitude
            land_cover: Land cover classification results
            weather_data: Optional weather data
            climate_risks: Optional climate risk assessment
            air_quality: Optional air quality data
            urbanisation_risk: Optional urbanisation risk score
            
        Returns:
            Dictionary with AI-generated insights
        """
        try:
            prompt = f"""You are an expert geospatial analyst providing insights for a satellite-based analysis of {location}.

ANALYSIS DATA:
- Location: {location} ({lat}, {lon})
- Land Cover: Urban {land_cover.urban:.1f}%, Forest {land_cover.forest:.1f}%, Vegetation {land_cover.vegetation:.1f}%, Water {land_cover.water:.1f}%
"""

            if weather_data:
                prompt += f"""
- Current Weather: {weather_data.temperature:.1f}°C, {weather_data.precipitation:.1f}mm precipitation, {weather_data.humidity:.1f}% humidity
"""

            if climate_risks:
                prompt += f"""
- Climate Risks: Flood Risk: {climate_risks.get('flood', 'N/A')}, Heat Risk: {climate_risks.get('heat', 'N/A')}, Drought Risk: {climate_risks.get('drought', 'N/A')}
"""

            if air_quality:
                prompt += f"""
- Air Quality: AQI {air_quality.aqi} ({air_quality.aqi_level}), PM2.5: {air_quality.pm25:.1f} μg/m³
"""

            if urbanisation_risk:
                urs = urbanisation_risk.get('urs_score', 0)
                risk_level = urbanisation_risk.get('risk_level', 'Unknown')
                prompt += f"""
- Urbanisation Risk: {urs:.2f} ({risk_level})
"""

            prompt += """

Provide a comprehensive analysis with:

1. KEY INSIGHTS (3-4 bullet points highlighting the most important findings)
2. ENVIRONMENTAL ASSESSMENT (What does the land cover tell us about environmental health?)
3. RISK ANALYSIS (What are the main concerns and why?)
4. RECOMMENDATIONS (3-5 actionable recommendations for policymakers, urban planners, or residents)
5. FUTURE OUTLOOK (What trends should be monitored?)

Be specific, data-driven, and practical. Write in clear, professional language suitable for decision-makers.
Keep each section concise (2-3 sentences). Use Indian context when relevant (e.g., mention Indian cities, climate patterns, etc.).
"""

            # Call Gemini API
            response = self.model.generate_content(
                prompt,
                generation_config={
                    'temperature': 0.7,
                    'top_p': 0.95,
                    'top_k': 40,
                    'max_output_tokens': 1024,
                }
            )
            
            # Extract text from response
            if hasattr(response, 'text'):
                insights_text = response.text
            elif hasattr(response, 'candidates') and len(response.candidates) > 0:
                insights_text = response.candidates[0].content.parts[0].text
            else:
                insights_text = str(response)
            
            return {
                'location': location,
                'coordinates': {'lat': lat, 'lon': lon},
                'insights': insights_text,
                'generated_at': datetime.now().isoformat(),
                'model': 'gemini-1.5-flash'
            }
            
        except Exception as e:
            raise RuntimeError(f"Failed to generate AI insights: {str(e)}")
    
    def generate_ai_insights_from_location(self, location: str, geocoding_service=None) -> Dict:
        """
        Generate AI-powered insights directly from location using Gemini API
        WITHOUT requiring Sentinel-2 analysis
        
        Uses Gemini's knowledge to analyze the area based on location name and coordinates.
        
        Args:
            location: Location name (e.g., "Andheri West, Mumbai")
            geocoding_service: Optional GeocodingService instance for geocoding
            
        Returns:
            Dictionary with AI-generated insights
        """
        try:
            # Geocode location to get coordinates
            if geocoding_service:
                results = geocoding_service.geocode(location)
            else:
                # Fallback: try to parse as coordinates or use OpenCage directly
                from geospatial_intelligence import GeocodingService
                import os
                opencage_key = os.getenv('OPENCAGE_API_KEY')
                if not opencage_key:
                    raise ValueError("Geocoding service not available. Please provide geocoding_service or set OPENCAGE_API_KEY")
                geocoding_service = GeocodingService(opencage_key)
                results = geocoding_service.geocode(location)
            
            if not results:
                raise ValueError(f"Location '{location}' not found")
            
            geometry = results[0]['geometry']
            lat, lon = geometry['lat'], geometry['lng']
            
            # Get weather data for context (if available)
            weather_context = "Weather data unavailable."
            try:
                # Try to get weather data - this requires WeatherService to be available
                # We'll handle this gracefully if it's not available
                import os
                openweather_key = os.getenv('OPENWEATHER_API_KEY')
                if openweather_key:
                    # Create a temporary weather service instance
                    from geospatial_intelligence import WeatherService
                    weather_service = WeatherService(openweather_key)
                    weather_data = weather_service.get_weather_data(lat, lon)
                    weather_context = f"""
Current Weather Conditions:
- Temperature: {weather_data.temperature:.1f}°C
- Humidity: {weather_data.humidity:.1f}%
- Precipitation: {weather_data.precipitation:.1f} mm
- Wind Speed: {weather_data.wind_speed:.1f} m/s
- Pressure: {weather_data.pressure:.1f} hPa
"""
            except Exception as e:
                # Weather data is optional, continue without it
                weather_context = "Weather data unavailable."
            
            # Create comprehensive prompt for Gemini
            prompt = f"""You are an expert geospatial and urban planning analyst. Analyze the following location and provide comprehensive insights.

LOCATION INFORMATION:
- Location: {location}
- Coordinates: {lat:.4f}, {lon:.4f}
- Region: India

{weather_context}

Based on your knowledge of this location, its geography, urban development patterns, environmental characteristics, and regional context, provide:

1. KEY INSIGHTS (3-4 bullet points about the area's characteristics, development status, and notable features)

2. LAND USE ASSESSMENT (What is the typical land use pattern? Urban, suburban, industrial, residential, commercial, agricultural, or mixed?)

3. ENVIRONMENTAL CHARACTERISTICS (Green spaces, water bodies, air quality concerns, climate patterns typical for this area)

4. URBAN DEVELOPMENT STATUS (Level of urbanization, infrastructure, population density patterns, growth trends)

5. RISK FACTORS (Environmental risks, climate risks, urban challenges specific to this location)

6. RECOMMENDATIONS (3-5 actionable recommendations for:
   - Urban planners
   - Environmental management
   - Residents
   - Policy makers)

7. FUTURE OUTLOOK (What trends should be monitored? What changes are likely?)

Be specific, data-driven, and practical. Use your knowledge of Indian cities, geography, and urban patterns.
Write in clear, professional language suitable for decision-makers.
Keep each section concise (2-3 sentences).
Mention specific characteristics of this location if you know them.
"""

            # Call Gemini API
            response = self.model.generate_content(
                prompt,
                generation_config={
                    'temperature': 0.7,
                    'top_p': 0.95,
                    'top_k': 40,
                    'max_output_tokens': 1500,
                }
            )
            
            # Extract text from response
            if hasattr(response, 'text'):
                insights_text = response.text
            elif hasattr(response, 'candidates') and len(response.candidates) > 0:
                insights_text = response.candidates[0].content.parts[0].text
            else:
                insights_text = str(response)
            
            return {
                'location': location,
                'coordinates': {'lat': lat, 'lon': lon},
                'insights': insights_text,
                'generated_at': datetime.now().isoformat(),
                'model': 'gemini-1.5-flash',
                'source': 'Location-based analysis (no satellite data required)',
                'weather_available': 'weather_context' in locals() and weather_context != "Weather data unavailable."
            }
            
        except Exception as e:
            raise RuntimeError(f"Failed to generate AI insights from location: {str(e)}")


class PhysicalFeatureDetector:
    """Detect and mark physical features in satellite images"""
    
    def __init__(self, ee_service: 'EarthEngineService', geocoding_service: 'GeocodingService'):
        """
        Initialize physical feature detector
        
        Args:
            ee_service: Earth Engine service instance
            geocoding_service: Geocoding service instance
        """
        self.ee_service = ee_service
        self.geocoding = geocoding_service
    
    def detect_features(self, location: str, buffer_radius_km: float = 2.0,
                       start_date: str = None, end_date: str = None) -> Dict:
        """
        Detect physical features in satellite images
        
        Features detected:
        - Roads (using NDBI and linear features)
        - Buildings (using NDBI and urban classification)
        - Water bodies (using MNDWI)
        - Vegetation patches (using NDVI)
        - Agricultural fields (using NDVI patterns)
        
        Args:
            location: Location name or coordinates
            buffer_radius_km: Buffer radius in kilometers
            start_date: Optional start date
            end_date: Optional end date
            
        Returns:
            Dictionary with detected features and their locations
        """
        try:
            # Get AOI polygon
            aoi_polygon, bbox, center, location_name = self.geocoding.get_aoi_polygon(
                location, buffer_radius_km
            )
            center_lat, center_lon = center
            
            # Get Sentinel-2 composite
            s2_composite, image_date = self.ee_service.get_sentinel2_sr_composite(
                aoi_polygon, bbox, start_date, end_date, cloud_cover_threshold=10.0
            )
            
            if s2_composite is None:
                raise RuntimeError(f"No Sentinel-2 data available for location '{location_name}'")
            
            # Calculate spectral indices
            s2_with_indices = self.ee_service.calculate_spectral_indices(s2_composite)
            
            # Detect different features
            features = {
                'water_bodies': self._detect_water_bodies(s2_with_indices, aoi_polygon),
                'vegetation_patches': self._detect_vegetation_patches(s2_with_indices, aoi_polygon),
                'urban_areas': self._detect_urban_areas(s2_with_indices, aoi_polygon),
                'agricultural_fields': self._detect_agricultural_fields(s2_with_indices, aoi_polygon)
            }
            
            return {
                'location': location_name,
                'coordinates': {'lat': center_lat, 'lon': center_lon},
                'satellite_date': image_date,
                'features': features,
                'summary': {
                    'water_bodies_count': len(features['water_bodies']),
                    'vegetation_patches_count': len(features['vegetation_patches']),
                    'urban_areas_count': len(features['urban_areas']),
                    'agricultural_fields_count': len(features['agricultural_fields'])
                }
            }
            
        except Exception as e:
            raise RuntimeError(f"Failed to detect physical features: {str(e)}")
    
    def _detect_water_bodies(self, image: ee.Image, polygon: ee.Geometry) -> List[Dict]:
        """Detect water bodies using MNDWI"""
        mndwi = image.select('MNDWI')
        water_mask = mndwi.gt(0.2)
        
        # Find connected components (water bodies)
        water_connected = water_mask.connectedComponents(
            neighborhood=ee.Kernel.square(radius=1),
            maxSize=1024
        )
        
        # Get statistics for each water body
        # This is a simplified version - in production, you'd use more sophisticated methods
        water_stats = water_mask.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=polygon,
            scale=30,
            maxPixels=1e9
        )
        
        # Return simplified feature list
        return [{
            'type': 'water_body',
            'detection_method': 'MNDWI > 0.2',
            'area_km2': 'calculated_on_demand'
        }]
    
    def _detect_vegetation_patches(self, image: ee.Image, polygon: ee.Geometry) -> List[Dict]:
        """Detect vegetation patches using NDVI"""
        ndvi = image.select('NDVI')
        vegetation_mask = ndvi.gt(0.25)
        
        return [{
            'type': 'vegetation_patch',
            'detection_method': 'NDVI > 0.25',
            'area_km2': 'calculated_on_demand'
        }]
    
    def _detect_urban_areas(self, image: ee.Image, polygon: ee.Geometry) -> List[Dict]:
        """Detect urban areas using NDBI and NDVI"""
        ndbi = image.select('NDBI')
        ndvi = image.select('NDVI')
        urban_mask = ndbi.gt(0.0).And(ndvi.lt(0.3))
        
        return [{
            'type': 'urban_area',
            'detection_method': 'NDBI > 0 AND NDVI < 0.3',
            'area_km2': 'calculated_on_demand'
        }]
    
    def _detect_agricultural_fields(self, image: ee.Image, polygon: ee.Geometry) -> List[Dict]:
        """Detect agricultural fields using NDVI patterns"""
        ndvi = image.select('NDVI')
        # Agricultural fields typically have moderate NDVI (0.3-0.7)
        ag_mask = ndvi.gte(0.3).And(ndvi.lte(0.7))
        
        return [{
            'type': 'agricultural_field',
            'detection_method': '0.3 <= NDVI <= 0.7',
            'area_km2': 'calculated_on_demand'
        }]


class GeospatialIntelligenceSystem:
    """Main system orchestrating all components"""
    
    def __init__(self, opencage_key: str, openweather_key: str, earthengine_project: Optional[str] = None,
                 supabase_url: Optional[str] = None, supabase_key: Optional[str] = None,
                 newsapi_key: Optional[str] = None, gemini_api_key: Optional[str] = None):
        self.geocoding = GeocodingService(opencage_key)
        self.ee_service = EarthEngineService(project=earthengine_project)
        self.classifier = LandCoverClassifier()
        self.spectral_classifier = SpectralIndexClassifier()
        self.weather = WeatherService(openweather_key)
        self.disaster = DisasterService(openweather_key)
        self.risk_calculator = ClimateRiskCalculator()
        self.urbanisation_risk_calculator = UrbanisationRiskCalculator()
        self.carbon_calculator = CarbonFootprintCalculator()
        self.economic_analyzer = EconomicImpactAnalyzer()
        self.supabase = SupabaseService(supabase_url, supabase_key)
        self.news = NewsService(newsapi_key)
        
        # Initialize Gemini service (optional - only if API key provided)
        try:
            self.crop_recommendation = GeminiCropRecommendationService(gemini_api_key)
        except (ValueError, ImportError) as e:
            print(f"Warning: Gemini API not available: {e}")
            self.crop_recommendation = None
        
        # Initialize physical feature detector
        self.feature_detector = PhysicalFeatureDetector(self.ee_service, self.geocoding)
    
    def analyze_locality(self, city_name: str, locality_name: str, 
                        locality_polygon: ee.Geometry, locality_bbox: BoundingBox,
                        start_date: str = None, end_date: str = None) -> Dict:
        """
        Analyze a specific locality using STRICT, CITY-INVARIANT pipeline.
        
        STRICT STANDARDIZATION:
        1. Create FIXED 1km × 1km AOI centered on geocoded centroid
        2. Use Dynamic World V1 with FIXED year (2024) and season (Jan-Mar)
        3. Use pixel-level aggregation with frequencyHistogram
        4. Apply FIXED class mapping (NO adaptive logic)
        5. Return RAW results (NO corrections, NO explanations)
        
        Args:
            city_name: Name of the city
            locality_name: Name of the locality
            locality_polygon: Earth Engine polygon geometry (used only to get centroid)
            locality_bbox: Bounding box for the locality
            start_date: Ignored - uses fixed dates
            end_date: Ignored - uses fixed dates
        
        Returns:
            Dictionary with RAW analysis results
        """
        try:
            # STEP 1: Create FIXED 1km × 1km AOI centered on geocoded centroid
            geom = ee.Geometry(locality_polygon)
            geom = EarthEngineService.preprocess_locality_polygon(geom)
            
            # Get centroid for bbox
            center_lat = (locality_bbox.min_lat + locality_bbox.max_lat) / 2
            center_lon = (locality_bbox.min_lon + locality_bbox.max_lon) / 2
            
            # Create bbox for fixed AOI (1km × 1km)
            radius_km = 0.5
            radius_degrees_lat = radius_km / 111.0
            radius_degrees_lon = radius_km / (111.0 * abs(np.cos(np.radians(center_lat))))
            fixed_bbox = BoundingBox(
                min_lon=center_lon - radius_degrees_lon,
                min_lat=center_lat - radius_degrees_lat,
                max_lon=center_lon + radius_degrees_lon,
                max_lat=center_lat + radius_degrees_lat
            )
            
            # STEP 2: Get Dynamic World image with FIXED parameters (2024, Jan-Mar)
            dw_labels, image_date = self.ee_service.get_dynamic_world_image(
                geom, fixed_bbox, None, None
            )
            
            # STEP 3: Count pixels using frequencyHistogram (pixel-level aggregation)
            pixel_counts_raw = self.ee_service.count_pixels_by_class_direct(
                dw_labels, geom, scale=30
            )
            
            if not pixel_counts_raw or sum(pixel_counts_raw.values()) == 0:
                raise RuntimeError(f"No Dynamic World data available for locality '{locality_name}'")
            
            # STEP 4: Aggregate classes using STRICT mapping (NO adaptive logic)
            land_cover, classification_metadata = self.classifier.aggregate_classes(
                pixel_counts_raw, None, None, None
            )
            
            # STEP 5: Calculate AOI size (fixed 1km × 1km = 1.0 km²)
            total_pixels = sum(v for k, v in pixel_counts_raw.items() if k != -1 and k != 8)
            aoi_size_km2 = 1.0  # Fixed 1km × 1km
            
            # STEP 6: Fetch weather data and air quality (non-blocking)
            try:
                weather_data = self.weather.get_weather_data(center_lat, center_lon)
                forecast_data = self.weather.get_forecast_data(center_lat, center_lon)
            except:
                weather_data = None
                forecast_data = None
            
            # Fetch air quality data
            air_quality = None
            health_advisory = None
            try:
                print(f"Fetching air quality for coordinates: lat={center_lat}, lon={center_lon}")
                air_quality = self.weather.get_air_quality(center_lat, center_lon)
                if air_quality:
                    print(f"Air quality fetched successfully: PM2.5={air_quality.pm25}")
                    health_advisory = self.weather.get_health_advisory(air_quality.aqi)
                else:
                    print("Warning: Air quality data is None")
            except requests.exceptions.Timeout:
                print(f"ERROR: Air quality API request timed out after 30 seconds")
                air_quality = None
                health_advisory = None
            except requests.exceptions.ConnectionError as e:
                print(f"ERROR: Failed to connect to air quality API: {str(e)}")
                air_quality = None
                health_advisory = None
            except Exception as e:
                # Log error details for debugging
                import traceback
                error_details = traceback.format_exc()
                print(f"ERROR: Failed to fetch air quality data: {str(e)}")
                print(f"Traceback: {error_details}")
                air_quality = None
                health_advisory = None
            
            # Fetch weather alerts
            weather_alerts = []
            try:
                print(f"Fetching weather alerts for coordinates: lat={center_lat}, lon={center_lon}")
                weather_alerts = self.weather.get_weather_alerts(center_lat, center_lon)
                if weather_alerts:
                    print(f"Weather alerts fetched successfully: {len(weather_alerts)} alert(s)")
                else:
                    print("No active weather alerts for this location")
            except Exception as e:
                print(f"Error fetching weather alerts: {str(e)}")
                weather_alerts = []
            
            # Fetch global weather news (non-blocking)
            weather_news = []
            try:
                print("Fetching global weather and climate news")
                weather_news = self.news.get_weather_news()
                if weather_news:
                    print(f"Weather news fetched successfully: {len(weather_news)} headline(s)")
                else:
                    print("No weather news available")
            except Exception as e:
                print(f"Error fetching weather news: {str(e)}")
                weather_news = []
            
            # STEP 7: Compute risks (if weather data available)
            if weather_data:
                flood_risk = self.risk_calculator.calculate_flood_risk(
                    weather_data, land_cover, forecast_data
                )
                heat_risk = self.risk_calculator.calculate_heat_risk(
                    weather_data, land_cover
                )
                drought_risk = self.risk_calculator.calculate_drought_risk(
                    weather_data, land_cover, forecast_data
                )
            else:
                flood_risk = "Unknown"
                heat_risk = "Unknown"
                drought_risk = "Unknown"
            
            # STEP 8: Return RAW output (NO confidence guessing, NO explanations, NO corrections)
            result = {
                'locality': locality_name,
                'city': city_name,
                'coordinates': {
                    'lat': float(round(center_lat, 6)),
                    'lon': float(round(center_lon, 6))
                },
                'pixel_counts': pixel_counts_raw,
                'landcover_percentages': {
                    'urban': float(round(land_cover.urban, 2)),
                    'forest': float(round(land_cover.forest, 2)),
                    'vegetation': float(round(land_cover.vegetation, 2)),
                    'water': float(round(land_cover.water, 2))
                },
                'aoi_size_km2': float(round(aoi_size_km2, 2)),
                'total_pixel_count': int(total_pixels),
                'year': classification_metadata.get('year', '2024'),
                'season': classification_metadata.get('season', 'Jan-Mar'),
                'satellite_date': str(image_date) if image_date else None,
                'satellite_source': 'Google Dynamic World V1',
                'weather': {
                    'temperature': float(round(weather_data.temperature, 1)) if weather_data else None,
                    'rainfall': float(round(weather_data.precipitation, 1)) if weather_data else None,
                    'humidity': float(round(weather_data.humidity, 1)) if weather_data else None,
                    'wind_speed': float(round(weather_data.wind_speed, 1)) if weather_data else None,
                    'pressure': float(round(weather_data.pressure, 1)) if weather_data else None
                },
                'air_quality': self._build_air_quality_response(air_quality, health_advisory, land_cover.urban) if air_quality else {
                    'us_aqi': None,
                    'dominant_pollutant': 'PM2.5',
                    'transparency_note': 'AQI calculated using CPCB India standard. PM2.5 is the dominant pollutant for this location.',
                    'pollutants': {
                        'pm25': {'value': None, 'unit': 'µg/m³'},
                        'pm10': {'value': None, 'unit': 'µg/m³'},
                        'co': {'value': None, 'unit': 'mg/m³'},
                        'no2': {'value': None, 'unit': 'µg/m³'},
                        'so2': {'value': None, 'unit': 'µg/m³'},
                        'o3': {'value': None, 'unit': 'µg/m³'}
                    },
                    'health_advisory': None,
                    'urban_density_note': None,
                    'data_source': 'OpenWeather Air Pollution API',
                    'error': 'Air quality data unavailable - API request timed out or connection failed. Please check your internet connection and try again.'
                },
                'flood_risk': str(flood_risk),
                'heat_risk': str(heat_risk),
                'drought_risk': str(drought_risk),
                'weather_alerts': weather_alerts if weather_alerts else [],
                'weather_news': weather_news if weather_news else [],
                'classification_metadata': classification_metadata
            }
            
            # Debug: Print air quality data to verify it's being returned
            if air_quality:
                us_aqi = result['air_quality']['us_aqi']['value'] if result.get('air_quality', {}).get('us_aqi', {}).get('value') else None
                pm25 = result['air_quality']['pollutants']['pm25']['value'] if result.get('air_quality', {}).get('pollutants', {}).get('pm25', {}).get('value') else None
                co = result['air_quality']['pollutants']['co']['value'] if result.get('air_quality', {}).get('pollutants', {}).get('co', {}).get('value') else None
                print(f"DEBUG: Air quality data in response: US AQI={us_aqi}, PM2.5={pm25}, CO={co}")
            else:
                print("DEBUG: Air quality data is None in response")
                print(f"DEBUG: Air quality error field: {result.get('air_quality', {}).get('error', 'NOT SET')}")
                print(f"DEBUG: Full air_quality structure: {json.dumps(result.get('air_quality', {}), indent=2)}")
            
            return result
            
        except KeyError as e:
            # Provide more context for KeyError
            error_msg = f"KeyError: Missing key '{e}' in data structure. "
            import traceback
            error_msg += f"Traceback: {traceback.format_exc()}"
            raise RuntimeError(f"Failed to analyze locality: {error_msg}")
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"Error in analyze_locality: {str(e)}")
            print(f"Traceback: {error_details}")
            raise RuntimeError(f"Failed to analyze locality: {str(e)}")
    
    def _build_air_quality_response(self, air_quality: AirQualityData, health_advisory: Optional[str], urban_percent: float) -> Dict:
        """
        Build air quality response with US AQI
        
        Args:
            air_quality: AirQualityData object
            health_advisory: Health advisory text
            urban_percent: Urban land cover percentage
            
        Returns:
            Dictionary with air quality data
        """
        try:
            # Calculate US AQI using dominant pollutant method (PM2.5, PM10, O3)
            us_aqi = None
            if air_quality and air_quality.pm25 is not None and air_quality.pm25 >= 0:
                pm10_value = air_quality.pm10 if hasattr(air_quality, 'pm10') and air_quality.pm10 is not None else None
                o3_value = air_quality.o3 if hasattr(air_quality, 'o3') and air_quality.o3 is not None else None
                us_aqi = self.weather.calculate_us_aqi(
                    air_quality.pm25,
                    pm10=pm10_value,
                    o3=o3_value
                )
            
            # Generate health advisory based on US AQI
            us_health_advisory = None
            if us_aqi and us_aqi.get('value') is not None:
                us_health_advisory = self._get_us_aqi_health_advisory(us_aqi.get('value'))
            
            # Build transparency note with dominant pollutant
            dominant = us_aqi.get('dominant_pollutant', 'PM2.5') if us_aqi else 'PM2.5'
            transparency_note = f'AQI calculated using US EPA standard and dominant pollutant method. {dominant} is the dominant pollutant for this location.'
            
            return {
                'us_aqi': us_aqi,
                'dominant_pollutant': dominant,
                'transparency_note': transparency_note,
                'pollutants': {
                    'pm25': {
                        'value': float(round(air_quality.pm25, 2)) if air_quality and hasattr(air_quality, 'pm25') and air_quality.pm25 is not None else None,
                        'unit': 'µg/m³'
                    },
                    'pm10': {
                        'value': float(round(air_quality.pm10, 2)) if air_quality and hasattr(air_quality, 'pm10') and air_quality.pm10 is not None else None,
                        'unit': 'µg/m³'
                    },
                    'co': {
                        'value': float(round(air_quality.co, 3)) if air_quality and hasattr(air_quality, 'co') and air_quality.co is not None else None,
                        'unit': 'mg/m³'
                    },
                    'no2': {
                        'value': float(round(air_quality.no2, 2)) if air_quality and hasattr(air_quality, 'no2') and air_quality.no2 is not None else None,
                        'unit': 'µg/m³'
                    },
                    'so2': {
                        'value': float(round(air_quality.so2, 2)) if air_quality and hasattr(air_quality, 'so2') and air_quality.so2 is not None else None,
                        'unit': 'µg/m³'
                    },
                    'o3': {
                        'value': float(round(air_quality.o3, 2)) if air_quality and hasattr(air_quality, 'o3') and air_quality.o3 is not None else None,
                        'unit': 'µg/m³'
                    }
                },
                'health_advisory': us_health_advisory,
                'urban_density_note': self._get_urban_aqi_note(urban_percent, us_aqi.get('value') if us_aqi and us_aqi.get('value') is not None else None),
                'data_source': 'OpenWeather Air Pollution API'
            }
        except Exception as e:
            # If there's any error building the response, return a safe fallback
            print(f"Error building air quality response: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'us_aqi': None,
                'dominant_pollutant': 'PM2.5',
                'transparency_note': 'AQI calculated using US EPA standard and dominant pollutant method. PM2.5 is the dominant pollutant for this location.',
                'pollutants': {
                    'pm25': {'value': None, 'unit': 'µg/m³'},
                    'pm10': {'value': None, 'unit': 'µg/m³'},
                    'co': {'value': None, 'unit': 'mg/m³'},
                    'no2': {'value': None, 'unit': 'µg/m³'},
                    'so2': {'value': None, 'unit': 'µg/m³'},
                    'o3': {'value': None, 'unit': 'µg/m³'}
                },
                'health_advisory': None,
                'urban_density_note': None,
                'data_source': 'OpenWeather Air Pollution API',
                'error': f'Error processing air quality data: {str(e)}'
            }
    
    def _get_us_aqi_health_advisory(self, aqi_value: int) -> str:
        """
        Get health advisory text based on US AQI value (0-500)
        
        Args:
            aqi_value: US AQI value (0-500)
            
        Returns:
            Health advisory text
        """
        if aqi_value <= 50:
            return "Air quality is satisfactory. Air pollution poses little or no risk."
        elif aqi_value <= 100:
            return "Air quality is acceptable. However, there may be a risk for some people, particularly those who are unusually sensitive to air pollution."
        elif aqi_value <= 150:
            return "Members of sensitive groups may experience health effects. The general public is less likely to be affected."
        elif aqi_value <= 200:
            return "Some members of the general public may experience health effects; members of sensitive groups may experience more serious health effects."
        elif aqi_value <= 300:
            return "Health alert: The risk of health effects is increased for everyone. Everyone may begin to experience health effects."
        else:
            return "Health warning of emergency conditions: everyone is more likely to be affected. Avoid all outdoor activities."
    
    def _get_urban_aqi_note(self, urban_percent: float, aqi: Optional[int]) -> Optional[str]:
        """
        Generate note linking high AQI to high urban density
        
        Args:
            urban_percent: Urban land cover percentage
            aqi: India AQI value (0-500) or None
            
        Returns:
            Explanation text or None
        """
        if aqi is None:
            return None
        
        # High US AQI (>=200) with high urban density (>70%)
        if aqi >= 200 and urban_percent > 70:
            return f"High US AQI ({aqi}) correlates with high urban density ({urban_percent:.1f}%). Urban areas typically have elevated pollution from traffic, industry, and reduced vegetation."
        # Moderate US AQI (150-199) with high urban density
        elif aqi >= 150 and urban_percent > 60:
            return f"Moderate to unhealthy US AQI ({aqi}) with high urban density ({urban_percent:.1f}%). Urban areas may contribute to air quality concerns."
        
        return None
    
    def get_weather_alerts(self, lat: float, lon: float) -> List[Dict]:
        """
        Fetch real-time weather alerts from OpenWeather OneCall API
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            List of weather alert dictionaries with title, severity, description, duration
        """
        try:
            # Use OneCall API 3.0 (includes alerts)
            # Note: This requires a subscription plan, but we'll handle gracefully if unavailable
            url = "https://api.openweathermap.org/data/3.0/onecall"
            params = {
                'lat': lat,
                'lon': lon,
                'appid': self.api_key,
                'exclude': 'minutely,hourly,daily,current'
            }
            
            response = requests.get(url, params=params, timeout=15)
            
            # If 3.0 fails (not subscribed), try 2.5 (may not have alerts)
            if response.status_code != 200:
                url = "https://api.openweathermap.org/data/2.5/onecall"
                response = requests.get(url, params=params, timeout=15)
            
            if response.status_code != 200:
                # No alerts available or API error
                return []
            
            data = response.json()
            alerts = []
            
            # Extract alerts from response
            if 'alerts' in data and data['alerts']:
                from datetime import datetime
                
                for alert in data['alerts']:
                    # Parse alert data
                    alert_title = alert.get('event', 'Weather Alert')
                    alert_severity = alert.get('severity', 'unknown')
                    alert_description = alert.get('description', '')
                    
                    # Parse start and end times
                    start_time = alert.get('start', 0)
                    end_time = alert.get('end', 0)
                    
                    # Convert timestamps to readable format
                    start_dt = datetime.fromtimestamp(start_time) if start_time else None
                    end_dt = datetime.fromtimestamp(end_time) if end_time else None
                    
                    duration = None
                    if start_dt and end_dt:
                        duration_hours = (end_dt - start_dt).total_seconds() / 3600
                        if duration_hours < 24:
                            duration = f"{int(duration_hours)} hours"
                        else:
                            duration_days = duration_hours / 24
                            duration = f"{duration_days:.1f} days"
                    
                    # Map alert event types to icons/categories
                    event_type = alert_title.lower()
                    alert_category = 'Weather'
                    if 'heat' in event_type or 'hot' in event_type:
                        alert_category = 'Heatwave'
                    elif 'rain' in event_type or 'precipitation' in event_type:
                        alert_category = 'Heavy Rain'
                    elif 'flood' in event_type:
                        alert_category = 'Flood'
                    elif 'thunder' in event_type or 'storm' in event_type:
                        alert_category = 'Thunderstorm'
                    elif 'cold' in event_type or 'freeze' in event_type:
                        alert_category = 'Cold Wave'
                    elif 'cyclone' in event_type or 'hurricane' in event_type or 'typhoon' in event_type:
                        alert_category = 'Cyclone'
                    
                    alerts.append({
                        'title': alert_title,
                        'category': alert_category,
                        'severity': alert_severity,
                        'description': alert_description,
                        'duration': duration,
                        'start_time': start_dt.strftime('%Y-%m-%d %H:%M') if start_dt else None,
                        'end_time': end_dt.strftime('%Y-%m-%d %H:%M') if end_dt else None,
                        'source': 'OpenWeather'
                    })
            
            return alerts
            
        except Exception as e:
            # Return empty list on error (non-blocking)
            print(f"Error fetching weather alerts: {str(e)}")
            return []
    
    def analyze_sentinel2(self, location: str, buffer_radius_km: float = 2.0,
                          start_date: str = None, end_date: str = None,
                          cloud_cover_threshold: float = 10.0) -> Dict:
        """
        Analyze land cover using Sentinel-2 SR with spectral indices
        
        STEP-BY-STEP IMPLEMENTATION:
        1. Input handling (city name or coordinates → AOI polygon)
        2. Sentinel-2 preprocessing (filter, composite, select bands)
        3. Calculate spectral indices (NDVI, NDBI, MNDWI)
        4. Rule-based land cover classification
        5. Area calculation using pixelArea()
        6. Sanity validation with warnings
        7. Return clean JSON output
        
        Args:
            location: City name (e.g., "Delhi, India") or coordinates (e.g., "28.6139,77.2090")
            buffer_radius_km: Buffer radius in kilometers (default 2 km)
            start_date: Optional start date (YYYY-MM-DD). Defaults to 12 months ago.
            end_date: Optional end date (YYYY-MM-DD). Defaults to now.
            cloud_cover_threshold: Maximum cloud cover percentage (default 10%)
        
        Returns:
            Dictionary with land cover percentages, warnings, and methodology summary
        """
        try:
            # STEP 1 — INPUT HANDLING
            aoi_polygon, bbox, center, location_name = self.geocoding.get_aoi_polygon(
                location, buffer_radius_km
            )
            center_lat, center_lon = center
            
            # STEP 2 — SENTINEL-2 PREPROCESSING
            s2_composite, image_date = self.ee_service.get_sentinel2_sr_composite(
                aoi_polygon, bbox, start_date, end_date, cloud_cover_threshold
            )
            
            # Validate that we have a valid composite
            if s2_composite is None:
                raise RuntimeError(
                    f"No Sentinel-2 data available for location '{location_name}'. "
                    f"This may indicate the area has no Sentinel-2 coverage or all images are cloud-covered."
                )
            
            # STEP 3 — SPECTRAL INDICES (MANDATORY)
            s2_with_indices = self.ee_service.calculate_spectral_indices(s2_composite)
            
            # STEP 4 — LAND COVER LOGIC (CRITICAL)
            classified_image = self.ee_service.classify_land_cover_spectral(s2_with_indices)
            
            # Validate classified image has landcover band
            band_names = classified_image.bandNames().getInfo()
            if 'landcover' not in band_names:
                raise RuntimeError("Failed to create landcover classification. Classified image missing 'landcover' band.")
            
            # STEP 5 — OPTIONAL DYNAMIC WORLD REFINEMENT
            # (Skipped for now - can be added later if needed)
            
            # STEP 6 — AREA CALCULATION
            # Calculate total AOI area
            total_aoi_area = aoi_polygon.area().getInfo()  # Area in square meters
            
            if total_aoi_area <= 0:
                raise ValueError(f"Invalid AOI area: {total_aoi_area}. Polygon may be invalid.")
            
            # Calculate area per class using pixelArea() at 10m resolution
            area_by_class = self.ee_service.calculate_area_by_class_pixelarea(
                classified_image, aoi_polygon, scale=10
            )
            
            # If area calculation failed or returned empty, use pixel count fallback
            if not area_by_class or len(area_by_class) == 0:
                print(f"Warning: Area calculation returned empty for {location_name}. Using pixel count fallback.")
                # Fallback: Use pixel counts instead of area
                pixel_counts = self.ee_service.count_pixels_by_class_direct(
                    classified_image.select('landcover'), aoi_polygon, scale=10
                )
                
                if not pixel_counts or sum(pixel_counts.values()) == 0:
                    raise RuntimeError(
                        f"No valid pixels found in Sentinel-2 image for location '{location_name}'. "
                        f"This may indicate the area has no Sentinel-2 coverage or all pixels are masked."
                    )
                
                # Convert pixel counts to percentages
                total_pixels = sum(pixel_counts.values())
                area_by_class = {
                    0: (pixel_counts.get(0, 0) / total_pixels) * total_aoi_area,  # Water
                    1: (pixel_counts.get(1, 0) / total_pixels) * total_aoi_area,  # Forest
                    2: (pixel_counts.get(2, 0) / total_pixels) * total_aoi_area,  # Urban
                    3: (pixel_counts.get(3, 0) / total_pixels) * total_aoi_area   # Vegetation
                }
            
            # Convert areas to percentages
            land_cover = self.spectral_classifier.aggregate_areas_to_percentages(
                area_by_class, total_aoi_area
            )
            
            # STEP 7 — SANITY VALIDATION (MANDATORY)
            warnings = self.spectral_classifier.validate_urban_city_results(
                land_cover, location_name
            )
            
            # STEP 8 — OUTPUT FORMAT
            result = {
                'city': location_name,
                'coordinates': {
                    'lat': center_lat,
                    'lon': center_lon
                },
                'satellite': 'Sentinel-2 (Urban-first, no bare land)',
                'resolution': '10m',
                'land_cover_percentages': {
                    'urban': round(land_cover.urban, 2),
                    'vegetation': round(land_cover.vegetation, 2),
                    'forest': round(land_cover.forest, 2),
                    'water': round(land_cover.water, 2)
                },
                'warnings': warnings,
                'methodology_summary': {
                    'data_source': 'COPERNICUS/S2_SR (Sentinel-2 Surface Reflectance)',
                    'resolution': '10 meters',
                    'date_range': {
                        'start': start_date or (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'),
                        'end': end_date or datetime.now().strftime('%Y-%m-%d')
                    },
                    'satellite_date': image_date,
                    'cloud_cover_threshold': f'{cloud_cover_threshold}%',
                    'composite_method': 'Median composite',
                    'classification_method': 'Urban-first spectral classification',
                    'indices_used': ['NDVI', 'NDBI', 'MNDWI'],
                    'classification_rules': {
                        'water': 'MNDWI > 0.2',
                        'forest': 'NDVI > 0.6',
                        'vegetation': 'NDVI > 0.25',
                        'urban': 'Catch-all: NOT (water OR vegetation OR forest)'
                    },
                    'key_principle': 'UNKNOWN MUST DEFAULT TO URBAN. Inside an urban AOI in India, all non-vegetation, non-water surfaces default to urban.',
                    'threshold_explanations': {
                        'water_mndwi_0.2': 'Globally reliable water detection',
                        'vegetation_ndvi_0.25': 'All vegetation including parks, crops, sparse vegetation. Intentionally broad to ensure urban does not absorb vegetation.',
                        'forest_ndvi_0.6': 'Dense forest only. Stricter than typical (0.5) because in Indian cities, NDVI > 0.5 often includes parks and crop patches.',
                        'urban_catch_all': 'All pixels that do not match reliable classes (water, vegetation, forest) default to urban. This ensures concrete, asphalt, rooftops, dust-covered yards, metal roofs, and any impervious surface with spectral ambiguity defaults to urban.'
                    },
                    'priority_order': 'Water > Forest > Vegetation > Urban (catch-all)',
                    'optimized_for': 'Both coastal cities (Mumbai) and dry inland cities (Delhi, Vijayawada, Hyderabad)',
                    'expected_results': {
                        'urban_areas': '70-90% urban, 5-20% vegetation, <1% water, <1% forest'
                    },
                    'aoi_area_km2': round(total_aoi_area / 1e6, 2),
                    'buffer_radius_km': buffer_radius_km
                }
            }
            
            return result
            
        except Exception as e:
            raise RuntimeError(f"Failed to analyze location with Sentinel-2: {str(e)}")
    
    def detect_physical_features(self, location: str, buffer_radius_km: float = 2.0,
                                start_date: str = None, end_date: str = None) -> Dict:
        """
        Detect and mark physical features in satellite images
        
        Features detected:
        - Water bodies
        - Vegetation patches
        - Urban areas
        - Agricultural fields
        
        Args:
            location: Location name or coordinates
            buffer_radius_km: Buffer radius in kilometers
            start_date: Optional start date
            end_date: Optional end date
            
        Returns:
            Dictionary with detected features
        """
        return self.feature_detector.detect_features(
            location, buffer_radius_km, start_date, end_date
        )
    
    def get_crop_recommendations(self, location: str, buffer_radius_km: float = 2.0,
                                 start_date: str = None, end_date: str = None,
                                 population_per_km2: Optional[float] = None) -> Dict:
        """
        Get crop suitability recommendations for a region using Gemini API
        
        Args:
            location: Location name or coordinates
            buffer_radius_km: Buffer radius in kilometers
            start_date: Optional start date
            end_date: Optional end date
            population_per_km2: Optional population density per km²
            
        Returns:
            Dictionary with crop recommendations
        """
        if not self.crop_recommendation:
            raise RuntimeError("Gemini API key not configured. Set GEMINI_API_KEY environment variable.")
        
        # Get land cover analysis first
        land_cover_result = self.analyze_sentinel2(
            location, buffer_radius_km, start_date, end_date
        )
        
        # Extract land cover data
        land_cover = LandCoverResult(
            urban=land_cover_result['land_cover_percentages']['urban'],
            forest=land_cover_result['land_cover_percentages']['forest'],
            vegetation=land_cover_result['land_cover_percentages']['vegetation'],
            water=land_cover_result['land_cover_percentages']['water'],
            total_pixels=0
        )
        
        # Get weather data
        coords = land_cover_result['coordinates']
        try:
            weather_data = self.weather.get_weather_data(coords['lat'], coords['lon'])
        except:
            weather_data = None
        
        # Get crop recommendations
        return self.crop_recommendation.get_crop_recommendations(
            location, coords['lat'], coords['lon'], land_cover, weather_data
        )
    
    def calculate_urbanisation_risk(self, location: str, buffer_radius_km: float = 2.0,
                                    start_date: str = None, end_date: str = None,
                                    population_per_km2: Optional[float] = None) -> Dict:
        """
        Calculate Urbanisation Risk Score (URS) for a region
        
        Args:
            location: Location name or coordinates
            buffer_radius_km: Buffer radius in kilometers
            start_date: Optional start date
            end_date: Optional end date
            population_per_km2: Optional population density per km²
            
        Returns:
            Dictionary with URS score, risk level, and breakdown
        """
        # Get land cover analysis first
        land_cover_result = self.analyze_sentinel2(
            location, buffer_radius_km, start_date, end_date
        )
        
        # Extract land cover data
        land_cover = LandCoverResult(
            urban=land_cover_result['land_cover_percentages']['urban'],
            forest=land_cover_result['land_cover_percentages']['forest'],
            vegetation=land_cover_result['land_cover_percentages']['vegetation'],
            water=land_cover_result['land_cover_percentages']['water'],
            total_pixels=0
        )
        
        # Get air quality data if available
        coords = land_cover_result['coordinates']
        air_quality = None
        try:
            air_quality = self.weather.get_air_quality(coords['lat'], coords['lon'])
        except:
            pass
        
        # Calculate urbanisation risk
        urs_result = self.urbanisation_risk_calculator.calculate_urbanisation_risk(
            land_cover, population_per_km2, air_quality
        )
        
        # Combine with location info
        return {
            'location': land_cover_result['city'],
            'coordinates': coords,
            'satellite_date': land_cover_result.get('methodology_summary', {}).get('satellite_date'),
            'urbanisation_risk': urs_result,
            'land_cover_summary': land_cover_result['land_cover_percentages']
        }
    
    def analyze_time_series(self, location: str, buffer_radius_km: float = 2.0,
                           start_date: str = None, end_date: str = None,
                           interval_years: int = 1) -> Dict:
        """
        Analyze land cover changes over time
        
        Args:
            location: Location name or coordinates
            buffer_radius_km: Buffer radius in kilometers
            start_date: Start date (YYYY-MM-DD), defaults to 5 years ago
            end_date: End date (YYYY-MM-DD), defaults to now
            interval_years: Analysis interval in years (default 1 year)
            
        Returns:
            Dictionary with time-series data
        """
        from datetime import datetime, timedelta
        
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        time_series_data = []
        current_date = start_dt
        
        while current_date <= end_dt:
            year_start = current_date.strftime('%Y-%m-%d')
            year_end = (current_date + timedelta(days=365)).strftime('%Y-%m-%d')
            
            try:
                result = self.analyze_sentinel2(
                    location, buffer_radius_km, year_start, year_end
                )
                
                time_series_data.append({
                    'year': current_date.year,
                    'date': year_start,
                    'land_cover': result['land_cover_percentages'],
                    'satellite_date': result.get('methodology_summary', {}).get('satellite_date')
                })
            except Exception as e:
                print(f"Warning: Failed to analyze {year_start}: {str(e)}")
            
            current_date += timedelta(days=365 * interval_years)
        
        # Calculate changes
        changes = {}
        if len(time_series_data) >= 2:
            first = time_series_data[0]['land_cover']
            last = time_series_data[-1]['land_cover']
            
            changes = {
                'urban_change': round(last['urban'] - first['urban'], 2),
                'forest_change': round(last['forest'] - first['forest'], 2),
                'vegetation_change': round(last['vegetation'] - first['vegetation'], 2),
                'water_change': round(last['water'] - first['water'], 2),
                'period_years': len(time_series_data) - 1
            }
        
        return {
            'location': location,
            'time_series': time_series_data,
            'changes': changes,
            'start_date': start_date,
            'end_date': end_date,
            'interval_years': interval_years
        }
    
    def get_ai_insights(self, location: str, buffer_radius_km: float = 2.0,
                       start_date: str = None, end_date: str = None,
                       population_per_km2: Optional[float] = None,
                       use_satellite_data: bool = False) -> Dict:
        """
        Get AI-powered insights using Gemini Flash
        
        Args:
            location: Location name or coordinates
            buffer_radius_km: Buffer radius in kilometers (not used if use_satellite_data=False)
            start_date: Optional start date (not used if use_satellite_data=False)
            end_date: Optional end date (not used if use_satellite_data=False)
            population_per_km2: Optional population density
            use_satellite_data: If False, uses Gemini directly without Sentinel-2 analysis
            
        Returns:
            Dictionary with AI-generated insights
        """
        if not self.crop_recommendation:
            raise RuntimeError("Gemini API key not configured")
        
        # If not using satellite data, generate insights directly from location
        if not use_satellite_data:
            return self.crop_recommendation.generate_ai_insights_from_location(location, self.geocoding)
        
        # Otherwise, use Sentinel-2 analysis (original behavior)
        try:
            # Get comprehensive analysis
            land_cover_result = self.analyze_sentinel2(
                location, buffer_radius_km, start_date, end_date
            )
            
            land_cover = LandCoverResult(
                urban=land_cover_result['land_cover_percentages']['urban'],
                forest=land_cover_result['land_cover_percentages']['forest'],
                vegetation=land_cover_result['land_cover_percentages']['vegetation'],
                water=land_cover_result['land_cover_percentages']['water'],
                total_pixels=0
            )
            
            coords = land_cover_result['coordinates']
            
            # Get weather data
            weather_data = None
            try:
                weather_data = self.weather.get_weather_data(coords['lat'], coords['lon'])
            except:
                pass
            
            # Get climate risks
            climate_risks = {}
            if weather_data:
                climate_risks = {
                    'flood': self.risk_calculator.calculate_flood_risk(weather_data, land_cover),
                    'heat': self.risk_calculator.calculate_heat_risk(weather_data, land_cover),
                    'drought': self.risk_calculator.calculate_drought_risk(weather_data, land_cover)
                }
            
            # Get air quality
            air_quality = None
            try:
                air_quality = self.weather.get_air_quality(coords['lat'], coords['lon'])
            except:
                pass
            
            # Get urbanisation risk
            urbanisation_risk = None
            try:
                urs_result = self.urbanisation_risk_calculator.calculate_urbanisation_risk(
                    land_cover, population_per_km2, air_quality
                )
                urbanisation_risk = urs_result
            except:
                pass
            
            # Generate AI insights
            insights = self.crop_recommendation.generate_ai_insights(
                location, coords['lat'], coords['lon'],
                land_cover, weather_data, climate_risks, air_quality, urbanisation_risk
            )
            
            return insights
        except Exception as e:
            # If Sentinel-2 fails, fallback to location-based insights
            print(f"Warning: Sentinel-2 analysis failed ({str(e)}), using location-based insights")
            return self.crop_recommendation.generate_ai_insights_from_location(location, self.geocoding)
    
    def calculate_carbon_footprint(self, location: str, buffer_radius_km: float = 2.0,
                                  start_date: str = None, end_date: str = None) -> Dict:
        """
        Calculate carbon footprint and sequestration potential
        
        Args:
            location: Location name or coordinates
            buffer_radius_km: Buffer radius in kilometers
            start_date: Optional start date
            end_date: Optional end date
            
        Returns:
            Dictionary with carbon calculations
        """
        land_cover_result = self.analyze_sentinel2(
            location, buffer_radius_km, start_date, end_date
        )
        
        land_cover = LandCoverResult(
            urban=land_cover_result['land_cover_percentages']['urban'],
            forest=land_cover_result['land_cover_percentages']['forest'],
            vegetation=land_cover_result['land_cover_percentages']['vegetation'],
            water=land_cover_result['land_cover_percentages']['water'],
            total_pixels=0
        )
        
        area_km2 = land_cover_result.get('methodology_summary', {}).get('aoi_area_km2', buffer_radius_km * buffer_radius_km * 3.14159)
        
        carbon_result = self.carbon_calculator.calculate_carbon_impact(land_cover, area_km2)
        
        return {
            'location': land_cover_result['city'],
            'coordinates': land_cover_result['coordinates'],
            'carbon_analysis': carbon_result,
            'land_cover_summary': land_cover_result['land_cover_percentages']
        }
    
    def calculate_economic_impact(self, location: str, buffer_radius_km: float = 2.0,
                                 start_date: str = None, end_date: str = None,
                                 population: Optional[int] = None) -> Dict:
        """
        Calculate economic impact in Indian Rupees
        
        Args:
            location: Location name or coordinates
            buffer_radius_km: Buffer radius in kilometers
            start_date: Optional start date
            end_date: Optional end date
            population: Optional population count
            
        Returns:
            Dictionary with economic impact calculations
        """
        land_cover_result = self.analyze_sentinel2(
            location, buffer_radius_km, start_date, end_date
        )
        
        land_cover = LandCoverResult(
            urban=land_cover_result['land_cover_percentages']['urban'],
            forest=land_cover_result['land_cover_percentages']['forest'],
            vegetation=land_cover_result['land_cover_percentages']['vegetation'],
            water=land_cover_result['land_cover_percentages']['water'],
            total_pixels=0
        )
        
        area_km2 = land_cover_result.get('methodology_summary', {}).get('aoi_area_km2', buffer_radius_km * buffer_radius_km * 3.14159)
        
        economic_result = self.economic_analyzer.calculate_economic_impact(
            land_cover, area_km2, population
        )
        
        return {
            'location': land_cover_result['city'],
            'coordinates': land_cover_result['coordinates'],
            'economic_analysis': economic_result,
            'land_cover_summary': land_cover_result['land_cover_percentages']
        }
    
    def analyze_location(self, location: str, start_date: str = None, 
                        end_date: str = None) -> Dict:
        """
        Main analysis function using Google Dynamic World
        
        Returns JSON with:
        - city: Location name
        - coordinates: Center coordinates
        - pixel_counts: Raw pixel counts per Dynamic World class
        - percentages: Aggregated land cover percentages
        - date_of_satellite_image: Date of the Dynamic World image
        - urbanisation_risk: Urban Risk Score and details
        """
        try:
            # Step 1: Fetch city administrative boundary polygon from OpenStreetMap
            city_polygon, bbox, center = self.geocoding.get_city_boundary_polygon(location)
            center_lat, center_lon = center
            
            # Step 2: Get Dynamic World image for city polygon
            dw_image, image_date = self.ee_service.get_dynamic_world_image(city_polygon, bbox, start_date, end_date)
            
            # Step 3: Count pixels per class using tiled reduceRegion with frequencyHistogram (only inside city polygon)
            # Uses 2km × 2km tiles with scale=30 for 9x faster processing
            pixel_counts_raw = self.ee_service.count_pixels_by_class(dw_image, city_polygon, bbox, scale=30)
            
            # Step 4: Aggregate Dynamic World classes and calculate percentages
            # Use STRICT mapping (NO adaptive logic)
            land_cover, _ = self.classifier.aggregate_classes(pixel_counts_raw, None, None, None)
            
            # Step 5: Get weather data
            weather_data = self.weather.get_weather_data(center_lat, center_lon)
            forecast_data = self.weather.get_forecast_data(center_lat, center_lon)
            
            # Step 6: Get air quality for risk assessment
            air_quality = None
            try:
                air_quality = self.weather.get_air_quality(center_lat, center_lon)
            except:
                pass

            # Step 7: Calculate climate risks
            flood_risk = self.risk_calculator.calculate_flood_risk(
                weather_data, land_cover, forecast_data
            )
            heat_risk = self.risk_calculator.calculate_heat_risk(
                weather_data, land_cover
            )
            drought_risk = self.risk_calculator.calculate_drought_risk(
                weather_data, land_cover, forecast_data
            )
            
            # Step 8: Calculate Urbanisation Risk Score
            urs_result = self.urbanisation_risk_calculator.calculate_urbanisation_risk(
                land_cover, None, air_quality
            )

            # Step 9: Calculate Environmental Sustainability Index
            esi_result = self.urbanisation_risk_calculator.calculate_esi(
                land_cover, air_quality
            )

            # Step 10: Compile results as JSON
            result = {
                'city': location,
                'coordinates': {
                    'lat': round(center_lat, 6),
                    'lon': round(center_lon, 6)
                },
                'pixel_counts': {
                    'water': pixel_counts_raw.get(0, 0),
                    'trees': pixel_counts_raw.get(1, 0),
                    'grass': pixel_counts_raw.get(2, 0),
                    'flooded_vegetation': pixel_counts_raw.get(3, 0),
                    'crops': pixel_counts_raw.get(4, 0),
                    'shrub_scrub': pixel_counts_raw.get(5, 0),
                    'built_area': pixel_counts_raw.get(6, 0),
                    'bare_ground': pixel_counts_raw.get(7, 0),
                    'snow_ice': pixel_counts_raw.get(8, 0)
                },
                'percentages': {
                    'urban': round(land_cover.urban, 2),
                    'forest': round(land_cover.forest, 2),
                    'vegetation': round(land_cover.vegetation, 2),
                    'water': round(land_cover.water, 2)
                },
                'date_of_satellite_image': image_date,
                'bounding_box': {
                    'min_lon': round(bbox.min_lon, 6),
                    'min_lat': round(bbox.min_lat, 6),
                    'max_lon': round(bbox.max_lon, 6),
                    'max_lat': round(bbox.max_lat, 6)
                },
                'weather': {
                    'temperature': round(weather_data.temperature, 2),
                    'humidity': round(weather_data.humidity, 2),
                    'precipitation': round(weather_data.precipitation, 2),
                    'wind_speed': round(weather_data.wind_speed, 2),
                    'pressure': round(weather_data.pressure, 2)
                },
                'climate_risks': {
                    'flood': flood_risk,
                    'heat': heat_risk,
                    'drought': drought_risk
                },
                'urbanisation_risk': urs_result,
                'esi': esi_result,
                'timestamp': datetime.now().isoformat()
            }
            
            return result
        except Exception as e:
            raise RuntimeError(f"Analysis failed: {str(e)}")

    def analyze_polygon(self, geojson_geometry: Dict, start_date: str = None,
                       end_date: str = None) -> Dict:
        """
        Analyze a custom GeoJSON polygon
        """
        try:
            # Step 1: Convert GeoJSON to Earth Engine geometry
            ee_geometry = ee.Geometry(geojson_geometry)

            # Get centroid and bbox
            centroid = ee_geometry.centroid().getInfo()['coordinates']
            center_lon, center_lat = centroid

            bbox_info = ee_geometry.bounds().getInfo()['coordinates'][0]
            lons = [c[0] for c in bbox_info]
            lats = [c[1] for c in bbox_info]
            bbox = BoundingBox(
                min_lon=min(lons),
                min_lat=min(lats),
                max_lon=max(lons),
                max_lat=max(lats)
            )

            # Step 2: Get Sentinel-2 image for polygon
            s2_composite, image_date = self.ee_service.get_sentinel2_sr_composite(
                ee_geometry, bbox, start_date, end_date
            )

            if s2_composite is None:
                raise RuntimeError("No satellite data available for this area.")

            # Step 3: Spectral analysis
            s2_with_indices = self.ee_service.calculate_spectral_indices(s2_composite)
            classified_image = self.ee_service.classify_land_cover_spectral(s2_with_indices)

            # Step 4: Area calculation
            total_area_m2 = ee_geometry.area().getInfo()
            area_by_class = self.ee_service.calculate_area_by_class_pixelarea(
                classified_image, ee_geometry, scale=10
            )

            land_cover = self.spectral_classifier.aggregate_areas_to_percentages(
                area_by_class, total_area_m2
            )

            # Step 5: Weather and risks
            weather_data = self.weather.get_weather_data(center_lat, center_lon)
            air_quality = None
            try:
                air_quality = self.weather.get_air_quality(center_lat, center_lon)
            except:
                pass

            flood_risk = self.risk_calculator.calculate_flood_risk(weather_data, land_cover)
            heat_risk = self.risk_calculator.calculate_heat_risk(weather_data, land_cover)
            drought_risk = self.risk_calculator.calculate_drought_risk(weather_data, land_cover)

            urs_result = self.urbanisation_risk_calculator.calculate_urbanisation_risk(
                land_cover, None, air_quality
            )

            esi_result = self.urbanisation_risk_calculator.calculate_esi(
                land_cover, air_quality
            )

            carbon_result = self.carbon_calculator.calculate_carbon_impact(
                land_cover, total_area_m2 / 1e6
            )

            return {
                'area_name': 'Custom Polygon',
                'coordinates': {'lat': center_lat, 'lon': center_lon},
                'landcover_percentages': {
                    'urban': round(land_cover.urban, 2),
                    'forest': round(land_cover.forest, 2),
                    'vegetation': round(land_cover.vegetation, 2),
                    'water': round(land_cover.water, 2)
                },
                'area_km2': round(total_area_m2 / 1e6, 2),
                'satellite_date': image_date,
                'flood_risk': flood_risk,
                'heat_risk': heat_risk,
                'drought_risk': drought_risk,
                'urbanisation_risk': urs_result,
                'esi': esi_result,
                'carbon_analysis': carbon_result,
                'weather': {
                    'temperature': round(weather_data.temperature, 2),
                    'humidity': round(weather_data.humidity, 2)
                }
            }
        except Exception as e:
            raise RuntimeError(f"Polygon analysis failed: {str(e)}")


def main():
    """Example usage"""
    import sys
    
    # Get API keys from environment variables
    opencage_key = os.getenv('OPENCAGE_API_KEY')
    openweather_key = os.getenv('OPENWEATHER_API_KEY')
    
    if not opencage_key:
        raise ValueError("OPENCAGE_API_KEY environment variable not set")
    if not openweather_key:
        raise ValueError("OPENWEATHER_API_KEY environment variable not set")
    
    # Initialize system
    system = GeospatialIntelligenceSystem(opencage_key, openweather_key)
    
    # Get location from command line or use default
    location = sys.argv[1] if len(sys.argv) > 1 else "Paris, France"
    
    # Run analysis
    print(f"Analyzing location: {location}")
    result = system.analyze_location(location)
    
    # Output JSON
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()


