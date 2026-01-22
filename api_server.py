"""
Flask API server for Geospatial Intelligence System
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from geospatial_intelligence import GeospatialIntelligenceSystem
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json

load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

# Cache for locality lists (7 days TTL)
# Structure: { city_name: { 'localities': [...], 'cached_at': datetime } }
app.locality_list_cache = {}

# Initialize system
opencage_key = os.getenv('OPENCAGE_API_KEY')
openweather_key = os.getenv('OPENWEATHER_API_KEY')
earthengine_project = os.getenv('EARTHENGINE_PROJECT')  # Optional
supabase_url = os.getenv('SUPABASE_URL')  # Optional
supabase_key = os.getenv('SUPABASE_KEY')  # Optional (service role key)
newsapi_key = os.getenv('NEWSAPI_KEY')  # Optional
gemini_api_key = os.getenv('GEMINI_API_KEY')  # Optional

if not opencage_key or not openweather_key:
    raise ValueError("API keys must be set in environment variables")

system = GeospatialIntelligenceSystem(
    opencage_key, 
    openweather_key, 
    earthengine_project,
    supabase_url,
    supabase_key,
    newsapi_key,
    gemini_api_key
)


@app.route('/analyze-city', methods=['POST'])
def analyze_city():
    """Analyze the entire city boundary"""
    try:
        data = request.get_json()
        if not data or 'city' not in data:
            return jsonify({'error': 'City parameter is required'}), 400

        city_name = data['city'].strip()
        result = system.analyze_location(city_name)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/analyze-polygon', methods=['POST'])
def analyze_polygon():
    """Analyze a user-drawn custom polygon"""
    try:
        data = request.get_json()
        if not data or 'geometry' not in data:
            return jsonify({'error': 'Geometry parameter is required'}), 400

        geometry = data['geometry']

        # Basic validation
        if not isinstance(geometry, dict) or 'type' not in geometry or 'coordinates' not in geometry:
            return jsonify({'error': 'Invalid GeoJSON geometry structure'}), 400

        if geometry['type'] not in ['Polygon', 'MultiPolygon']:
            return jsonify({'error': f'Unsupported geometry type: {geometry["type"]}. Only Polygon and MultiPolygon are supported.'}), 400

        # Check vertex count to avoid massive requests
        vertex_count = 0
        try:
            if geometry['type'] == 'Polygon':
                for ring in geometry['coordinates']:
                    vertex_count += len(ring)
            elif geometry['type'] == 'MultiPolygon':
                for polygon in geometry['coordinates']:
                    for ring in polygon:
                        vertex_count += len(ring)
        except (KeyError, TypeError):
            return jsonify({'error': 'Invalid coordinate structure in GeoJSON'}), 400

        if vertex_count > 1000:
            return jsonify({'error': 'Polygon has too many vertices (limit 1000)'}), 400

        result = system.analyze_polygon(geometry)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/localities', methods=['POST'])
def get_localities():
    """Get list of localities for a city (fast, names only)"""
    try:
        data = request.get_json()
        
        if not data or 'city' not in data:
            return jsonify({'error': 'City parameter is required'}), 400
        
        city_name = data['city'].strip()
        radius_km = data.get('radius_km', 8)  # Default 8km (reduced to avoid Overpass API overload)
        
        # Check cache first (7 days TTL)
        if city_name in app.locality_list_cache:
            cached_data = app.locality_list_cache[city_name]
            cached_at = cached_data.get('cached_at')
            if cached_at:
                age = datetime.now() - cached_at
                if age.days < 7:
                    # Return cached data
                    return jsonify({
                        'city': city_name,
                        'localities': cached_data['localities'],
                        'cached': True
                    }), 200
        
        # Fetch localities (names and centers only, no polygons)
        localities = system.geocoding.get_localities(city_name, radius_km)
        
        # Build response with names only
        locality_list = []
        for loc in localities:
            locality_list.append({
                'name': loc['name'],
                'lat': loc.get('lat'),
                'lon': loc.get('lon'),
                'place_type': loc.get('place_type', 'unknown')
            })
        
        # Cache the locality list for 7 days
        app.locality_list_cache[city_name] = {
            'localities': locality_list,
            'raw_data': localities,  # Store raw data for geometry fetching
            'cached_at': datetime.now()
        }
        
        return jsonify({
            'city': city_name,
            'localities': locality_list,
            'cached': False
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/analyze', methods=['POST'])
def analyze():
    """Analyze a locality (fetches geometry on-demand)"""
    try:
        data = request.get_json()
        
        if not data or 'city' not in data or 'locality' not in data:
            return jsonify({'error': 'City and locality parameters are required'}), 400
        
        city_name = data['city'].strip()
        locality_name = data['locality'].strip()
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        # Step 1: Find locality in cached list
        locality_info = None
        if city_name in app.locality_list_cache:
            cached_data = app.locality_list_cache[city_name]
            raw_data = cached_data.get('raw_data', [])
            
            # Find matching locality (case-insensitive)
            for loc in raw_data:
                if loc['name'].strip().lower() == locality_name.lower():
                    locality_info = loc
                    break
        
        if not locality_info:
            # Locality not found in cache - user needs to fetch localities first
            available = []
            if city_name in app.locality_list_cache:
                cached_data = app.locality_list_cache[city_name]
                available = [loc['name'] for loc in cached_data.get('localities', [])]
            
            error_msg = f'Locality "{locality_name}" not found. Please fetch localities first by clicking "Find Localities".'
            if available:
                error_msg += f' Available localities: {", ".join(available[:10])}...'
            return jsonify({'error': error_msg}), 404
        
        # Step 2: Fetch geometry on-demand for this ONE locality
        locality_polygon, locality_bbox = system.geocoding.get_locality_geometry(
            locality_info['name'],
            locality_info.get('lat', 0),
            locality_info.get('lon', 0)
        )
        
        # Step 3: Run analysis
        result = system.analyze_locality(
            city_name, locality_name, locality_polygon, locality_bbox,
            start_date, end_date
        )
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/analyze-sentinel2', methods=['POST'])
def analyze_sentinel2():
    """
    Analyze land cover using Sentinel-2 SR with spectral indices
    
    Accepts:
    - location: City name or coordinates (lat,lon)
    - buffer_radius_km: Optional buffer radius in km (default 2.0)
    - start_date: Optional start date (YYYY-MM-DD)
    - end_date: Optional end date (YYYY-MM-DD)
    - cloud_cover_threshold: Optional cloud cover threshold (default 10.0)
    
    Returns:
    - Clean JSON with land cover percentages, warnings, and methodology
    """
    try:
        data = request.get_json()
        
        if not data or 'location' not in data:
            return jsonify({'error': 'Location parameter is required'}), 400
        
        location = data['location'].strip()
        buffer_radius_km = float(data.get('buffer_radius_km', 2.0))
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        cloud_cover_threshold = float(data.get('cloud_cover_threshold', 10.0))
        
        # Validate buffer radius
        if buffer_radius_km <= 0 or buffer_radius_km > 10:
            return jsonify({
                'error': 'Buffer radius must be between 0 and 10 km'
            }), 400
        
        # Run Sentinel-2 analysis
        result = system.analyze_sentinel2(
            location=location,
            buffer_radius_km=buffer_radius_km,
            start_date=start_date,
            end_date=end_date,
            cloud_cover_threshold=cloud_cover_threshold
        )
        
        return jsonify(result), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/detect-features', methods=['POST'])
def detect_features():
    """
    Detect and mark physical features in satellite images
    
    Accepts:
    - location: City name or coordinates (lat,lon)
    - buffer_radius_km: Optional buffer radius in km (default 2.0)
    - start_date: Optional start date (YYYY-MM-DD)
    - end_date: Optional end date (YYYY-MM-DD)
    
    Returns:
    - Dictionary with detected features (water bodies, vegetation, urban areas, agricultural fields)
    """
    try:
        data = request.get_json()
        
        if not data or 'location' not in data:
            return jsonify({'error': 'Location parameter is required'}), 400
        
        location = data['location'].strip()
        buffer_radius_km = float(data.get('buffer_radius_km', 2.0))
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        # Validate buffer radius
        if buffer_radius_km <= 0 or buffer_radius_km > 10:
            return jsonify({
                'error': 'Buffer radius must be between 0 and 10 km'
            }), 400
        
        # Detect physical features
        result = system.detect_physical_features(
            location=location,
            buffer_radius_km=buffer_radius_km,
            start_date=start_date,
            end_date=end_date
        )
        
        return jsonify(result), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/crop-recommendations', methods=['POST'])
def crop_recommendations():
    """
    Get crop suitability recommendations for a region using Gemini API
    
    Accepts:
    - location: City name or coordinates (lat,lon)
    - buffer_radius_km: Optional buffer radius in km (default 2.0)
    - start_date: Optional start date (YYYY-MM-DD)
    - end_date: Optional end date (YYYY-MM-DD)
    - population_per_km2: Optional population density per km²
    
    Returns:
    - Dictionary with crop recommendations from Gemini API
    """
    try:
        data = request.get_json()
        
        if not data or 'location' not in data:
            return jsonify({'error': 'Location parameter is required'}), 400
        
        location = data['location'].strip()
        buffer_radius_km = float(data.get('buffer_radius_km', 2.0))
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        population_per_km2 = data.get('population_per_km2')
        
        # Validate buffer radius
        if buffer_radius_km <= 0 or buffer_radius_km > 10:
            return jsonify({
                'error': 'Buffer radius must be between 0 and 10 km'
            }), 400
        
        # Get crop recommendations
        result = system.get_crop_recommendations(
            location=location,
            buffer_radius_km=buffer_radius_km,
            start_date=start_date,
            end_date=end_date,
            population_per_km2=population_per_km2
        )
        
        return jsonify(result), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/urbanisation-risk', methods=['POST'])
def urbanisation_risk():
    """
    Calculate Urbanisation Risk Score (URS) for a region
    
    Accepts:
    - location: City name or coordinates (lat,lon)
    - buffer_radius_km: Optional buffer radius in km (default 2.0)
    - start_date: Optional start date (YYYY-MM-DD)
    - end_date: Optional end date (YYYY-MM-DD)
    - population_per_km2: Optional population density per km²
    
    Returns:
    - Dictionary with URS score, risk level, and breakdown
    """
    try:
        data = request.get_json()
        
        if not data or 'location' not in data:
            return jsonify({'error': 'Location parameter is required'}), 400
        
        location = data['location'].strip()
        buffer_radius_km = float(data.get('buffer_radius_km', 2.0))
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        population_per_km2 = data.get('population_per_km2')
        
        # Validate buffer radius
        if buffer_radius_km <= 0 or buffer_radius_km > 10:
            return jsonify({
                'error': 'Buffer radius must be between 0 and 10 km'
            }), 400
        
        # Calculate urbanisation risk
        result = system.calculate_urbanisation_risk(
            location=location,
            buffer_radius_km=buffer_radius_km,
            start_date=start_date,
            end_date=end_date,
            population_per_km2=population_per_km2
        )
        
        return jsonify(result), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/ai-insights', methods=['POST'])
def ai_insights():
    """
    Get AI-powered insights using Gemini Flash
    
    Accepts:
    - location: City name or coordinates (lat,lon)
    - use_satellite_data: Optional boolean (default False) - if False, uses Gemini directly without Sentinel-2
    - buffer_radius_km: Optional buffer radius in km (default 2.0, only used if use_satellite_data=True)
    - start_date: Optional start date (YYYY-MM-DD, only used if use_satellite_data=True)
    - end_date: Optional end date (YYYY-MM-DD, only used if use_satellite_data=True)
    - population_per_km2: Optional population density per km²
    
    Returns:
    - Dictionary with AI-generated insights
    """
    try:
        data = request.get_json()
        
        if not data or 'location' not in data:
            return jsonify({'error': 'Location parameter is required'}), 400
        
        location = data['location'].strip()
        use_satellite_data = data.get('use_satellite_data', False)  # Default to False - direct Gemini analysis
        buffer_radius_km = float(data.get('buffer_radius_km', 2.0))
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        population_per_km2 = data.get('population_per_km2')
        
        if buffer_radius_km <= 0 or buffer_radius_km > 10:
            return jsonify({'error': 'Buffer radius must be between 0 and 10 km'}), 400
        
        result = system.get_ai_insights(
            location=location,
            buffer_radius_km=buffer_radius_km,
            start_date=start_date,
            end_date=end_date,
            population_per_km2=population_per_km2,
            use_satellite_data=use_satellite_data
        )
        
        return jsonify(result), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/time-series', methods=['POST'])
def time_series():
    """
    Analyze land cover changes over time
    
    Accepts:
    - location: City name or coordinates (lat,lon)
    - buffer_radius_km: Optional buffer radius in km (default 2.0)
    - start_date: Optional start date (YYYY-MM-DD), defaults to 5 years ago
    - end_date: Optional end date (YYYY-MM-DD), defaults to now
    - interval_years: Analysis interval in years (default 1)
    
    Returns:
    - Dictionary with time-series data
    """
    try:
        data = request.get_json()
        
        if not data or 'location' not in data:
            return jsonify({'error': 'Location parameter is required'}), 400
        
        location = data['location'].strip()
        buffer_radius_km = float(data.get('buffer_radius_km', 2.0))
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        interval_years = int(data.get('interval_years', 1))
        
        if buffer_radius_km <= 0 or buffer_radius_km > 10:
            return jsonify({'error': 'Buffer radius must be between 0 and 10 km'}), 400
        
        if interval_years < 1 or interval_years > 5:
            return jsonify({'error': 'Interval years must be between 1 and 5'}), 400
        
        result = system.analyze_time_series(
            location=location,
            buffer_radius_km=buffer_radius_km,
            start_date=start_date,
            end_date=end_date,
            interval_years=interval_years
        )
        
        return jsonify(result), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/carbon-footprint', methods=['POST'])
def carbon_footprint():
    """
    Calculate carbon footprint and sequestration potential
    
    Accepts:
    - location: City name or coordinates (lat,lon)
    - buffer_radius_km: Optional buffer radius in km (default 2.0)
    - start_date: Optional start date (YYYY-MM-DD)
    - end_date: Optional end date (YYYY-MM-DD)
    
    Returns:
    - Dictionary with carbon calculations in rupees
    """
    try:
        data = request.get_json()
        
        if not data or 'location' not in data:
            return jsonify({'error': 'Location parameter is required'}), 400
        
        location = data['location'].strip()
        buffer_radius_km = float(data.get('buffer_radius_km', 2.0))
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if buffer_radius_km <= 0 or buffer_radius_km > 10:
            return jsonify({'error': 'Buffer radius must be between 0 and 10 km'}), 400
        
        result = system.calculate_carbon_footprint(
            location=location,
            buffer_radius_km=buffer_radius_km,
            start_date=start_date,
            end_date=end_date
        )
        
        return jsonify(result), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/economic-impact', methods=['POST'])
def economic_impact():
    """
    Calculate economic impact in Indian Rupees
    
    Accepts:
    - location: City name or coordinates (lat,lon)
    - buffer_radius_km: Optional buffer radius in km (default 2.0)
    - start_date: Optional start date (YYYY-MM-DD)
    - end_date: Optional end date (YYYY-MM-DD)
    - population: Optional population count
    
    Returns:
    - Dictionary with economic impact calculations in rupees
    """
    try:
        data = request.get_json()
        
        if not data or 'location' not in data:
            return jsonify({'error': 'Location parameter is required'}), 400
        
        location = data['location'].strip()
        buffer_radius_km = float(data.get('buffer_radius_km', 2.0))
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        population = data.get('population')
        
        if buffer_radius_km <= 0 or buffer_radius_km > 10:
            return jsonify({'error': 'Buffer radius must be between 0 and 10 km'}), 400
        
        result = system.calculate_economic_impact(
            location=location,
            buffer_radius_km=buffer_radius_km,
            start_date=start_date,
            end_date=end_date,
            population=population
        )
        
        return jsonify(result), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

