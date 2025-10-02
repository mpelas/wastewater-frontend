import json
import functions_framework
from shapely.geometry import Point, shape
from flask import jsonify
from google.cloud import storage


# Constants for the Cloud Function.
GCS_BUCKET_NAME = "mpelas-wastewater-bucket"
PERIFEREIES_GEOJSON_PATH = "perifereiesWGS84.geojson"  # Not used for this function, but kept
OUTPUT_GEOJSON_PATH = "no_swim_zones/wastewater_no_swim_zones.geojson"

def get_gcs_blob(bucket_name, blob_name):
    """Retrieves a blob from Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    return bucket.blob(blob_name)
    
def load_geojson_from_gcs(bucket_name, file_path):
    """Loads and parses the GeoJSON file from GCS storage bucket."""
    try:
        blob = get_gcs_blob(bucket_name, file_path)
        geojson_data_text = blob.download_as_text()
        
        geojson_dict = json.loads(geojson_data_text)
        
        print(f"==== Successfully loaded GeoJSON from gs://{bucket_name}/{file_path}")
        return geojson_dict 
        
    except Exception as e:
        print(f"FATAL ERROR: Failed to load GeoJSON from GCS: {e}")
        raise


# Load GeoJSON data at module level (runs once when the function cold starts)
print("==== STARTING GEOJSON LOAD ====")
try:
    geojson_data = load_geojson_from_gcs(GCS_BUCKET_NAME, OUTPUT_GEOJSON_PATH)
    print(f"==== GEOJSON LOADED: {len(geojson_data.get('features', []))} features ====")
except Exception as e:
    print(f"==== CRITICAL: Failed to load GeoJSON at startup: {e} ====")
    # Set to None so the service can start, but requests will fail gracefully
    geojson_data = None


def check_no_swim_zone(latitude, longitude):
    """Check if a point is inside any no-swim zone and return zone details"""
    point = Point(longitude, latitude)  # GeoJSON uses [longitude, latitude] order
    
    # Check if geojson_data has a 'features' key and is not None
    if geojson_data is None or "features" not in geojson_data:
        return False, None
        
    for feature in geojson_data["features"]:
        zone = shape(feature["geometry"])
        if point.within(zone):
            return True, feature
    
    return False, None


@functions_framework.http
def check_swim_zone(request):
    """HTTP Cloud Function to check if coordinates are in a no-swimming zone"""
    
    # Handle CORS
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    # Set CORS headers for actual request
    headers = {'Access-Control-Allow-Origin': '*'}
    
    try:
        # Get parameters from request
        if request.method == 'GET':
            latitude = request.args.get('latitude', type=float)
            longitude = request.args.get('longitude', type=float)
        elif request.method == 'POST':
            request_json = request.get_json()
            if request_json:
                latitude = request_json.get('latitude')
                longitude = request_json.get('longitude')
            else:
                return jsonify({'error': 'Invalid JSON'}), 400, headers
        else:
            return jsonify({'error': 'Method not allowed'}), 405, headers
            
        if latitude is None or longitude is None:
            return jsonify({'error': 'Missing latitude or longitude parameters'}), 400, headers
        
        # Check if point is in no-swim zone
        in_zone, zone_details = check_no_swim_zone(latitude, longitude)
        
        result = {
            'in_no_swim_zone': in_zone,
            'coordinates': {
                'latitude': latitude,
                'longitude': longitude
            }
        }
        
        # If in a no-swim zone, include full zone details
        if in_zone and zone_details:
            result['zone_details'] = zone_details['properties']
            result['zone_geometry'] = zone_details['geometry']
            
            # Highlight compliance status
            compliance = zone_details['properties'].get('Column1.compliance', None)
            if compliance is False:
                result['compliance_warning'] = "⚠️ NON-COMPLIANT ZONE - Column1.compliance: false"
                result['compliance_status'] = "NON_COMPLIANT"
            else:
                result['compliance_status'] = "COMPLIANT" if compliance else "UNKNOWN"
        
        return jsonify(result), 200, headers
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500, headers