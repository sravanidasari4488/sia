import React, { useEffect } from 'react';
import { MapContainer, TileLayer, Circle, Popup, useMap, FeatureGroup } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import 'leaflet-draw/dist/leaflet.draw.css';
import 'leaflet-draw';

// Fix for default marker icons in React-Leaflet
import icon from 'leaflet/dist/images/marker-icon.png';
import iconShadow from 'leaflet/dist/images/marker-shadow.png';

let DefaultIcon = L.icon({
  iconUrl: icon,
  shadowUrl: iconShadow,
  iconSize: [25, 41],
  iconAnchor: [12, 41]
});

L.Marker.prototype.options.icon = DefaultIcon;

function MapController({ center, zoom }) {
  const map = useMap();
  
  useEffect(() => {
    if (center && zoom) {
      map.setView(center, zoom);
    }
  }, [center, zoom, map]);
  
  return null;
}

function DrawingControl({ onCreated }) {
  const map = useMap();

  useEffect(() => {
    const drawControl = new L.Control.Draw({
      draw: {
        polygon: {
          allowIntersection: false,
          showArea: true,
        },
        polyline: false,
        circle: false,
        marker: false,
        circlemarker: false,
        rectangle: true,
      }
    });

    map.addControl(drawControl);

    map.on(L.Draw.Event.CREATED, (e) => {
      const layer = e.layer;
      const geojson = layer.toGeoJSON();
      onCreated(geojson);
    });

    return () => {
      map.removeControl(drawControl);
      map.off(L.Draw.Event.CREATED);
    };
  }, [map, onCreated]);

  return null;
}

// Fix CSS variable usage in JSX
const getThemeColor = (varName, fallback) => {
  if (typeof window !== 'undefined') {
    const root = document.documentElement;
    const value = getComputedStyle(root).getPropertyValue(varName);
    return value.trim() || fallback;
  }
  return fallback;
};

function InteractiveMap({ lat, lon, landCover, bufferRadiusKm = 2.0, onPolygonDrawn }) {
  const position = [lat, lon];
  const radius = bufferRadiusKm * 1000; // Convert km to meters
  
  // Calculate colors based on land cover
  const getColorForLandCover = () => {
    if (!landCover) return '#667eea';
    
    const { urban, forest, vegetation, water } = landCover;
    
    if (water > 10) return '#3b82f6'; // Blue for water
    if (forest > 30) return '#10b981'; // Green for forest
    if (vegetation > 30) return '#84cc16'; // Light green for vegetation
    if (urban > 50) return '#8b5cf6'; // Purple for urban
    return '#f59e0b'; // Orange for mixed
  };
  
  const circleColor = getColorForLandCover();
  
  return (
    <div style={{ width: '100%', height: '500px', borderRadius: '12px', overflow: 'hidden', border: '2px solid #e5e7eb' }}>
      <MapContainer
        center={position}
        zoom={13}
        style={{ height: '100%', width: '100%' }}
        scrollWheelZoom={true}
      >
        <MapController center={position} zoom={13} />
        
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        
        {/* Satellite imagery option */}
        <TileLayer
          attribution='&copy; <a href="https://www.esri.com/">Esri</a>'
          url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
          opacity={0.7}
        />
        
        {/* Drawing Tools */}
        <FeatureGroup>
          <DrawingControl onCreated={onPolygonDrawn} />
        </FeatureGroup>

        {/* Analysis area circle */}
        <Circle
          center={position}
          radius={radius}
          pathOptions={{
            color: circleColor,
            fillColor: circleColor,
            fillOpacity: 0.2,
            weight: 3
          }}
        >
          <Popup>
            <div style={{ minWidth: '200px' }}>
              <h3 style={{ margin: '0 0 10px 0', fontSize: '16px', fontWeight: '600' }}>
                Analysis Area
              </h3>
              {landCover && (
                <div style={{ fontSize: '14px', lineHeight: '1.6' }}>
                  <div><strong>Urban:</strong> {landCover.urban?.toFixed(1)}%</div>
                  <div><strong>Forest:</strong> {landCover.forest?.toFixed(1)}%</div>
                  <div><strong>Vegetation:</strong> {landCover.vegetation?.toFixed(1)}%</div>
                  <div><strong>Water:</strong> {landCover.water?.toFixed(1)}%</div>
                </div>
              )}
              <div style={{ marginTop: '8px', fontSize: '12px', color: '#6b7280' }}>
                Radius: {bufferRadiusKm} km
              </div>
            </div>
          </Popup>
        </Circle>
      </MapContainer>
    </div>
  );
}

export default InteractiveMap;

