import React, { useState } from 'react';
import { Routes, Route, Link } from 'react-router-dom';
import { useTheme } from './contexts/ThemeContext';
import './App.css';
import axios from 'axios';
import GoogleMaps from './components/UI/GoogleMaps';
import InteractiveMap from './components/UI/InteractiveMap';
import UseCases from './pages/UseCases';
import ThemeToggle from './components/UI/ThemeToggle';
import AIInsights from './components/Analysis/AIInsights';
import TimeSeriesChart from './components/Analysis/TimeSeriesChart';
import CarbonFootprint from './components/Analysis/CarbonFootprint';
import EconomicImpact from './components/Analysis/EconomicImpact';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:5000';

function AnalysisPage() {
  const { themes } = useTheme();
  const [city, setCity] = useState('');
  const [locality, setLocality] = useState('');
  const [localities, setLocalities] = useState([]);
  const [loadingLocalities, setLoadingLocalities] = useState(false);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [cityResult, setCityResult] = useState(null);
  const [polygonResult, setPolygonResult] = useState(null);
  const [analyzingPolygon, setAnalyzingPolygon] = useState(false);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState('analysis'); // 'analysis' or 'satellite'

  const handleCitySubmit = async (e) => {
    e.preventDefault();
    
    if (!city.trim()) {
      setError('Please enter a city name');
      return;
    }

    setLoadingLocalities(true);
    setError(null);
    setLocalities([]);
    setLocality('');
    setResult(null);

    try {
      // 1. Fetch localities
      const locResponse = await axios.post(`${API_URL}/localities`, {
        city: city.trim(),
        radius_km: 8
      });
      
      setLocalities(locResponse.data.localities || []);
      if (locResponse.data.localities.length === 0) {
        setError('No localities found for this city. Try a different city name.');
      }

      // 2. Fetch city-wide analysis (including risk score)
      const cityResponse = await axios.post(`${API_URL}/analyze-city`, {
        city: city.trim()
      });
      setCityResult(cityResponse.data);

    } catch (err) {
      setError(
        err.response?.data?.error || 
        err.message || 
        'Failed to fetch localities. Please try again.'
      );
    } finally {
      setLoadingLocalities(false);
    }
  };

  const handlePolygonDrawn = async (geojson) => {
    setAnalyzingPolygon(true);
    setPolygonResult(null);
    setError(null);

    try {
      const response = await axios.post(`${API_URL}/analyze-polygon`, {
        geometry: geojson.geometry
      });
      setPolygonResult(response.data);
      // Switch to satellite tab to show polygon results if not already there
      setActiveTab('satellite');
    } catch (err) {
      setError(
        err.response?.data?.error ||
        err.message ||
        'Failed to analyze custom area. Please try again.'
      );
    } finally {
      setAnalyzingPolygon(false);
    }
  };

  const handleLocalityAnalyze = async (e) => {
    e.preventDefault();
    
    if (!city.trim() || !locality.trim()) {
      setError('Please select a locality');
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const response = await axios.post(`${API_URL}/analyze`, {
        city: city.trim(),
        locality: locality.trim()
      });
      
      setResult(response.data);
    } catch (err) {
      setError(
        err.response?.data?.error || 
        err.message || 
        'Failed to analyze locality. Please try again.'
      );
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="App">
      <div className="container">
        <header className="header" style={{ background: themes.colors.headerGradient }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '15px' }}>
            <div style={{ flex: 1 }}>
              <h1 className="title">
                <span className="icon">🛰️</span>
                Geospatial Intelligence System
              </h1>
              <p className="subtitle">
                Real-time satellite-based land cover analysis and climate risk assessment
              </p>
            </div>
            <ThemeToggle />
          </div>
          <div style={{ marginTop: '20px', display: 'flex', gap: '15px', flexWrap: 'wrap' }}>
            <Link 
              to="/use-cases" 
              style={{
                color: 'white',
                textDecoration: 'none',
                padding: '10px 20px',
                background: 'rgba(255, 255, 255, 0.2)',
                borderRadius: '8px',
                fontWeight: '600',
                transition: 'all 0.3s ease',
                display: 'inline-block'
              }}
              onMouseEnter={(e) => {
                e.target.style.background = 'rgba(255, 255, 255, 0.3)';
              }}
              onMouseLeave={(e) => {
                e.target.style.background = 'rgba(255, 255, 255, 0.2)';
              }}
            >
              📚 View Use Cases
            </Link>
          </div>
        </header>

        <form onSubmit={handleCitySubmit} className="search-form">
          <div className="input-group">
            <input
              type="text"
              value={city}
              onChange={(e) => setCity(e.target.value)}
              placeholder="Enter city name (e.g., Hyderabad, Delhi)"
              className="location-input"
              disabled={loadingLocalities || loading}
            />
            <button 
              type="submit" 
              className="submit-button"
              disabled={loadingLocalities || loading}
            >
              {loadingLocalities ? (
                <>
                  <span className="spinner"></span>
                  Loading...
                </>
              ) : (
                <>
                  <span>🔍</span>
                  Find Localities
                </>
              )}
            </button>
          </div>
        </form>

        {localities.length > 0 && (
          <form onSubmit={handleLocalityAnalyze} className="search-form">
            <div className="input-group">
              <select
                value={locality}
                onChange={(e) => setLocality(e.target.value)}
                className="location-input"
                disabled={loading}
                style={{ padding: '12px', fontSize: '16px', borderRadius: '8px', border: '1px solid #ddd' }}
              >
                <option value="">Select a locality...</option>
                {localities.map((loc, index) => (
                  <option key={index} value={loc.name}>
                    {loc.name}
                  </option>
                ))}
              </select>
              <button 
                type="submit" 
                className="submit-button"
                disabled={loading || !locality}
              >
                {loading ? (
                  <>
                    <span className="spinner"></span>
                    Analyzing...
                  </>
                ) : (
                  <>
                    <span>🔍</span>
                    Analyze
                  </>
                )}
              </button>
            </div>
          </form>
        )}

        {error && (
          <div className="error-message">
            <span className="error-icon">⚠️</span>
            {error}
          </div>
        )}

        {cityResult && (
          <div className="result-section" style={{ border: '2px solid #6366f1', background: '#f5f3ff' }}>
            <h2 className="section-title" style={{ color: '#4338ca' }}>🏙️ City-wide Urban Risk Analysis: {cityResult.city}</h2>
            <div className="risk-grid">
              <div className="risk-item">
                <div className="risk-header">
                  <span className="risk-icon">📊</span>
                  <span className="land-cover-label">Urban Risk Score</span>
                </div>
                <div
                  className="risk-value"
                  style={{
                    color: cityResult.urbanisation_risk.risk_level === 'Critical' || cityResult.urbanisation_risk.risk_level === 'High' ? '#ef4444' :
                           cityResult.urbanisation_risk.risk_level === 'Moderate' ? '#f59e0b' : '#10b981'
                  }}
                >
                  {(cityResult.urbanisation_risk.urs_score * 100).toFixed(1)} ({cityResult.urbanisation_risk.risk_level})
                </div>
              </div>

              <div className="risk-item">
                <div className="risk-header">
                  <span className="risk-icon">🌱</span>
                  <span className="land-cover-label">Sustainability Index (ESI)</span>
                </div>
                <div
                  className="risk-value"
                  style={{
                    color: cityResult.esi.rating === 'Poor' ? '#ef4444' :
                           cityResult.esi.rating === 'Fair' ? '#f59e0b' : '#10b981'
                  }}
                >
                  {(cityResult.esi.esi_score * 100).toFixed(1)} ({cityResult.esi.rating})
                </div>
              </div>

              <div className="risk-item">
                <div className="risk-header">
                  <span className="risk-icon">🏙️</span>
                  <span className="land-cover-label">City Urbanization</span>
                </div>
                <div className="risk-value">{cityResult.percentages.urban.toFixed(1)}%</div>
              </div>
            </div>
            <div style={{ fontSize: '12px', color: '#6b7280', marginTop: '10px', fontStyle: 'italic' }}>
              * Based on city-wide administrative boundary analysis.
            </div>
          </div>
        )}

        {result && (
          <div className="results">
            {/* Tabs for Analysis Results and Satellite View */}
            <div className="tabs-container" style={{
              marginBottom: '20px',
              borderBottom: '2px solid #e5e7eb',
              display: 'flex',
              gap: '0'
            }}>
              <button
                onClick={() => setActiveTab('analysis')}
                style={{
                  flex: 1,
                  padding: '12px 20px',
                  border: 'none',
                  background: 'transparent',
                  borderBottom: activeTab === 'analysis' ? '3px solid #6366f1' : '3px solid transparent',
                  color: activeTab === 'analysis' ? '#6366f1' : '#6b7280',
                  fontWeight: activeTab === 'analysis' ? '600' : '400',
                  cursor: 'pointer',
                  fontSize: '16px',
                  transition: 'all 0.2s'
                }}
              >
                📊 Analysis Results
              </button>
              <button
                onClick={() => setActiveTab('satellite')}
                style={{
                  flex: 1,
                  padding: '12px 20px',
                  border: 'none',
                  background: 'transparent',
                  borderBottom: activeTab === 'satellite' ? '3px solid #6366f1' : '3px solid transparent',
                  color: activeTab === 'satellite' ? '#6366f1' : '#6b7280',
                  fontWeight: activeTab === 'satellite' ? '600' : '400',
                  cursor: 'pointer',
                  fontSize: '16px',
                  transition: 'all 0.2s'
                }}
              >
                🛰️ Satellite View
              </button>
            </div>

            {/* Analysis Results Tab */}
            {activeTab === 'analysis' && (
              <>
            <div className="result-section">
              <h2 className="section-title">📍 Location Information</h2>
              <div className="info-grid">
                <div className="info-item">
                  <span className="info-label">City:</span>
                  <span className="info-value">{result.city}</span>
                </div>
                <div className="info-item">
                  <span className="info-label">Locality:</span>
                  <span className="info-value">{result.locality}</span>
                </div>
                <div className="info-item">
                  <span className="info-label">Satellite Source:</span>
                  <span className="info-value">{result.satellite_source}</span>
                </div>
              </div>
            </div>

            <div className="result-section">
              <h2 className="section-title">🌍 Land Cover Classification</h2>
              <div className="land-cover-grid">
                <div className="land-cover-item">
                  <div className="land-cover-header">
                    <span className="land-cover-icon">🏙️</span>
                    <span className="land-cover-label">Urban</span>
                  </div>
                  <div className="land-cover-value">{result.landcover_percentages.urban.toFixed(2)}%</div>
                  <div className="progress-bar">
                    <div 
                      className="progress-fill urban" 
                      style={{ width: `${result.landcover_percentages.urban}%` }}
                    ></div>
                  </div>
                </div>

                <div className="land-cover-item">
                  <div className="land-cover-header">
                    <span className="land-cover-icon">🌲</span>
                    <span className="land-cover-label">Forest</span>
                  </div>
                  <div className="land-cover-value">{result.landcover_percentages.forest.toFixed(2)}%</div>
                  <div className="progress-bar">
                    <div 
                      className="progress-fill forest" 
                      style={{ width: `${result.landcover_percentages.forest}%` }}
                    ></div>
                  </div>
                </div>

                <div className="land-cover-item">
                  <div className="land-cover-header">
                    <span className="land-cover-icon">🌿</span>
                    <span className="land-cover-label">Vegetation</span>
                  </div>
                  <div className="land-cover-value">{result.landcover_percentages.vegetation.toFixed(2)}%</div>
                  <div className="progress-bar">
                    <div 
                      className="progress-fill vegetation" 
                      style={{ width: `${result.landcover_percentages.vegetation}%` }}
                    ></div>
                  </div>
                </div>

                <div className="land-cover-item">
                  <div className="land-cover-header">
                    <span className="land-cover-icon">💧</span>
                    <span className="land-cover-label">Water</span>
                  </div>
                  <div className="land-cover-value">{result.landcover_percentages.water.toFixed(2)}%</div>
                  <div className="progress-bar">
                    <div 
                      className="progress-fill water" 
                      style={{ width: `${result.landcover_percentages.water}%` }}
                    ></div>
                  </div>
                </div>
              </div>
            </div>

            {result.weather && (
              <div className="result-section">
                <h2 className="section-title">🌤️ Weather Data</h2>
                <div className="weather-grid">
                  <div className="weather-item">
                    <span className="weather-icon">🌡️</span>
                    <div className="weather-details">
                      <span className="weather-label">Temperature</span>
                      <span className="weather-value">{result.weather.temperature}°C</span>
                    </div>
                  </div>
                  <div className="weather-item">
                    <span className="weather-icon">🌧️</span>
                    <div className="weather-details">
                      <span className="weather-label">Rainfall</span>
                      <span className="weather-value">{result.weather.rainfall} mm</span>
                    </div>
                  </div>
                  <div className="weather-item">
                    <span className="weather-icon">💨</span>
                    <div className="weather-details">
                      <span className="weather-label">Humidity</span>
                      <span className="weather-value">{result.weather.humidity}%</span>
                    </div>
                  </div>
                  <div className="weather-item">
                    <span className="weather-icon">💨</span>
                    <div className="weather-details">
                      <span className="weather-label">Wind Speed</span>
                      <span className="weather-value">{result.weather.wind_speed} m/s</span>
                    </div>
                  </div>
                  <div className="weather-item">
                    <span className="weather-icon">📊</span>
                    <div className="weather-details">
                      <span className="weather-label">Pressure</span>
                      <span className="weather-value">{result.weather.pressure} hPa</span>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {result.air_quality && (
              <div className="result-section">
                <h2 className="section-title">🌬️ Air Quality</h2>
                
                {result.air_quality.error ? (
                  <div style={{
                    padding: '16px',
                    background: '#fef2f2',
                    borderRadius: '8px',
                    border: '2px solid #ef4444',
                    color: '#991b1b'
                  }}>
                    <div style={{ fontWeight: 'bold', marginBottom: '8px' }}>⚠️ Air Quality Data Unavailable</div>
                    <div style={{ fontSize: '14px' }}>{result.air_quality.error}</div>
                    <div style={{ fontSize: '12px', marginTop: '8px', color: '#6b7280' }}>
                      This may be due to network connectivity issues or API service unavailability.
                    </div>
                  </div>
                ) : (result.air_quality.us_aqi && result.air_quality.us_aqi.value !== null) ? (
                  <>
                    {/* AQI Display Section */}
                <div style={{ marginBottom: '24px' }}>
                  {/* US AQI */}
                  {result.air_quality.us_aqi && result.air_quality.us_aqi.value !== null && (
                    <div style={{ 
                      padding: '20px',
                      background: result.air_quality.us_aqi.value >= 301 ? '#fef2f2' : 
                                 result.air_quality.us_aqi.value >= 201 ? '#fffbeb' : 
                                 result.air_quality.us_aqi.value >= 151 ? '#fff7ed' : 
                                 result.air_quality.us_aqi.value >= 101 ? '#fefce8' : '#f0fdf4',
                      borderRadius: '12px',
                      border: `2px solid ${
                        result.air_quality.us_aqi.value >= 301 ? '#ef4444' : 
                        result.air_quality.us_aqi.value >= 201 ? '#f59e0b' : 
                        result.air_quality.us_aqi.value >= 151 ? '#fb923c' :
                        result.air_quality.us_aqi.value >= 101 ? '#eab308' : '#10b981'
                      }`,
                      marginBottom: '16px'
                    }}>
                      <div style={{ 
                        fontSize: '28px', 
                        fontWeight: '700',
                        color: result.air_quality.us_aqi.value >= 301 ? '#ef4444' : 
                               result.air_quality.us_aqi.value >= 201 ? '#f59e0b' : 
                               result.air_quality.us_aqi.value >= 151 ? '#fb923c' :
                               result.air_quality.us_aqi.value >= 101 ? '#eab308' : '#10b981',
                        marginBottom: '12px'
                      }}>
                        US AQI: {result.air_quality.us_aqi.value} ({result.air_quality.us_aqi.category})
                      </div>
                      
                      <div style={{ 
                        fontSize: '15px', 
                        color: '#4b5563',
                        fontWeight: '600',
                        marginBottom: '16px',
                        paddingBottom: '12px',
                        borderBottom: '1px solid #e5e7eb'
                      }}>
                        Dominant Pollutant: {result.air_quality.us_aqi.dominant_pollutant || result.air_quality.dominant_pollutant || 'PM2.5'}
                      </div>
                      
                      {result.air_quality.health_advisory && (
                        <div style={{ 
                          fontSize: '14px', 
                          color: '#374151',
                          padding: '12px',
                          background: 'rgba(255, 255, 255, 0.7)',
                          borderRadius: '8px',
                          marginTop: '8px'
                        }}>
                          {result.air_quality.health_advisory}
                        </div>
                      )}
                    </div>
                  )}
                  
                  {/* Transparency Note */}
                  {result.air_quality.transparency_note && (
                    <div style={{ 
                      fontSize: '12px', 
                      color: '#6b7280',
                      padding: '10px 12px',
                      background: '#f9fafb',
                      borderRadius: '8px',
                      marginBottom: '16px',
                      fontStyle: 'italic',
                      border: '1px solid #e5e7eb'
                    }}>
                      ℹ️ {result.air_quality.transparency_note}
                    </div>
                  )}
                  
                  {/* AQI Legend */}
                  <div style={{
                    marginTop: '20px',
                    padding: '24px',
                    background: '#f8f9fa',
                    borderRadius: '16px',
                    border: '2px solid #e9ecef',
                    transition: 'all 0.3s ease'
                  }}>
                    <div style={{ 
                      fontSize: '1.125rem', 
                      fontWeight: '600', 
                      marginBottom: '20px',
                      color: '#2d3748',
                      paddingBottom: '12px',
                      borderBottom: '2px solid #e9ecef'
                    }}>
                      AQI Scale (US EPA)
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                      {/* Good */}
                      <div style={{ 
                        display: 'flex', 
                        alignItems: 'flex-start', 
                        gap: '16px',
                        padding: '16px',
                        background: 'white',
                        borderRadius: '12px',
                        border: '1px solid #e9ecef',
                        transition: 'all 0.3s ease'
                      }}>
                        <div style={{
                          width: '24px',
                          height: '24px',
                          backgroundColor: '#10b981',
                          borderRadius: '6px',
                          flexShrink: 0,
                          marginTop: '2px',
                          boxShadow: '0 2px 4px rgba(16, 185, 129, 0.2)'
                        }}></div>
                        <div style={{ flex: 1 }}>
                          <div style={{ fontWeight: '600', marginBottom: '6px', color: '#2d3748', fontSize: '1rem' }}>Good (0 to 50)</div>
                          <div style={{ fontSize: '0.875rem', color: '#718096', lineHeight: '1.5' }}>
                            Air quality is satisfactory. Air pollution poses little or no risk.
                          </div>
                        </div>
                      </div>
                      
                      {/* Moderate */}
                      <div style={{ 
                        display: 'flex', 
                        alignItems: 'flex-start', 
                        gap: '16px',
                        padding: '16px',
                        background: 'white',
                        borderRadius: '12px',
                        border: '1px solid #e9ecef',
                        transition: 'all 0.3s ease'
                      }}>
                        <div style={{
                          width: '24px',
                          height: '24px',
                          backgroundColor: '#fbbf24',
                          borderRadius: '6px',
                          flexShrink: 0,
                          marginTop: '2px',
                          boxShadow: '0 2px 4px rgba(251, 191, 36, 0.2)'
                        }}></div>
                        <div style={{ flex: 1 }}>
                          <div style={{ fontWeight: '600', marginBottom: '6px', color: '#2d3748', fontSize: '1rem' }}>Moderate (51 to 100)</div>
                          <div style={{ fontSize: '0.875rem', color: '#718096', lineHeight: '1.5' }}>
                            Air quality is acceptable. However, there may be a risk for some people, particularly those who are unusually sensitive to air pollution.
                          </div>
                        </div>
                      </div>
                      
                      {/* Poor */}
                      <div style={{ 
                        display: 'flex', 
                        alignItems: 'flex-start', 
                        gap: '16px',
                        padding: '16px',
                        background: 'white',
                        borderRadius: '12px',
                        border: '1px solid #e9ecef',
                        transition: 'all 0.3s ease'
                      }}>
                        <div style={{
                          width: '24px',
                          height: '24px',
                          backgroundColor: '#fb923c',
                          borderRadius: '6px',
                          flexShrink: 0,
                          marginTop: '2px',
                          boxShadow: '0 2px 4px rgba(251, 146, 60, 0.2)'
                        }}></div>
                        <div style={{ flex: 1 }}>
                          <div style={{ fontWeight: '600', marginBottom: '6px', color: '#2d3748', fontSize: '1rem' }}>Unhealthy for Sensitive Groups (101 to 150)</div>
                          <div style={{ fontSize: '0.875rem', color: '#718096', lineHeight: '1.5' }}>
                            Members of sensitive groups may experience health effects. The general public is less likely to be affected.
                          </div>
                        </div>
                      </div>
                      
                      {/* Unhealthy */}
                      <div style={{ 
                        display: 'flex', 
                        alignItems: 'flex-start', 
                        gap: '16px',
                        padding: '16px',
                        background: 'white',
                        borderRadius: '12px',
                        border: '1px solid #e9ecef',
                        transition: 'all 0.3s ease'
                      }}>
                        <div style={{
                          width: '24px',
                          height: '24px',
                          backgroundColor: '#f472b6',
                          borderRadius: '6px',
                          flexShrink: 0,
                          marginTop: '2px',
                          boxShadow: '0 2px 4px rgba(244, 114, 182, 0.2)'
                        }}></div>
                        <div style={{ flex: 1 }}>
                          <div style={{ fontWeight: '600', marginBottom: '6px', color: '#2d3748', fontSize: '1rem' }}>Unhealthy (151 to 200)</div>
                          <div style={{ fontSize: '0.875rem', color: '#718096', lineHeight: '1.5' }}>
                            Some members of the general public may experience health effects; members of sensitive groups may experience more serious health effects.
                          </div>
                        </div>
                      </div>
                      
                      {/* Severe */}
                      <div style={{ 
                        display: 'flex', 
                        alignItems: 'flex-start', 
                        gap: '16px',
                        padding: '16px',
                        background: 'white',
                        borderRadius: '12px',
                        border: '1px solid #e9ecef',
                        transition: 'all 0.3s ease'
                      }}>
                        <div style={{
                          width: '24px',
                          height: '24px',
                          backgroundColor: '#a855f7',
                          borderRadius: '6px',
                          flexShrink: 0,
                          marginTop: '2px',
                          boxShadow: '0 2px 4px rgba(168, 85, 247, 0.2)'
                        }}></div>
                        <div style={{ flex: 1 }}>
                          <div style={{ fontWeight: '600', marginBottom: '6px', color: '#2d3748', fontSize: '1rem' }}>Very Unhealthy (201 to 300)</div>
                          <div style={{ fontSize: '0.875rem', color: '#718096', lineHeight: '1.5' }}>
                            Health alert: The risk of health effects is increased for everyone. Everyone may begin to experience health effects.
                          </div>
                        </div>
                      </div>
                      
                      {/* Hazardous */}
                      <div style={{ 
                        display: 'flex', 
                        alignItems: 'flex-start', 
                        gap: '16px',
                        padding: '16px',
                        background: 'white',
                        borderRadius: '12px',
                        border: '1px solid #e9ecef',
                        transition: 'all 0.3s ease'
                      }}>
                        <div style={{
                          width: '24px',
                          height: '24px',
                          backgroundColor: '#ef4444',
                          borderRadius: '6px',
                          flexShrink: 0,
                          marginTop: '2px',
                          boxShadow: '0 2px 4px rgba(239, 68, 68, 0.2)'
                        }}></div>
                        <div style={{ flex: 1 }}>
                          <div style={{ fontWeight: '600', marginBottom: '6px', color: '#2d3748', fontSize: '1rem' }}>Hazardous (301 to 500)</div>
                          <div style={{ fontSize: '0.875rem', color: '#718096', lineHeight: '1.5' }}>
                            Health warning of emergency conditions: everyone is more likely to be affected. Avoid all outdoor activities.
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
                
                {/* Supporting Pollutants */}
                {result.air_quality.pollutants && (
                  <>
                    <div style={{ 
                      fontSize: '1rem', 
                      fontWeight: '600',
                      color: '#2d3748',
                      marginTop: '24px',
                      marginBottom: '16px',
                      paddingBottom: '8px',
                      borderBottom: '2px solid #e9ecef'
                    }}>
                      Other pollutants (for reference)
                    </div>
                    <div style={{ 
                      fontSize: '12px', 
                      color: '#6b7280',
                      marginBottom: '12px',
                      fontStyle: 'italic'
                    }}>
                      Data source: {result.air_quality.data_source || 'OpenWeather'}
                    </div>
                    <div className="weather-grid">
                      {result.air_quality.pollutants.pm25 && result.air_quality.pollutants.pm25.value !== null && (
                        <div className="weather-item">
                          <span className="weather-icon">💨</span>
                          <div className="weather-details">
                            <span className="weather-label">PM2.5</span>
                            <span className="weather-value">
                              {result.air_quality.pollutants.pm25.value} {result.air_quality.pollutants.pm25.unit}
                            </span>
                          </div>
                        </div>
                      )}
                      {result.air_quality.pollutants.pm10 && result.air_quality.pollutants.pm10.value !== null && (
                        <div className="weather-item">
                          <span className="weather-icon">💨</span>
                          <div className="weather-details">
                            <span className="weather-label">PM10</span>
                            <span className="weather-value">
                              {result.air_quality.pollutants.pm10.value} {result.air_quality.pollutants.pm10.unit}
                            </span>
                          </div>
                        </div>
                      )}
                      {result.air_quality.pollutants.co && result.air_quality.pollutants.co.value !== null && (
                        <div className="weather-item">
                          <span className="weather-icon">🏭</span>
                          <div className="weather-details">
                            <span className="weather-label">CO</span>
                            <span className="weather-value">
                              {result.air_quality.pollutants.co.value} {result.air_quality.pollutants.co.unit}
                            </span>
                          </div>
                        </div>
                      )}
                      {result.air_quality.pollutants.no2 && result.air_quality.pollutants.no2.value !== null && (
                        <div className="weather-item">
                          <span className="weather-icon">🏭</span>
                          <div className="weather-details">
                            <span className="weather-label">NO₂</span>
                            <span className="weather-value">
                              {result.air_quality.pollutants.no2.value} {result.air_quality.pollutants.no2.unit}
                            </span>
                          </div>
                        </div>
                      )}
                      {result.air_quality.pollutants.so2 && result.air_quality.pollutants.so2.value !== null && (
                        <div className="weather-item">
                          <span className="weather-icon">🏭</span>
                          <div className="weather-details">
                            <span className="weather-label">SO₂</span>
                            <span className="weather-value">
                              {result.air_quality.pollutants.so2.value} {result.air_quality.pollutants.so2.unit}
                            </span>
                          </div>
                        </div>
                      )}
                      {result.air_quality.pollutants.o3 && result.air_quality.pollutants.o3.value !== null && (
                        <div className="weather-item">
                          <span className="weather-icon">☁️</span>
                          <div className="weather-details">
                            <span className="weather-label">O₃</span>
                            <span className="weather-value">
                              {result.air_quality.pollutants.o3.value} {result.air_quality.pollutants.o3.unit}
                            </span>
                          </div>
                        </div>
                      )}
                    </div>
                  </>
                )}
                  </>
                ) : (
                  <div style={{
                    padding: '16px',
                    background: '#fffbeb',
                    borderRadius: '8px',
                    border: '2px solid #f59e0b',
                    color: '#92400e'
                  }}>
                    <div style={{ fontWeight: 'bold', marginBottom: '8px' }}>⚠️ Air Quality Data Not Available</div>
                    <div style={{ fontSize: '14px' }}>
                      Unable to fetch air quality data at this time. Please try again later.
                    </div>
                  </div>
                )}
                
                {result.air_quality.urban_density_note && (
                  <div style={{
                    marginTop: '16px',
                    padding: '12px',
                    background: '#eff6ff',
                    borderRadius: '8px',
                    border: '1px solid #bfdbfe',
                    fontSize: '14px',
                    color: '#1e40af'
                  }}>
                    <strong>Note:</strong> {result.air_quality.urban_density_note}
                  </div>
                )}
              </div>
            )}

            <div className="result-section">
              <h2 className="section-title">⚠️ Climate Risk Assessment</h2>
              <div className="risk-grid">
                <div className="risk-item">
                  <div className="risk-header">
                    <span className="risk-icon">🌊</span>
                    <span className="risk-label">Flood Risk</span>
                  </div>
                  <div 
                    className="risk-value"
                    style={{ 
                      color: result.flood_risk === 'High' ? '#ef4444' : 
                             result.flood_risk === 'Medium' ? '#f59e0b' : '#10b981' 
                    }}
                  >
                    {result.flood_risk}
                  </div>
                </div>

                <div className="risk-item">
                  <div className="risk-header">
                    <span className="risk-icon">🔥</span>
                    <span className="risk-label">Heat Risk</span>
                  </div>
                  <div 
                    className="risk-value"
                    style={{ 
                      color: result.heat_risk === 'High' ? '#ef4444' : 
                             result.heat_risk === 'Medium' ? '#f59e0b' : '#10b981' 
                    }}
                  >
                    {result.heat_risk}
                  </div>
                </div>

                <div className="risk-item">
                  <div className="risk-header">
                    <span className="risk-icon">🌵</span>
                    <span className="risk-label">Drought Risk</span>
                  </div>
                  <div 
                    className="risk-value"
                    style={{ 
                      color: result.drought_risk === 'High' ? '#ef4444' : 
                             result.drought_risk === 'Medium' ? '#f59e0b' : '#10b981' 
                    }}
                  >
                    {result.drought_risk}
                  </div>
                </div>
              </div>
            </div>

            {/* Weather Alerts Section */}
            {result.weather_alerts !== undefined && (
              <div className="result-section">
                <h2 className="section-title">🌦️ Weather Alerts</h2>
                {result.weather_alerts && result.weather_alerts.length > 0 ? (
                  <div className="disaster-grid">
                    {result.weather_alerts.map((alert, index) => {
                      // Map alert categories to icons
                      const alertIcons = {
                        'Heatwave': '🌡️',
                        'Heavy Rain': '🌧️',
                        'Flood': '🌊',
                        'Thunderstorm': '⛈️',
                        'Cold Wave': '🧊',
                        'Cyclone': '🌀',
                        'Weather': '🌦️'
                      };
                      const icon = alertIcons[alert.category] || '🌦️';
                      
                      // Map severity to colors
                      const severityColors = {
                        'extreme': '#ef4444',
                        'severe': '#f59e0b',
                        'moderate': '#fbbf24',
                        'minor': '#84cc16',
                        'unknown': '#6b7280'
                      };
                      const severityColor = severityColors[alert.severity?.toLowerCase()] || '#6b7280';
                      
                      return (
                        <div key={index} className="disaster-card">
                          <div className="disaster-header">
                            <span className="disaster-icon">{icon}</span>
                            <span className="disaster-title">{alert.title}</span>
                            <span className="disaster-severity" style={{ 
                              background: severityColor,
                              color: 'white'
                            }}>
                              {alert.severity || 'Alert'}
                            </span>
                          </div>
                          <div className="disaster-details">
                            <span className="disaster-type">{alert.category}</span>
                            {alert.duration && (
                              <span className="disaster-time">Duration: {alert.duration}</span>
                            )}
                          </div>
                          {alert.description && (
                            <div className="disaster-description">{alert.description}</div>
                          )}
                          {alert.start_time && alert.end_time && (
                            <div style={{ 
                              fontSize: '0.75rem', 
                              color: '#9ca3af',
                              marginTop: '8px'
                            }}>
                              {alert.start_time} - {alert.end_time}
                            </div>
                          )}
                          <div className="disaster-source">Source: {alert.source || 'OpenWeather'}</div>
                        </div>
                      );
                    })}
                  </div>
                ) : (
                  <div className="no-disasters">
                    <span className="no-disasters-icon">✅</span>
                    <span className="no-disasters-text">No active weather warnings for this location</span>
                  </div>
                )}
              </div>
            )}

            {/* Weather News Section */}
            {result.weather_news !== undefined && (
              <div className="result-section">
                <h2 className="section-title">🌍 Global Weather & Climate News</h2>
                {result.weather_news && result.weather_news.length > 0 ? (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                    {result.weather_news.map((news, index) => (
                      <div 
                        key={index}
                        style={{
                          padding: '16px',
                          background: 'white',
                          borderRadius: '12px',
                          border: '1px solid #e9ecef',
                          transition: 'all 0.3s ease',
                          cursor: news.url ? 'pointer' : 'default'
                        }}
                        onClick={() => news.url && window.open(news.url, '_blank')}
                        onMouseEnter={(e) => {
                          if (news.url) {
                            e.currentTarget.style.borderColor = '#667eea';
                            e.currentTarget.style.boxShadow = '0 2px 8px rgba(102, 126, 234, 0.1)';
                          }
                        }}
                        onMouseLeave={(e) => {
                          if (news.url) {
                            e.currentTarget.style.borderColor = '#e9ecef';
                            e.currentTarget.style.boxShadow = 'none';
                          }
                        }}
                      >
                        <div style={{ 
                          display: 'flex', 
                          justifyContent: 'space-between',
                          alignItems: 'flex-start',
                          gap: '12px'
                        }}>
                          <div style={{ flex: 1 }}>
                            <div style={{ 
                              fontSize: '15px', 
                              fontWeight: '600',
                              color: '#2d3748',
                              lineHeight: '1.4',
                              marginBottom: '8px'
                            }}>
                              {news.title}
                            </div>
                            <div style={{ 
                              display: 'flex', 
                              gap: '12px',
                              fontSize: '12px',
                              color: '#6b7280'
                            }}>
                              <span>{news.source}</span>
                              {news.published_at && (
                                <span>{news.published_at}</span>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div style={{
                    padding: '20px',
                    textAlign: 'center',
                    color: '#6b7280',
                    fontSize: '14px'
                  }}>
                    Weather news updates will appear here as they are published.
                  </div>
                )}
              </div>
            )}

            {result.disasters !== undefined && (
              <div className="result-section">
                <h2 className="section-title">🚨 Natural Disaster Alerts</h2>
                {result.disasters && result.disasters.length > 0 ? (
                  <div className="disaster-grid">
                    {result.disasters.map((disaster, index) => (
                      <div key={index} className="disaster-card" style={{
                        borderLeft: `4px solid ${
                          disaster.severity === 'High' ? '#ef4444' : 
                          disaster.severity === 'Medium' ? '#f59e0b' : '#10b981'
                        }`
                      }}>
                        <div className="disaster-header">
                          <span className="disaster-icon">
                            {disaster.type === 'earthquake' ? '🌍' : 
                             disaster.type === 'cyclone' ? '🌀' : '⚠️'}
                          </span>
                          <span className="disaster-title">{disaster.title}</span>
                          <span className="disaster-severity" style={{
                            color: disaster.severity === 'High' ? '#ef4444' : 
                                   disaster.severity === 'Medium' ? '#f59e0b' : '#10b981'
                          }}>
                            {disaster.severity}
                          </span>
                        </div>
                        <div className="disaster-details">
                          {disaster.distance_km > 0 && (
                            <span className="disaster-distance">{disaster.distance_km} km away</span>
                          )}
                          <span className="disaster-time">{disaster.time}</span>
                        </div>
                        {disaster.description && (
                          <div className="disaster-description">{disaster.description}</div>
                        )}
                        <div className="disaster-source">Source: {disaster.source}</div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="no-disasters">
                    <span className="no-disasters-icon">✅</span>
                    <span className="no-disasters-text">No active natural disasters near this locality</span>
                  </div>
                )}
              </div>
            )}

            {/* AI Insights */}
            {result.coordinates && (
              <AIInsights
                location={`${result.locality}, ${result.city}`}
                landCover={result.landcover_percentages}
                weatherData={result.weather}
                climateRisks={{
                  flood: result.flood_risk,
                  heat: result.heat_risk,
                  drought: result.drought_risk
                }}
                airQuality={result.air_quality}
                urbanisationRisk={result.urbanisation_risk}
              />
            )}

            {/* Time-Series Analysis */}
            {result.coordinates && (
              <TimeSeriesChart location={`${result.locality}, ${result.city}`} />
            )}

            {/* Carbon Footprint */}
            {result.coordinates && (
              <CarbonFootprint location={`${result.locality}, ${result.city}`} />
            )}

            {/* Economic Impact */}
            {result.coordinates && (
              <EconomicImpact 
                location={`${result.locality}, ${result.city}`}
                population={result.population}
              />
            )}
              </>
            )}

            {/* Satellite View Tab */}
            {activeTab === 'satellite' && (
              <div className="result-section">
                <h2 className="section-title">🛰️ Interactive Map & Custom Area Analysis</h2>
                <p style={{ marginBottom: '15px', color: '#6b7280' }}>
                  Use the drawing tools on the left to analyze a custom area (Polygon or Rectangle).
                </p>
                
                {/* Interactive Map */}
                {result.coordinates && result.landcover_percentages && (
                  <div style={{ marginBottom: '30px' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '15px' }}>
                      <h3 style={{ fontSize: '1.2rem', margin: 0, color: 'var(--theme-text)' }}>
                        Interactive Analysis Map
                      </h3>
                      {analyzingPolygon && (
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#6366f1', fontWeight: '600' }}>
                          <span className="spinner" style={{ width: '16px', height: '16px', border: '2px solid #6366f1', borderTopColor: 'transparent' }}></span>
                          Analyzing custom area...
                        </div>
                      )}
                    </div>
                    <InteractiveMap
                      lat={result.coordinates.lat}
                      lon={result.coordinates.lon}
                      landCover={result.landcover_percentages}
                      bufferRadiusKm={2.0}
                      onPolygonDrawn={handlePolygonDrawn}
                    />
                  </div>
                )}

                {/* Custom Polygon Result */}
                {polygonResult && (
                  <div className="result-section" style={{ border: '2px solid #10b981', background: '#f0fdf4', marginTop: '20px' }}>
                    <h3 className="section-title" style={{ color: '#059669' }}>📍 Custom Area Analysis</h3>
                    <div className="info-grid">
                      <div className="info-item">
                        <span className="info-label">Area:</span>
                        <span className="info-value">{polygonResult.area_km2} km²</span>
                      </div>
                      <div className="info-item">
                        <span className="info-label">Urban Risk Score:</span>
                        <span className="info-value" style={{
                          color: polygonResult.urbanisation_risk.risk_level === 'Critical' || polygonResult.urbanisation_risk.risk_level === 'High' ? '#ef4444' : '#059669'
                        }}>
                          {(polygonResult.urbanisation_risk.urs_score * 100).toFixed(1)} ({polygonResult.urbanisation_risk.risk_level})
                        </span>
                      </div>
                    </div>

                    <div className="land-cover-grid" style={{ marginTop: '15px' }}>
                      <div className="land-cover-item" style={{ background: 'white' }}>
                        <div className="land-cover-label">Urban</div>
                        <div className="land-cover-value">{polygonResult.landcover_percentages.urban}%</div>
                      </div>
                      <div className="land-cover-item" style={{ background: 'white' }}>
                        <div className="land-cover-label">Forest</div>
                        <div className="land-cover-value">{polygonResult.landcover_percentages.forest}%</div>
                      </div>
                      <div className="land-cover-item" style={{ background: 'white' }}>
                        <div className="land-cover-label">Vegetation</div>
                        <div className="land-cover-value">{polygonResult.landcover_percentages.vegetation}%</div>
                      </div>
                      <div className="land-cover-item" style={{ background: 'white' }}>
                        <div className="land-cover-label">Water</div>
                        <div className="land-cover-value">{polygonResult.landcover_percentages.water}%</div>
                      </div>
                    </div>

                    {polygonResult.carbon_analysis && (
                      <div style={{ marginTop: '20px', padding: '15px', background: 'white', borderRadius: '12px', border: '1px solid #d1fae5' }}>
                        <h4 style={{ margin: '0 0 10px 0', color: '#065f46', display: 'flex', alignItems: 'center', gap: '8px' }}>
                          🌱 Carbon Impact
                        </h4>
                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: '15px' }}>
                          <div>
                            <div style={{ fontSize: '12px', color: '#6b7280' }}>Net CO₂ Impact</div>
                            <div style={{ fontWeight: '600', color: polygonResult.carbon_analysis.net_carbon_impact.co2_per_year > 0 ? '#059669' : '#dc2626' }}>
                              {polygonResult.carbon_analysis.net_carbon_impact.co2_per_year.toFixed(2)} tonnes/year
                            </div>
                          </div>
                          <div>
                            <div style={{ fontSize: '12px', color: '#6b7280' }}>Economic Value</div>
                            <div style={{ fontWeight: '600', color: '#059669' }}>
                              ₹{Math.abs(polygonResult.carbon_analysis.net_carbon_impact.value_rupees_per_year).toLocaleString()} / year
                            </div>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {/* Google Maps Reference */}
                <div style={{ marginTop: '20px' }}>
                  <h3 style={{ fontSize: '1.2rem', marginBottom: '15px', color: 'var(--theme-text)' }}>
                    Reference Imagery
                  </h3>
                  <div style={{
                    padding: '20px',
                    background: '#f9fafb',
                    borderRadius: '8px',
                    border: '1px solid #e5e7eb'
                  }}>
                    {result.coordinates ? (
                      <GoogleMaps
                        lat={result.coordinates.lat}
                        lon={result.coordinates.lon}
                        localityName={`${result.locality}, ${result.city}`}
                      />
                    ) : (
                      <div style={{
                        textAlign: 'center',
                        padding: '40px',
                        color: '#6b7280'
                      }}>
                        <p>Coordinates not available for this location.</p>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function App() {
  return (
    <Routes>
      <Route path="/" element={<AnalysisPage />} />
      <Route path="/use-cases" element={<UseCases />} />
    </Routes>
  );
}

export default App;
