// State
let currentSegmentId = null;
let impressionsChart = null;
let minutesChart = null;
let devicesChart = null;

// Get form data
function getFormData() {
    const segmentName = document.getElementById('segment-name').value;
    
    const ageBands = Array.from(document.querySelectorAll('input[type="checkbox"][value*="-"]'))
        .filter(cb => cb.value.includes('-') || cb.value.includes('+'))
        .filter(cb => cb.checked)
        .map(cb => cb.value);
    
    const demographics = Array.from(document.querySelectorAll('input[type="checkbox"]'))
        .filter(cb => ['Urban', 'Suburban', 'Rural', 'High Income', 'College Educated'].includes(cb.value))
        .filter(cb => cb.checked)
        .map(cb => cb.value);
    
    const locations = Array.from(document.querySelectorAll('input[type="checkbox"]'))
        .filter(cb => ['United States', 'Canada', 'United Kingdom', 'Europe', 'Asia Pacific'].includes(cb.value))
        .filter(cb => cb.checked)
        .map(cb => cb.value);
    
    const interests = Array.from(document.querySelectorAll('input[type="checkbox"]'))
        .filter(cb => ['Technology', 'Sports', 'Travel', 'Fashion', 'Food & Dining', 'Entertainment'].includes(cb.value))
        .filter(cb => cb.checked)
        .map(cb => cb.value);
    
    const minEngagement = parseFloat(document.getElementById('min-engagement').value) || 0;
    
    return {
        segment_name: segmentName,
        age_bands: ageBands,
        demographics: demographics,
        locations: locations,
        interests: interests,
        min_engagement_minutes: minEngagement
    };
}

// Format numbers
function formatNumber(num) {
    return new Intl.NumberFormat().format(Math.round(num));
}

// Create segment
async function createSegment() {
    const data = getFormData();
    
    if (!data.segment_name) {
        alert('Please enter a segment name');
        return;
    }
    
    if (data.age_bands.length === 0 && data.demographics.length === 0 && 
        data.locations.length === 0 && data.interests.length === 0) {
        alert('Please select at least one targeting criterion');
        return;
    }
    
    // Show loading
    document.getElementById('loading').style.display = 'block';
    document.getElementById('create-segment-btn').disabled = true;
    
    try {
        // Create segment
        const response = await fetch('/api/segments', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(data)
        });
        
        if (!response.ok) throw new Error('Failed to create segment');
        
        const segment = await response.json();
        currentSegmentId = segment.segment_id;
        
        // Get analytics
        const analyticsResponse = await fetch(`/api/segments/${segment.segment_id}/analytics`);
        if (!analyticsResponse.ok) throw new Error('Failed to get analytics');
        
        const analytics = await analyticsResponse.json();
        
        // Display results
        displaySegmentInfo(segment);
        displayAnalytics(analytics, segment);
        
        // Show analytics section
        document.getElementById('analytics-section').style.display = 'block';
        document.getElementById('analytics-section').scrollIntoView({ behavior: 'smooth' });
        
    } catch (error) {
        console.error('Error:', error);
        alert('Error creating segment: ' + error.message);
    } finally {
        document.getElementById('loading').style.display = 'none';
        document.getElementById('create-segment-btn').disabled = false;
    }
}

// Display segment info
function displaySegmentInfo(segment) {
    const html = `
        <h3>${segment.segment_name}</h3>
        <p><strong>Age Bands:</strong> ${segment.age_bands.join(', ') || 'All'}</p>
        <p><strong>Demographics:</strong> ${segment.demographics.join(', ') || 'All'}</p>
        <p><strong>Locations:</strong> ${segment.locations.join(', ') || 'All'}</p>
        <p><strong>Interests:</strong> ${segment.interests.join(', ') || 'All'}</p>
    `;
    document.getElementById('segment-info').innerHTML = html;
}

// Display analytics
function displayAnalytics(analytics, segment) {
    // Calculate totals
    const prevTotal = analytics.previous_month.reduce((sum, day) => sum + day.impressions, 0);
    const predTotal = analytics.predicted_month.reduce((sum, day) => sum + day.impressions, 0);
    const avgMinutes = analytics.previous_month.reduce((sum, day) => sum + day.minutes, 0) / 30;
    
    // Update metrics
    document.getElementById('prev-impressions').textContent = formatNumber(prevTotal);
    document.getElementById('pred-impressions').textContent = formatNumber(predTotal);
    document.getElementById('avg-minutes').textContent = formatNumber(avgMinutes);
    document.getElementById('estimated-reach').textContent = formatNumber(segment.estimated_reach);
    
    // Create charts
    createImpressionsChart(analytics);
    createMinutesChart(analytics);
    createDevicesChart(analytics.previous_month[29].devices);
}

// Create impressions chart
function createImpressionsChart(analytics) {
    const ctx = document.getElementById('impressions-chart');
    
    if (impressionsChart) {
        impressionsChart.destroy();
    }
    
    const dates = analytics.previous_month.map(d => new Date(d.date).toLocaleDateString('en-US', {month: 'short', day: 'numeric'}));
    const predictedDates = analytics.predicted_month.map(d => new Date(d.date).toLocaleDateString('en-US', {month: 'short', day: 'numeric'}));
    
    impressionsChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [...dates, ...predictedDates],
            datasets: [
                {
                    label: 'Previous Month',
                    data: analytics.previous_month.map(d => d.impressions),
                    borderColor: '#e50914',
                    backgroundColor: 'rgba(229, 9, 20, 0.2)',
                    tension: 0.4,
                    fill: true
                },
                {
                    label: 'Predicted Next Month',
                    data: [...Array(30).fill(null), ...analytics.predicted_month.map(d => d.impressions)],
                    borderColor: '#999',
                    backgroundColor: 'rgba(153, 153, 153, 0.1)',
                    borderDash: [5, 5],
                    tension: 0.4,
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        color: '#cccccc'
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        color: '#999',
                        callback: function(value) {
                            return formatNumber(value);
                        }
                    },
                    grid: {
                        color: '#222'
                    }
                },
                x: {
                    ticks: {
                        color: '#999'
                    },
                    grid: {
                        color: '#222'
                    }
                }
            }
        }
    });
}

// Create minutes chart
function createMinutesChart(analytics) {
    const ctx = document.getElementById('minutes-chart');
    
    if (minutesChart) {
        minutesChart.destroy();
    }
    
    const dates = analytics.previous_month.map(d => new Date(d.date).toLocaleDateString('en-US', {month: 'short', day: 'numeric'}));
    
    minutesChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: dates,
            datasets: [{
                label: 'Engagement Minutes',
                data: analytics.previous_month.map(d => d.minutes),
                backgroundColor: 'rgba(229, 9, 20, 0.7)',
                borderColor: '#e50914',
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        color: '#999',
                        callback: function(value) {
                            return formatNumber(value);
                        }
                    },
                    grid: {
                        color: '#222'
                    }
                },
                x: {
                    ticks: {
                        color: '#999'
                    },
                    grid: {
                        color: '#222'
                    }
                }
            }
        }
    });
}

// Create devices chart
function createDevicesChart(devices) {
    const ctx = document.getElementById('devices-chart');
    
    if (devicesChart) {
        devicesChart.destroy();
    }
    
    devicesChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: devices.map(d => d.device),
            datasets: [{
                data: devices.map(d => d.percentage),
                backgroundColor: [
                    'rgba(229, 9, 20, 0.9)',
                    'rgba(229, 9, 20, 0.7)',
                    'rgba(229, 9, 20, 0.5)',
                    'rgba(229, 9, 20, 0.3)'
                ],
                borderWidth: 2,
                borderColor: '#0a0a0a'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: '#cccccc'
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.label + ': ' + context.parsed.toFixed(1) + '%';
                        }
                    }
                }
            }
        }
    });
}

// Event listeners
document.getElementById('create-segment-btn').addEventListener('click', createSegment);

// Allow Enter key in segment name
document.getElementById('segment-name').addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        createSegment();
    }
});

console.log('Advertiser Segment Builder loaded');
