# 🧱✨ Databricks Sparkle Cursor App ✨🧱

A magical Node.js application built specifically for the Databricks Apps platform that transforms your mouse cursor into a beautiful sparkle-shooting experience with enterprise-grade features!

## 🎯 Databricks-Specific Features

- **🏢 Workspace Integration**: Real-time workspace and user information display
- **📊 Enterprise Analytics**: Advanced sparkle metrics and performance monitoring  
- **🎨 Databricks Theming**: Official Databricks colors (Orange, Teal, Navy Blue)
- **⚡ Health Monitoring**: Built-in health checks and status indicators
- **🔧 API Endpoints**: RESTful APIs for configuration and analytics
- **🎮 Enhanced Controls**: Databricks-specific keyboard shortcuts and interactions
- **📈 Performance Metrics**: Real-time FPS, memory usage, and sparkle count tracking
- **🌈 Advanced Effects**: Special Databricks-themed sparkle patterns and animations

## 🚀 Quick Start (Databricks Apps)

### Prerequisites
- **Databricks Workspace** with Apps enabled
- **Databricks CLI** configured
- **Node.js 16+** runtime support

### Deployment

1. **Clone to your Databricks workspace**:
   ```bash
   git clone <repo-url>
   cd sparkle-cursor-databricks
   ```

2. **Deploy using Databricks CLI**:
   ```bash
   databricks apps deploy
   ```

3. **Access your app**:
   - Navigate to your Databricks workspace
   - Go to **Apps** section
   - Launch **Sparkle Cursor** app
   - Start creating magical sparkles! ✨

### Local Development

```bash
# Navigate to source directory
cd src

# Install dependencies
npm install

# Start development server
npm run dev

# Open browser to http://localhost:8080
```

## 🎮 Enhanced Controls & Features

### Basic Controls
| Action | Effect |
|--------|--------|
| **Mouse Movement** | Create sparkle trails with Databricks colors |
| **Left Click** | Enhanced sparkle burst (18 sparkles) |
| **Right Click** | Mega sparkle burst (30 sparkles) |
| **Hold Shift** | Mega sparkles with extended lifetime |

### Databricks-Specific Controls
| Action | Effect |
|--------|--------|
| **Space Bar** | Databricks-themed burst (Orange/Teal/Navy) |
| **Ctrl/Cmd + D** | Toggle Databricks mode (enhanced theming) |
| **Hover Sparkle Zone** | Auto-activate Databricks mode |
| **Click Data Icons** | Themed bursts (📓⚡💾🤖📊) |
| **Konami Code** | 🌈 Databricks Rainbow Mode! |

### Console Commands
```javascript
// Get real-time analytics
databricksSparkleTrail.getAnalytics()

// Export analytics data
databricksSparkleTrail.exportAnalytics()

// Adjust sparkle intensity (1-10 scale)
databricksSparkleTrail.setSparkleIntensity(8)

// Get performance metrics
getDatabricksSparkleMetrics()
```

## 🏗️ Architecture

### File Structure
```
sparkle-cursor-databricks/
├── databricks.yml           # Databricks app configuration
├── src/
│   ├── app.js              # Express server with Databricks APIs
│   ├── package.json        # Node.js dependencies
│   └── public/
│       ├── index.html      # Databricks-themed UI
│       ├── styles.css      # Enterprise styling
│       └── sparkle.js      # Enhanced sparkle engine
└── README.md               # This file
```

### API Endpoints

#### Core Endpoints
- `GET /` - Main sparkle cursor application
- `GET /health` - Health check (required by Databricks)

#### Analytics APIs
- `GET /api/sparkle-config` - Sparkle behavior configuration
- `GET /api/sparkle-analytics` - Real-time sparkle metrics
- `GET /api/user-info` - Workspace and user information

## 🚀 Ready to Sparkle?

Deploy this app to your Databricks workspace and transform your data experience with magical mouse effects!

```bash
databricks apps deploy
```

**Created with ❤️ and lots of ✨ sparkles ✨ for the Databricks community**

*Now go forth and add some magic to your data workflows! 🧱✨*
