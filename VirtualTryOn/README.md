# 👗 Virtual Try-On

A fashion virtual try-on web application that lets users select clothing items and generates professional outfit photos using Azure OpenAI's GPT-Image-1.

## ✨ Features

- **Interactive Web App** - Browse products by category, select items, and generate looks
- **AI Outfit Generation** - Combines multiple clothing items into a single professional photo
- **Shopping Cart** - Save looks to cart with size selection, remove individual items
- **Product Catalog** - Organized by categories: Pants, Skirts, Dresses, Jackets, Sweaters, Shoes
- **Modern UI** - Clean, responsive design with smooth animations

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- Azure subscription with Azure OpenAI (GPT-Image-1 deployed)
- Azure CLI installed

### 1. Clone & Install

```bash
git clone https://github.com/SyChell/VirtualTryOn.git
cd VirtualTryOn
pip install -r requirements.txt
```

### 2. Configure Environment

Copy `.env.example` to `.env` and fill in your values:

```env
# Required: Azure OpenAI
AOAI_API_BASE=https://your-resource.cognitiveservices.azure.com
AOAI_DEPLOYMENT_NAME=gpt-image-1
AOAI_API_VERSION=2025-04-01-preview

# Optional: Microsoft Fabric Eventstreams
FABRIC_EH_SALES_CONNECTION_STRING=Endpoint=sb://...
FABRIC_EH_COMBINATIONS_CONNECTION_STRING=Endpoint=sb://...
```

### 3. Login to Azure

```bash
az login
```

### 4. Run the App

```bash
python app.py
```

Open **http://localhost:5000** in your browser.

## 📁 Project Structure

```
VirtualTryOn/
├── app.py                  # Flask web server & API endpoints
├── agent.py                # AI image generation (Azure OpenAI)
├── fabric_client.py        # Microsoft Fabric Eventstream integration
├── requirements.txt        # Python dependencies
├── .env                    # Configuration (not committed)
├── .env.example            # Configuration template
├── data/
│   └── catalog.json        # Product catalog
├── static/
│   ├── css/styles.css      # Styling
│   ├── js/app.js           # Frontend logic
│   ├── products/           # Product images by category
│   └── generated/          # Generated outfit images
└── templates/
    └── index.html          # Main page
```

## 🛠️ How It Works

1. **Browse Products** - Select items from different categories
2. **Generate Look** - Click "Look Generieren" to create an outfit
3. **AI Processing** - Azure OpenAI combines clothing images into one photo
4. **Add to Cart** - Select sizes and save the look to your cart
5. **Place Order** - Order data is sent to Microsoft Fabric for analytics

### Technical Flow

```
User selects items → Flask API → Azure OpenAI → Generated outfit
                  ↓
         Microsoft Fabric Eventstreams (combinations + orders)
```

## 📓 Development Notebook

Use `main.ipynb` for testing the AI generation independently:

1. Open in VS Code or Jupyter
2. Update `IMAGES_FOLDER` to point to product images (e.g., `./static/products/hosen`)
3. Run all cells to generate an outfit

## 🔧 Configuration

| Variable | Description | Required |
|----------|-------------|----------|
| `AOAI_API_BASE` | Azure OpenAI endpoint | Yes |
| `AOAI_DEPLOYMENT_NAME` | Model deployment name | Yes |
| `AOAI_API_VERSION` | API version | Yes |
| `FABRIC_EH_SALES_CONNECTION_STRING` | Fabric Sales Eventstream connection string | No |
| `FABRIC_EH_COMBINATIONS_CONNECTION_STRING` | Fabric Combinations Eventstream connection string | No |

## 🐛 Troubleshooting

### "AOAI_API_BASE environment variable is required"
Create a `.env` file with your Azure OpenAI endpoint.

### Authentication errors
Run `az login` and ensure you're signed into the correct subscription.

### Images not loading
Check that product images exist in `static/products/<category>/` and filenames match `catalog.json`.

### Generation fails
- Verify GPT-Image-1 is deployed in Azure OpenAI
- Check Azure CLI authentication: `az account show`

### Fabric events not sending
- Check that connection strings are set in `.env`
- Verify the Eventstream "Custom App" source is configured in Fabric

## 🔗 Microsoft Fabric Integration

The app sends data to Microsoft Fabric Eventstreams for analytics:

- **Combinations Stream** - Tracks outfit combinations users generate
- **Sales Stream** - Tracks orders placed

### Data Models

**Combination:**
```json
{
  "combination_id": "abc123...",
  "user_id": "user-xyz",
  "items": [{"product_id": "...", "name": "...", "price": 99.95, "color": "..."}]
}
```

**Order:**
```json
{
  "order_id": "uuid",
  "combination_id": "abc123...",
  "user_id": "user-xyz",
  "items": [...]
}
```

### Getting Connection Strings

1. Open Microsoft Fabric workspace
2. Go to your Eventstream
3. Add a **Custom App** source
4. Copy the connection string from the source settings

## 📦 Dependencies

- **Flask** - Web framework
- **Flask-CORS** - Cross-origin support
- **azure-eventhub** - Microsoft Fabric Eventstream integration
- **requests** - HTTP client
- **python-dotenv** - Environment variables


