"""
Virtual Try-On Agent
Simple image-in/image-out pipeline: takes clothing images, outputs combined outfit image.

Uses Azure OpenAI Image Edit API with multi-image support (REST API).

TODO: Transition to hosted agent in next iteration.
"""

import os
import base64
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential

load_dotenv()


def create_session_with_retries():
    """Create a requests session with retry logic."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def generate_outfit_image(image_paths: list[str], output_path: str) -> str:
    """
    Generates a combined outfit image from multiple clothing item images.
    
    Args:
        image_paths: List of paths to clothing item images
        output_path: Path to save the generated image
        
    Returns:
        Path to the generated image
    """
    if not image_paths:
        raise ValueError("No images provided")
    
    # Config
    api_base = os.environ.get("AOAI_API_BASE")
    deployment = os.environ.get("AOAI_DEPLOYMENT_NAME", "gpt-image-1")
    api_version = os.environ.get("AOAI_API_VERSION", "2025-04-01-preview")
    
    if not api_base:
        raise ValueError("AOAI_API_BASE environment variable is required")
    
    url = f"{api_base}/openai/deployments/{deployment}/images/edits?api-version={api_version}"
    
    # Auth
    credential = DefaultAzureCredential()
    token = credential.get_token("https://cognitiveservices.azure.com/.default").token
    headers = {"Authorization": f"Bearer {token}"}
    
    # Build prompt
    item_names = [os.path.splitext(os.path.basename(p))[0] for p in image_paths]
    prompt = f"""Create a professional fashion photo of a female model wearing: {", ".join(item_names)}

- Use ONLY the clothing items from the provided images
- Do not add any other items
- Clean white background, full body shot
"""
    
    print(f"🎨 Calling Image Edit API with {len(image_paths)} images...")
    
    # Multi-image upload
    files = []
    file_handles = []
    session = create_session_with_retries()
    try:
        for img_path in image_paths:
            fh = open(img_path, "rb")
            file_handles.append(fh)
            files.append(("image[]", (os.path.basename(img_path), fh, "image/jpeg")))
        
        data = {
            "prompt": prompt,
            "n": 1,
            "size": "1024x1536",
            "quality": "high"
        }
        
        # Use longer timeout for image generation (5 minutes)
        response = session.post(url, headers=headers, files=files, data=data, timeout=300)
        response.raise_for_status()
        
        result = response.json()
        if result.get("data") and result["data"][0].get("b64_json"):
            with open(output_path, "wb") as f:
                f.write(base64.b64decode(result["data"][0]["b64_json"]))
            return output_path
        
        raise ValueError("No image in response")
    finally:
        for fh in file_handles:
            fh.close()
