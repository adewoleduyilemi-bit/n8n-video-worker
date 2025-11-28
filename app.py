"""
External Worker: Audio-Video Merge Service
Handles FFmpeg video processing with audio sync
Deploy on Railway, Render, or any Python-capable server with FFmpeg
"""

from flask import Flask, request, jsonify, send_file
import subprocess
import requests
import os
import json
import uuid
from datetime import datetime
import logging

app = Flask(__name__)

# Configuration - Railway uses /tmp for ephemeral storage
UPLOAD_FOLDER = '/tmp/downloads'
TEMP_FOLDER = '/tmp/n8n_video_processing'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(TEMP_FOLDER, exist_ok=True)

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Variant configurations
VARIANTS = {
    'pablo': {
        'voice_id': 'OhisAd2u8Q6qSA4xXAAT',
        'speed': 1.00,
        'filter': 'none'
    },
    'josh': {
        'voice_id': 'Rsz5u2Huh1hPlPr0oxRQ',
        'speed': 1.01,
        'filter': 'eq=contrast=1.02'
    },
    'michael': {
        'voice_id': 'dfpTJ8gngbfXIon7bId3',
        'speed': 0.99,
        'filter': 'eq=saturation=1.015'
    },
    'ryan': {
        'voice_id': '4e32WqNVWRquDa1OcRYZ',
        'speed': 1.02,
        'filter': 'unsharp=5:5:1.5'
    },
    'brad': {
        'voice_id': 'f5HLTX707KIM4SzJYzSz',
        'speed': 0.98,
        'filter': 'eq=gamma=1.01'
    }
}

def download_file(url, dest_path):
    """Download file from URL"""
    try:
        response = requests.get(url, timeout=300, stream=True)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        logger.error(f"Download failed: {str(e)}")
        return False

def get_video_duration(video_path):
    """Get video duration in seconds"""
    try:
        cmd = [
            'ffprobe', '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return float(result.stdout.strip())
    except Exception as e:
        logger.error(f"Duration check failed: {str(e)}")
        return 0

def adjust_audio_speed(audio_path, speed, output_path):
    """Adjust audio playback speed using ffmpeg"""
    try:
        # atempo filter can only handle speeds between 0.5 and 2.0
        cmd = [
            'ffmpeg', '-i', audio_path,
            '-af', f'atempo={speed}',
            '-y', output_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error(f"Speed adjustment failed: {result.stderr}")
            return False
        return True
    except Exception as e:
        logger.error(f"Speed adjustment error: {str(e)}")
        return False

def apply_video_filter(video_path, filter_spec, output_path):
    """Apply FFmpeg video filter"""
    try:
        if filter_spec == 'none':
            # Copy without filter
            cmd = [
                'ffmpeg', '-i', video_path,
                '-c:v', 'libx264', '-crf', '23', '-preset', 'fast',
                '-y', output_path
            ]
        else:
            # Apply filter
            cmd = [
                'ffmpeg', '-i', video_path,
                '-vf', filter_spec,
                '-c:v', 'libx264', '-crf', '23', '-preset', 'fast',
                '-y', output_path
            ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            logger.error(f"Filter application failed: {result.stderr}")
            return False
        return True
    except Exception as e:
        logger.error(f"Filter error: {str(e)}")
        return False

def merge_audio_video(video_path, audio_path, output_path):
    """Merge audio and video with sync"""
    try:
        cmd = [
            'ffmpeg',
            '-i', video_path,
            '-i', audio_path,
            '-c:v', 'copy',
            '-c:a', 'aac',
            '-map', '0:v:0',
            '-map', '1:a:0',
            '-shortest',
            '-y', output_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            logger.error(f"Merge failed: {result.stderr}")
            return False
        
        logger.info(f"Successfully merged to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Merge error: {str(e)}")
        return False

@app.route('/', methods=['GET'])
def home():
    """Root endpoint"""
    return jsonify({
        'service': 'FFmpeg Video Processing API',
        'status': 'online',
        'endpoints': {
            'health': '/health',
            'merge': '/merge (POST)',
            'variants': '/variants',
            'download': '/download/<filename>'
        }
    })

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    # Test if FFmpeg is available
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True, timeout=5)
        ffmpeg_available = result.returncode == 0
    except:
        ffmpeg_available = False
    
    return jsonify({
        'status': 'ok',
        'ffmpeg_available': ffmpeg_available,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/merge', methods=['POST'])
def process_variant():
    """
    Process a single video variant
    
    Expected JSON:
    {
        "video_url": "https://...",
        "audio_url": "https://...",
        "variant_name": "pablo",
        "workflow_id": "abc123"
    }
    """
    try:
        data = request.json
        
        # Validate input
        required_fields = ['video_url', 'audio_url', 'variant_name', 'workflow_id']
        if not all(field in data for field in required_fields):
            return jsonify({'error': 'Missing required fields'}), 400
        
        video_url = data['video_url']
        audio_url = data['audio_url']
        variant_name = data['variant_name']
        workflow_id = data['workflow_id']
        
        if variant_name not in VARIANTS:
            return jsonify({'error': f'Unknown variant: {variant_name}'}), 400
        
        # Create working directory
        work_dir = os.path.join(TEMP_FOLDER, workflow_id, variant_name)
        os.makedirs(work_dir, exist_ok=True)
        
        # Paths
        video_file = os.path.join(work_dir, 'source_video.mp4')
        audio_file = os.path.join(work_dir, 'source_audio.mp3')
        audio_speed_file = os.path.join(work_dir, 'audio_speed.mp3')
        video_filtered_file = os.path.join(work_dir, 'video_filtered.mp4')
        output_filename = f'{workflow_id}_{variant_name}.mp4'
        output_file = os.path.join(UPLOAD_FOLDER, output_filename)
        
        logger.info(f"Processing variant {variant_name} for workflow {workflow_id}")
        
        # Download video
        logger.info(f"Downloading video from {video_url}")
        if not download_file(video_url, video_file):
            return jsonify({'error': 'Failed to download video'}), 400
        
        # Download audio
        logger.info(f"Downloading audio from {audio_url}")
        if not download_file(audio_url, audio_file):
            return jsonify({'error': 'Failed to download audio'}), 400
        
        variant_config = VARIANTS[variant_name]
        speed = variant_config['speed']
        filter_spec = variant_config['filter']
        
        # Adjust audio speed
        logger.info(f"Adjusting audio speed to {speed}x")
        if not adjust_audio_speed(audio_file, speed, audio_speed_file):
            return jsonify({'error': 'Failed to adjust audio speed'}), 400
        
        # Apply video filter
        logger.info(f"Applying filter: {filter_spec}")
        if not apply_video_filter(video_file, filter_spec, video_filtered_file):
            return jsonify({'error': 'Failed to apply video filter'}), 400
        
        # Merge audio and video
        logger.info("Merging audio and video")
        if not merge_audio_video(video_filtered_file, audio_speed_file, output_file):
            return jsonify({'error': 'Failed to merge audio and video'}), 400
        
        # Verify output file exists
        if not os.path.exists(output_file):
            return jsonify({'error': 'Output file not created'}), 400
        
        file_size = os.path.getsize(output_file)
        logger.info(f"Successfully created {output_file} ({file_size} bytes)")
        
        # Build download URL using Railway's domain
        download_url = f"https://{request.host}/download/{output_filename}"
        
        # Cleanup temp directory
        try:
            import shutil
            shutil.rmtree(work_dir)
        except:
            pass
        
        return jsonify({
            'status': 'success',
            'variant': variant_name,
            'output_file': output_file,
            'download_url': download_url,
            'file_size': file_size,
            'timestamp': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/download/<filename>', methods=['GET'])
def download_file_endpoint(filename):
    """Serve processed video files"""
    try:
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True, download_name=filename)
        else:
            return jsonify({'error': 'File not found'}), 404
    except Exception as e:
        logger.error(f"Download error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/variants', methods=['GET'])
def get_variants():
    """Get list of available variants"""
    return jsonify({
        'variants': list(VARIANTS.keys()),
        'details': VARIANTS
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
